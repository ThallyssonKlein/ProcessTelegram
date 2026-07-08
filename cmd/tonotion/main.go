// Command tonotion importa os itens exportados do Telegram (saved_messages.txt
// ou saved_links.txt, mesmo formato) para uma database do Notion.
//
// A escrita no Notion é o gargalo: a API tem um teto de ~3 requisições/segundo
// por integração. Então usamos um pool de workers para sobrepor a latência das
// requisições, mas TODOS compartilham um rate limiter global que respeita esse
// teto, e cada requisição faz retry com backoff honrando o header Retry-After
// nos 429. Um arquivo de checkpoint (<in>.notion-done) guarda os IDs já criados,
// então re-rodar o comando retoma de onde parou sem duplicar.
//
// Uso:
//
//	# 1ª vez: cria a database com o schema certo e imprime o ID
//	./tonotion -create-under <PAGE_ID>
//
//	# importa (pega o token e a db do .env, ou passe por flag)
//	./tonotion -in saved_messages.txt
//	./tonotion -in saved_links.txt
//
// Variáveis de ambiente (.env):
//
//	NOTION_TOKEN        token da integração (https://www.notion.so/my-integrations)
//	NOTION_DATABASE_ID  id da database de destino (ou use a flag -db)
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"
	"golang.org/x/time/rate"
)

const (
	notionAPI     = "https://api.notion.com/v1"
	notionVersion = "2022-06-28"
	// Limite de caracteres de um rich_text/title no Notion.
	notionTextLimit = 2000
	// Limite de blocos filhos por requisição de criação de página.
	notionMaxChildren = 100
)

func main() {
	var (
		in          = flag.String("in", "saved_messages.txt", "arquivo de entrada (formato do processtelegram)")
		dbID        = flag.String("db", "", "id da database do Notion (default: env NOTION_DATABASE_ID)")
		workers     = flag.Int("workers", 5, "workers paralelos (o rate limiter global é quem limita a taxa real)")
		rps         = flag.Float64("rps", 3, "requisições por segundo (teto da API do Notion ~= 3)")
		maxRetries  = flag.Int("retries", 6, "tentativas por página em caso de 429/5xx")
		createUnder = flag.String("create-under", "", "id de uma page: cria a database com o schema, imprime o id e sai")
		envFile     = flag.String("env", ".env", "arquivo .env com as credenciais")
	)
	flag.Parse()

	if err := godotenv.Load(*envFile); err != nil && !os.IsNotExist(err) {
		log.Fatalf("lendo %s: %v", *envFile, err)
	}
	token := os.Getenv("NOTION_TOKEN")
	if token == "" {
		log.Fatal("defina NOTION_TOKEN (https://www.notion.so/my-integrations)")
	}

	c := &client{
		token:      token,
		http:       &http.Client{Timeout: 60 * time.Second},
		limiter:    rate.NewLimiter(rate.Limit(*rps), 1),
		maxRetries: *maxRetries,
	}
	ctx := context.Background()

	// Modo utilitário: cria a database e sai.
	if *createUnder != "" {
		id, err := c.createDatabase(ctx, *createUnder)
		if err != nil {
			log.Fatalf("criando database: %v", err)
		}
		fmt.Printf("database criada: %s\n\nAdicione ao .env:\n  NOTION_DATABASE_ID=%s\n", id, id)
		return
	}

	database := *dbID
	if database == "" {
		database = os.Getenv("NOTION_DATABASE_ID")
	}
	if database == "" {
		log.Fatal("defina NOTION_DATABASE_ID (ou -db), ou rode com -create-under <PAGE_ID> primeiro")
	}

	if err := run(ctx, c, database, *in, *workers); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, c *client, database, inPath string, workers int) error {
	// Descobre os nomes das propriedades da database pelos seus tipos, então o
	// mesmo script funciona independente de como o usuário nomeou as colunas.
	schema, err := c.databaseSchema(ctx, database)
	if err != nil {
		return fmt.Errorf("lendo schema da database: %w", err)
	}
	if schema.title == "" {
		return errors.New("a database não tem propriedade do tipo title")
	}
	log.Printf("schema: title=%q date=%q number=%q url=%q select=%q",
		schema.title, schema.date, schema.number, schema.url, schema.selectp)

	items, err := parseFile(inPath)
	if err != nil {
		return fmt.Errorf("lendo %s: %w", inPath, err)
	}
	log.Printf("%d itens em %s", len(items), inPath)

	// Checkpoint: pula os que já foram criados em execuções anteriores.
	donePath := inPath + ".notion-done"
	done, err := loadDone(donePath)
	if err != nil {
		return fmt.Errorf("lendo checkpoint: %w", err)
	}
	doneFile, err := os.OpenFile(donePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("abrindo checkpoint: %w", err)
	}
	defer doneFile.Close()

	source := sourceName(inPath)

	var (
		mu             sync.Mutex // protege doneFile + contadores
		created, fails int
		skipped        int
	)

	jobs := make(chan msgItem)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := range jobs {
				if err := c.createPage(ctx, database, schema, source, it); err != nil {
					log.Printf("[msg %d] FALHA: %v", it.id, err)
					mu.Lock()
					fails++
					mu.Unlock()
					continue
				}
				mu.Lock()
				fmt.Fprintf(doneFile, "%d\n", it.id)
				created++
				n := created
				mu.Unlock()
				if n%100 == 0 {
					log.Printf("progresso: %d criados", n)
				}
			}
		}()
	}

	for _, it := range items {
		if done[it.id] {
			skipped++
			continue
		}
		jobs <- it
	}
	close(jobs)
	wg.Wait()

	log.Printf("concluído: %d criados, %d já existiam (pulados), %d falhas", created, skipped, fails)
	if fails > 0 {
		return fmt.Errorf("%d falhas — rode de novo para retomar (o checkpoint pula os já criados)", fails)
	}
	return nil
}

// --- Parsing do arquivo de entrada ---

type msgItem struct {
	id   int
	date time.Time
	text string
}

var headerRe = regexp.MustCompile(`^----- msg (\d+) \| (.+?) -----$`)

const inputDateLayout = "2006-01-02 15:04:05"

func parseFile(path string) ([]msgItem, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var items []msgItem
	var cur *msgItem
	var body []string

	flush := func() {
		if cur == nil {
			return
		}
		cur.text = strings.TrimRight(strings.Join(body, "\n"), "\n")
		if strings.TrimSpace(cur.text) != "" {
			items = append(items, *cur)
		}
		cur = nil
		body = body[:0]
	}

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024) // linhas longas (URLs)
	for sc.Scan() {
		line := sc.Text()
		if m := headerRe.FindStringSubmatch(line); m != nil {
			flush()
			id, _ := strconv.Atoi(m[1])
			t, err := time.Parse(inputDateLayout, m[2])
			if err != nil {
				t = time.Time{}
			}
			cur = &msgItem{id: id, date: t}
			continue
		}
		if cur != nil {
			body = append(body, line)
		}
	}
	flush()
	return items, sc.Err()
}

var urlRe = regexp.MustCompile(`https?://[^\s]+`)

func firstURL(s string) string {
	return urlRe.FindString(s)
}

// firstLine devolve a primeira linha não-vazia, para servir de título.
func firstLine(s string) string {
	for _, ln := range strings.Split(s, "\n") {
		ln = strings.TrimSpace(ln)
		if ln != "" {
			return ln
		}
	}
	return strings.TrimSpace(s)
}

func sourceName(path string) string {
	base := path
	if i := strings.LastIndexAny(base, "/\\"); i >= 0 {
		base = base[i+1:]
	}
	base = strings.TrimSuffix(base, ".txt")
	return base
}

func truncateRunes(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}

// --- Checkpoint ---

func loadDone(path string) (map[int]bool, error) {
	done := map[int]bool{}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return done, nil
		}
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if id, err := strconv.Atoi(strings.TrimSpace(sc.Text())); err == nil {
			done[id] = true
		}
	}
	return done, sc.Err()
}

// --- Cliente Notion ---

type client struct {
	token      string
	http       *http.Client
	limiter    *rate.Limiter
	maxRetries int
}

// do faz uma requisição respeitando o rate limiter e com retry em 429/5xx.
func (c *client) do(ctx context.Context, method, path string, body any) ([]byte, error) {
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			return nil, err
		}
	}

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if err := c.limiter.Wait(ctx); err != nil {
			return nil, err
		}

		req, err := http.NewRequestWithContext(ctx, method, notionAPI+path, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+c.token)
		req.Header.Set("Notion-Version", notionVersion)
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			sleep(ctx, backoff(attempt))
			continue
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		switch {
		case resp.StatusCode >= 200 && resp.StatusCode < 300:
			return respBody, nil
		case resp.StatusCode == 429:
			wait := backoff(attempt)
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				if secs, err := strconv.Atoi(ra); err == nil {
					wait = time.Duration(secs) * time.Second
				}
			}
			lastErr = fmt.Errorf("429 rate limited: %s", snippet(respBody))
			sleep(ctx, wait)
		case resp.StatusCode >= 500:
			lastErr = fmt.Errorf("%d: %s", resp.StatusCode, snippet(respBody))
			sleep(ctx, backoff(attempt))
		default:
			// 4xx (exceto 429): erro do request, não adianta repetir.
			return nil, fmt.Errorf("%s %s: %d: %s", method, path, resp.StatusCode, snippet(respBody))
		}
	}
	return nil, fmt.Errorf("esgotou tentativas: %w", lastErr)
}

func backoff(attempt int) time.Duration {
	d := time.Duration(1<<attempt) * time.Second // 1,2,4,8,...
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	return truncateRunes(s, 300)
}

// schema mapeia tipo -> nome da propriedade na database.
type schema struct {
	title   string
	date    string
	number  string
	url     string
	selectp string
}

func (c *client) databaseSchema(ctx context.Context, dbID string) (schema, error) {
	b, err := c.do(ctx, http.MethodGet, "/databases/"+dbID, nil)
	if err != nil {
		return schema{}, err
	}
	var resp struct {
		Properties map[string]struct {
			Type string `json:"type"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(b, &resp); err != nil {
		return schema{}, err
	}
	var s schema
	for name, p := range resp.Properties {
		switch p.Type {
		case "title":
			s.title = name
		case "date":
			if s.date == "" {
				s.date = name
			}
		case "number":
			if s.number == "" {
				s.number = name
			}
		case "url":
			if s.url == "" {
				s.url = name
			}
		case "select":
			if s.selectp == "" {
				s.selectp = name
			}
		}
	}
	return s, nil
}

func (c *client) createDatabase(ctx context.Context, parentPageID string) (string, error) {
	body := map[string]any{
		"parent": map[string]any{"type": "page_id", "page_id": parentPageID},
		"title": []any{
			map[string]any{"type": "text", "text": map[string]any{"content": "Telegram Saved"}},
		},
		"properties": map[string]any{
			"Name":   map[string]any{"title": map[string]any{}},
			"URL":    map[string]any{"url": map[string]any{}},
			"Date":   map[string]any{"date": map[string]any{}},
			"Msg ID": map[string]any{"number": map[string]any{}},
			"Source": map[string]any{"select": map[string]any{}},
		},
	}
	b, err := c.do(ctx, http.MethodPost, "/databases", body)
	if err != nil {
		return "", err
	}
	var resp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(b, &resp); err != nil {
		return "", err
	}
	return resp.ID, nil
}

func (c *client) createPage(ctx context.Context, dbID string, s schema, source string, it msgItem) error {
	props := map[string]any{
		s.title: map[string]any{
			"title": []any{textObj(truncateRunes(firstLine(it.text), notionTextLimit))},
		},
	}
	if s.number != "" {
		props[s.number] = map[string]any{"number": it.id}
	}
	if s.date != "" && !it.date.IsZero() {
		props[s.date] = map[string]any{
			"date": map[string]any{"start": it.date.Format(time.RFC3339)},
		}
	}
	if s.url != "" {
		if u := firstURL(it.text); u != "" {
			props[s.url] = map[string]any{"url": u}
		}
	}
	if s.selectp != "" {
		props[s.selectp] = map[string]any{"select": map[string]any{"name": source}}
	}

	body := map[string]any{
		"parent":     map[string]any{"database_id": dbID},
		"properties": props,
		"children":   textBlocks(it.text),
	}
	_, err := c.do(ctx, http.MethodPost, "/pages", body)
	return err
}

// textObj monta um objeto rich_text a partir de uma string (já truncada).
func textObj(content string) map[string]any {
	return map[string]any{
		"type": "text",
		"text": map[string]any{"content": content},
	}
}

// textBlocks quebra o texto completo em parágrafos de até notionTextLimit
// caracteres, preservando tudo no corpo da página (o título é só um resumo).
func textBlocks(text string) []any {
	r := []rune(text)
	var blocks []any
	for i := 0; i < len(r) && len(blocks) < notionMaxChildren; i += notionTextLimit {
		end := i + notionTextLimit
		if end > len(r) {
			end = len(r)
		}
		chunk := string(r[i:end])
		blocks = append(blocks, map[string]any{
			"object": "block",
			"type":   "paragraph",
			"paragraph": map[string]any{
				"rich_text": []any{textObj(chunk)},
			},
		})
	}
	return blocks
}
