// Command deletemedia varre a pasta de mídias baixadas pelo savemedia e apaga
// do "Saved Messages" (peer self) exatamente as mensagens de onde cada arquivo
// veio.
//
// Ele NÃO re-varre o Telegram: os arquivos do savemedia são nomeados
// "<msgID>_<...>", então basta ler o prefixo numérico do nome de cada arquivo
// para descobrir quais mensagens apagar. Assim você só apaga o que já baixou e
// tem em disco.
//
// Os lotes de deleção (até 100 IDs por chamada, limite da API) rodam num pool
// de workers em paralelo, mas todos passam pelos middlewares de floodwait +
// ratelimit, então o paralelismo respeita o teto da API do Telegram.
//
// ATENÇÃO: apagar do Saved Messages é PERMANENTE. Por segurança, o padrão é
// dry-run (só lista o que apagaria). Passe --confirm para apagar de verdade.
//
//	./deletemedia                 # dry-run: lista os IDs achados em ./media
//	./deletemedia --confirm       # apaga de verdade
//	./deletemedia --dir fotos     # varre outra pasta
package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/joho/godotenv"

	"github.com/gotd/contrib/middleware/floodwait"
	"github.com/gotd/contrib/middleware/ratelimit"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"
	"golang.org/x/time/rate"
)

// nomes gerados pelo savemedia começam com "<msgID>_".
var namePrefix = regexp.MustCompile(`^(\d+)_`)

func main() {
	var (
		dir         = flag.String("dir", "media", "pasta com as mídias baixadas (fonte dos IDs)")
		confirm     = flag.Bool("confirm", false, "apaga de verdade; sem esta flag apenas lista (dry-run)")
		workers     = flag.Int("workers", 4, "lotes de deleção em paralelo")
		sessionFile = flag.String("session", "session.json", "arquivo de sessão (mantém o login)")
		envFile     = flag.String("env", ".env", "arquivo .env com as credenciais")
	)
	flag.Parse()

	if err := godotenv.Load(*envFile); err != nil && !os.IsNotExist(err) {
		log.Fatalf("lendo %s: %v", *envFile, err)
	}

	apiID, err := strconv.Atoi(os.Getenv("TG_API_ID"))
	if err != nil {
		log.Fatal("defina TG_API_ID (pegue em https://my.telegram.org)")
	}
	apiHash := os.Getenv("TG_API_HASH")
	if apiHash == "" {
		log.Fatal("defina TG_API_HASH (pegue em https://my.telegram.org)")
	}
	phone := os.Getenv("TG_PHONE")
	if phone == "" {
		log.Fatal("defina TG_PHONE (ex: +5511999998888)")
	}

	ids, err := scanIDs(*dir)
	if err != nil {
		log.Fatalf("varrendo %s: %v", *dir, err)
	}
	if len(ids) == 0 {
		log.Fatalf("nenhum arquivo '<msgID>_...' encontrado em %s (rode o savemedia primeiro?)", *dir)
	}
	log.Printf("%d mensagens serão %s (a partir das mídias em %s)",
		len(ids), ternary(*confirm, "APAGADAS", "listadas (dry-run)"), *dir)

	ctx := context.Background()
	if err := run(ctx, apiID, apiHash, phone, *sessionFile, ids, *confirm, *workers); err != nil {
		log.Fatal(err)
	}
}

// scanIDs percorre a pasta (recursivo) e extrai o msg id do prefixo do nome de
// cada arquivo, ignorando downloads incompletos (.part) e diretórios.
func scanIDs(dir string) ([]int, error) {
	var ids []int
	seen := make(map[int]bool)

	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if strings.HasSuffix(name, ".part") {
			return nil // download incompleto
		}
		m := namePrefix.FindStringSubmatch(name)
		if m == nil {
			return nil
		}
		id, err := strconv.Atoi(m[1])
		if err != nil || seen[id] {
			return nil
		}
		seen[id] = true
		ids = append(ids, id)
		return nil
	})
	return ids, err
}

func run(ctx context.Context, apiID int, apiHash, phone, sessionPath string, ids []int, confirm bool, workers int) error {
	client := telegram.NewClient(apiID, apiHash, telegram.Options{
		SessionStorage: &sessionStorageFile{Path: sessionPath},
		Middlewares: []telegram.Middleware{
			floodwait.NewSimpleWaiter(),
			ratelimit.New(rate.Every(time.Millisecond*100), 5),
		},
	})

	return client.Run(ctx, func(ctx context.Context) error {
		flow := auth.NewFlow(termAuth{phone: phone}, auth.SendCodeOptions{})
		if err := client.Auth().IfNecessary(ctx, flow); err != nil {
			return fmt.Errorf("auth: %w", err)
		}
		api := client.API()

		if !confirm {
			log.Println("DRY-RUN: nada será apagado. IDs que seriam apagados:")
			log.Printf("  %s", joinIDs(ids))
			log.Println("Reveja a lista e rode de novo com --confirm para apagar.")
			return nil
		}

		// messages.deleteMessages aceita no máximo 100 IDs por chamada. Monta os
		// lotes e distribui para um pool de workers; o rate limiter global segura
		// a taxa real, então o paralelismo é seguro.
		const chunk = 100
		var batches [][]int
		for start := 0; start < len(ids); start += chunk {
			end := start + chunk
			if end > len(ids) {
				end = len(ids)
			}
			b := make([]int, end-start)
			copy(b, ids[start:end])
			batches = append(batches, b)
		}
		log.Printf("%d mensagens em %d lote(s), %d workers", len(ids), len(batches), workers)

		var (
			deleted   int64
			failed    int64
			doneBatch int64
			firstErr  error
			errOnce   sync.Once
		)
		total := len(batches)

		jobsCh := make(chan []int)
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for b := range jobsCh {
					affected, err := api.MessagesDeleteMessages(ctx, &tg.MessagesDeleteMessagesRequest{
						Revoke: true, // apaga para todos (no Saved Messages é só você)
						ID:     b,
					})
					n := atomic.AddInt64(&doneBatch, 1)
					if err != nil {
						atomic.AddInt64(&failed, int64(len(b)))
						log.Printf("[lote %d/%d] FALHA (%d IDs): %v", n, total, len(b), err)
						errOnce.Do(func() { firstErr = err })
						continue
					}
					atomic.AddInt64(&deleted, int64(len(b)))
					log.Printf("[lote %d/%d] OK: %d IDs enviados (pts %d)", n, total, len(b), affected.PtsCount)
				}
			}()
		}

		for _, b := range batches {
			jobsCh <- b
		}
		close(jobsCh)
		wg.Wait()

		log.Printf("concluído: %d apagadas, %d falhas", deleted, failed)
		if firstErr != nil {
			return fmt.Errorf("houve falhas ao apagar: %w", firstErr)
		}
		return nil
	})
}

func joinIDs(ids []int) string {
	var b strings.Builder
	for i, id := range ids {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(strconv.Itoa(id))
	}
	return b.String()
}

func ternary(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

// --- Autenticação via terminal (igual aos outros comandos) ---

type termAuth struct {
	phone string
}

func (a termAuth) Phone(_ context.Context) (string, error) { return a.phone, nil }

func (a termAuth) Password(_ context.Context) (string, error) {
	fmt.Print("Senha 2FA (deixe vazio se não tiver): ")
	var p string
	fmt.Scanln(&p)
	return strings.TrimSpace(p), nil
}

func (a termAuth) Code(_ context.Context, _ *tg.AuthSentCode) (string, error) {
	fmt.Print("Código recebido no Telegram: ")
	var c string
	fmt.Scanln(&c)
	return strings.TrimSpace(c), nil
}

func (a termAuth) AcceptTermsOfService(_ context.Context, _ tg.HelpTermsOfService) error {
	return nil
}

func (a termAuth) SignUp(_ context.Context) (auth.UserInfo, error) {
	return auth.UserInfo{}, fmt.Errorf("conta não registrada")
}

type sessionStorageFile = session.FileStorage
