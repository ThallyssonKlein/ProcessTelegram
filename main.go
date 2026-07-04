// Command processtelegram lê todas as mensagens do seu "Saved Messages"
// (peer self) via MTProto, filtra apenas as de texto puro e grava num .txt.
//
// A leitura do histórico é sequencial e limitada por rate limit / floodwait
// (é aí que os limites da API do Telegram pesam). O processamento de cada
// mensagem roda em paralelo num pool de workers, e a escrita preserva a
// ordem original.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"

	"github.com/gotd/contrib/middleware/floodwait"
	"github.com/gotd/contrib/middleware/ratelimit"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/query"
	"github.com/gotd/td/tg"
	"golang.org/x/time/rate"
)

func main() {
	var (
		out     = flag.String("out", "saved_messages.txt", "arquivo de saída")
		workers = flag.Int("workers", 8, "número de workers paralelos para processar as mensagens")
		batch   = flag.Int("batch", 100, "mensagens por página na leitura (máx 100)")
		sessionFile = flag.String("session", "session.json", "arquivo de sessão (mantém o login)")
		envFile     = flag.String("env", ".env", "arquivo .env com as credenciais")
	)
	flag.Parse()

	// Carrega o .env se existir; variáveis já definidas no ambiente têm precedência.
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

	ctx := context.Background()
	if err := run(ctx, apiID, apiHash, phone, *sessionFile, *out, *workers, *batch); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, apiID int, apiHash, phone, sessionPath, outPath string, workers, batch int) error {
	client := telegram.NewClient(apiID, apiHash, telegram.Options{
		SessionStorage: &sessionStorageFile{Path: sessionPath},
		Middlewares: []telegram.Middleware{
			// Espera automaticamente quando o Telegram devolve FLOOD_WAIT.
			floodwait.NewSimpleWaiter(),
			// Teto de segurança: ~10 req/s com burst pequeno.
			ratelimit.New(rate.Every(time.Millisecond*100), 5),
		},
	})

	return client.Run(ctx, func(ctx context.Context) error {
		// Login (só pede código/senha na primeira vez; depois usa a sessão).
		flow := auth.NewFlow(termAuth{phone: phone}, auth.SendCodeOptions{})
		if err := client.Auth().IfNecessary(ctx, flow); err != nil {
			return fmt.Errorf("auth: %w", err)
		}

		api := client.API()

		// 1) Leitura sequencial e rate-limited do histórico do Saved Messages.
		type item struct {
			idx  int
			id   int
			date time.Time
			text string
		}
		var items []item

		log.Println("lendo Saved Messages...")
		iter := query.Messages(api).
			GetHistory(&tg.InputPeerSelf{}).
			BatchSize(batch).
			Iter()

		i := 0
		for iter.Next(ctx) {
			m, ok := iter.Value().Msg.(*tg.Message)
			if !ok {
				continue // mensagens de serviço, etc.
			}
			// "só texto mesmo": sem mídia e com conteúdo não-vazio.
			if m.Media != nil {
				continue
			}
			if strings.TrimSpace(m.Message) == "" {
				continue
			}
			items = append(items, item{
				idx:  i,
				id:   m.ID,
				date: time.Unix(int64(m.Date), 0),
				text: m.Message,
			})
			i++
		}
		if err := iter.Err(); err != nil {
			return fmt.Errorf("lendo histórico: %w", err)
		}
		log.Printf("%d mensagens de texto encontradas", len(items))

		// 2) Processamento em paralelo (pool de workers).
		//    processText é o ponto onde você pluga trabalho pesado por mensagem.
		results := make([]item, len(items))
		jobs := make(chan int)
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for k := range jobs {
					it := items[k]
					it.text = processText(it.text)
					results[k] = it // índice fixo => ordem preservada
				}
			}()
		}
		for k := range items {
			jobs <- k
		}
		close(jobs)
		wg.Wait()

		// 3) Escrita na ordem original (mais recente -> mais antiga, como o Telegram devolve).
		sort.SliceStable(results, func(a, b int) bool { return results[a].idx < results[b].idx })

		f, err := os.Create(outPath)
		if err != nil {
			return err
		}
		defer f.Close()
		bw := bufio.NewWriter(f)
		defer bw.Flush()

		for _, it := range results {
			fmt.Fprintf(bw, "----- msg %d | %s -----\n%s\n\n",
				it.id, it.date.Format("2006-01-02 15:04:05"), it.text)
		}

		log.Printf("gravado em %s", outPath)
		return nil
	})
}

// processText é onde entra o trabalho paralelo por mensagem.
// Por padrão é identidade; troque por tradução, limpeza, chamada de API, etc.
func processText(s string) string {
	return s
}

// --- Autenticação via terminal ---

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

// sessionStorageFile persiste a sessão em disco para não relogar toda hora.
type sessionStorageFile = session.FileStorage
