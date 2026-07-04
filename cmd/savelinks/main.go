// Command savelinks lê o seu "Saved Messages" (peer self) e exporta APENAS as
// mensagens que são links: tanto as que vêm com preview de página
// (MessageMediaWebPage) — que o processtelegram descarta por terem "mídia" —
// quanto as que são texto puro contendo uma URL.
//
// A saída usa o mesmo formato de cabeçalho do processtelegram
// ("----- msg <id> | <data> -----"), então o arquivo gerado aqui também pode
// ser usado como fonte de IDs para o deletesaved.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
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

// urlRe detecta uma URL no texto da mensagem (http/https, www. ou t.me/...).
var urlRe = regexp.MustCompile(`(?i)\b(?:https?://|www\.|t\.me/)\S+`)

func main() {
	var (
		out         = flag.String("out", "saved_links.txt", "arquivo de saída")
		batch       = flag.Int("batch", 100, "mensagens por página na leitura (máx 100)")
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

	ctx := context.Background()
	if err := run(ctx, apiID, apiHash, phone, *sessionFile, *out, *batch); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, apiID int, apiHash, phone, sessionPath, outPath string, batch int) error {
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

		f, err := os.Create(outPath)
		if err != nil {
			return err
		}
		defer f.Close()
		bw := bufio.NewWriter(f)
		defer bw.Flush()

		log.Println("lendo Saved Messages (procurando links)...")
		iter := query.Messages(api).
			GetHistory(&tg.InputPeerSelf{}).
			BatchSize(batch).
			Iter()

		n := 0
		for iter.Next(ctx) {
			m, ok := iter.Value().Msg.(*tg.Message)
			if !ok {
				continue // mensagens de serviço, etc.
			}
			if !isLink(m) {
				continue
			}
			// A saída preserva a ordem em que o Telegram devolve (mais recente -> mais antiga).
			fmt.Fprintf(bw, "----- msg %d | %s -----\n%s\n\n",
				m.ID, time.Unix(int64(m.Date), 0).Format("2006-01-02 15:04:05"), m.Message)
			n++
		}
		if err := iter.Err(); err != nil {
			return fmt.Errorf("lendo histórico: %w", err)
		}

		log.Printf("%d mensagens com link exportadas para %s", n, outPath)
		return nil
	})
}

// isLink diz se a mensagem é "um link": ou tem preview de página anexado, ou o
// texto contém uma URL. Descarta mídia de verdade (foto/vídeo/doc) mesmo que a
// legenda tenha um link, para exportar só mensagens que são de fato links.
func isLink(m *tg.Message) bool {
	if strings.TrimSpace(m.Message) == "" {
		return false
	}
	if m.Media != nil {
		if _, isWebPage := m.Media.(*tg.MessageMediaWebPage); !isWebPage {
			return false // foto/vídeo/documento/etc.
		}
		return true // link com preview de página
	}
	return urlRe.MatchString(m.Message) // texto puro contendo uma URL
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
