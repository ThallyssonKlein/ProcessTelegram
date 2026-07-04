// Command deletesaved apaga do seu "Saved Messages" (peer self) exatamente as
// mensagens que foram exportadas para o .txt pelo processtelegram.
//
// Ele NÃO re-varre o Telegram procurando texto puro: ele lê os IDs das linhas
// "----- msg <id> | ... -----" do arquivo de saída e apaga só esses IDs. Assim
// você apaga apenas o que já tinha inspecionado no .txt.
//
// ATENÇÃO: apagar do Saved Messages é PERMANENTE. Por segurança, o padrão é
// dry-run (só lista o que apagaria). Passe --confirm para apagar de verdade.
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
	"github.com/gotd/td/tg"
	"golang.org/x/time/rate"
)

// linha do tipo:  ----- msg 244514 | 2026-07-01 21:31:53 -----
var msgHeader = regexp.MustCompile(`^-----\s*msg\s+(\d+)\s*\|`)

func main() {
	var (
		in          = flag.String("in", "saved_messages.txt", "arquivo .txt gerado pelo processtelegram (fonte dos IDs)")
		confirm     = flag.Bool("confirm", false, "apaga de verdade; sem esta flag apenas lista (dry-run)")
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

	ids, err := readIDs(*in)
	if err != nil {
		log.Fatalf("lendo IDs de %s: %v", *in, err)
	}
	if len(ids) == 0 {
		log.Fatalf("nenhum ID encontrado em %s (o arquivo tem cabeçalhos '----- msg <id> | ... -----'?)", *in)
	}
	log.Printf("%d mensagens serão %s", len(ids), ternary(*confirm, "APAGADAS", "listadas (dry-run)"))

	ctx := context.Background()
	if err := run(ctx, apiID, apiHash, phone, *sessionFile, ids, *confirm); err != nil {
		log.Fatal(err)
	}
}

// readIDs extrai os IDs das linhas de cabeçalho do .txt, na ordem em que aparecem.
func readIDs(path string) ([]int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var ids []int
	seen := make(map[int]bool)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1024*1024), 8*1024*1024) // linhas grandes
	for sc.Scan() {
		m := msgHeader.FindStringSubmatch(sc.Text())
		if m == nil {
			continue
		}
		id, err := strconv.Atoi(m[1])
		if err != nil || seen[id] {
			continue
		}
		seen[id] = true
		ids = append(ids, id)
	}
	return ids, sc.Err()
}

func run(ctx context.Context, apiID int, apiHash, phone, sessionPath string, ids []int, confirm bool) error {
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

		// messages.deleteMessages aceita no máximo 100 IDs por chamada.
		const chunk = 100
		total := 0
		for start := 0; start < len(ids); start += chunk {
			end := start + chunk
			if end > len(ids) {
				end = len(ids)
			}
			batch := make([]int, end-start)
			copy(batch, ids[start:end])

			affected, err := api.MessagesDeleteMessages(ctx, &tg.MessagesDeleteMessagesRequest{
				Revoke: true, // apaga para todos (no Saved Messages é só você mesmo)
				ID:     batch,
			})
			if err != nil {
				return fmt.Errorf("apagando lote %d-%d: %w", start, end, err)
			}
			total += affected.PtsCount
			log.Printf("lote %d-%d enviado (%d IDs)", start, end, len(batch))
		}

		log.Printf("concluído: %d mensagens apagadas do Saved Messages", len(ids))
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

// --- Autenticação via terminal (igual ao programa principal) ---

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
