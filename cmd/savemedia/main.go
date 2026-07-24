// Command savemedia lê o seu "Saved Messages" (peer self) e baixa TODAS as
// mídias e arquivos (fotos, vídeos, áudios, documentos) para uma pasta local,
// sempre na maior qualidade disponível (o maior PhotoSize das fotos e o
// documento original, sem recompressão).
//
// A leitura do histórico é sequencial e rate-limited (é aí que os limites da
// API do Telegram pesam); os downloads rodam num pool de workers em paralelo,
// e cada arquivo grande é baixado em várias threads. Todo o tráfego passa pelos
// middlewares de floodwait + ratelimit, então o paralelismo respeita o teto da
// API. Re-rodar o comando pula os arquivos que já existem com o tamanho certo.
//
//	./savemedia                         # baixa para ./media
//	./savemedia --out fotos --workers 6 # outra pasta, mais workers
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/telegram/query"
	"github.com/gotd/td/tg"
	"golang.org/x/time/rate"
)

func main() {
	var (
		out         = flag.String("out", "media", "pasta de saída para as mídias")
		workers     = flag.Int("workers", 4, "arquivos baixados em paralelo")
		threads     = flag.Int("threads", 4, "threads por arquivo (para arquivos grandes)")
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
	if err := run(ctx, apiID, apiHash, phone, *sessionFile, *out, *workers, *threads, *batch); err != nil {
		log.Fatal(err)
	}
}

// mediaJob descreve um arquivo a baixar: de onde vem, para onde vai e o tamanho
// esperado (usado para pular downloads já completos).
type mediaJob struct {
	msgID int
	date  time.Time
	loc   tg.InputFileLocationClass
	name  string // nome final do arquivo (já com prefixo do msg id)
	size  int64  // tamanho esperado em bytes (0 = desconhecido)
	kind  string // "foto", "documento", etc. — só para log
	dir   string // subpasta por tipo: "fotos", "videos", "documentos", ...
}

func run(ctx context.Context, apiID int, apiHash, phone, sessionPath, outDir string, workers, threads, batch int) error {
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return fmt.Errorf("criando pasta %s: %w", outDir, err)
	}

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
		flow := auth.NewFlow(termAuth{phone: phone}, auth.SendCodeOptions{})
		if err := client.Auth().IfNecessary(ctx, flow); err != nil {
			return fmt.Errorf("auth: %w", err)
		}
		api := client.API()

		// 1) Varredura sequencial e rate-limited do histórico, montando a lista
		//    de jobs. Não faz download aqui — só coleta os locais das mídias.
		log.Println("lendo Saved Messages (procurando mídias)...")
		iter := query.Messages(api).
			GetHistory(&tg.InputPeerSelf{}).
			BatchSize(batch).
			Iter()

		var jobs []mediaJob
		var scanned int
		for iter.Next(ctx) {
			scanned++
			if scanned%500 == 0 {
				log.Printf("  ...%d mensagens varridas, %d mídias até agora", scanned, len(jobs))
			}
			m, ok := iter.Value().Msg.(*tg.Message)
			if !ok || m.Media == nil {
				continue
			}
			if j, ok := jobFromMedia(m); ok {
				jobs = append(jobs, j)
			}
		}
		if err := iter.Err(); err != nil {
			return fmt.Errorf("lendo histórico: %w", err)
		}
		log.Printf("varredura concluída: %d mensagens, %d mídias para baixar", scanned, len(jobs))
		if len(jobs) == 0 {
			return nil
		}

		// 2) Download em paralelo (pool de workers). O middleware de ratelimit
		//    garante que o paralelismo não estoure o teto da API do Telegram.
		dl := downloader.NewDownloader()
		var (
			downloaded int64
			skipped    int64
			failed     int64
			bytesGot   int64
			nextIdx    int64 // contador só para numerar os logs (X/N)
		)
		total := len(jobs)

		jobsCh := make(chan mediaJob)
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				for j := range jobsCh {
					n := atomic.AddInt64(&nextIdx, 1)
					dir := filepath.Join(outDir, j.dir)
					if err := os.MkdirAll(dir, 0755); err != nil {
						atomic.AddInt64(&failed, 1)
						log.Printf("[%d/%d] FALHA ao criar %s: %v", n, total, dir, err)
						continue
					}
					rel := filepath.Join(j.dir, j.name)
					path := filepath.Join(dir, j.name)

					// Pula se já existe com o tamanho esperado (retomada).
					if fi, err := os.Stat(path); err == nil && (j.size == 0 || fi.Size() == j.size) {
						atomic.AddInt64(&skipped, 1)
						log.Printf("[%d/%d] pulando (já existe) msg %d -> %s", n, total, j.msgID, rel)
						continue
					}

					log.Printf("[%d/%d] baixando msg %d (%s, %s) -> %s",
						n, total, j.msgID, j.kind, humanSize(j.size), rel)
					start := time.Now()

					tmp := path + ".part"
					if _, err := dl.Download(api, j.loc).WithThreads(threads).ToPath(ctx, tmp); err != nil {
						atomic.AddInt64(&failed, 1)
						log.Printf("[%d/%d] FALHA msg %d (%s): %v", n, total, j.msgID, rel, err)
						os.Remove(tmp)
						continue
					}
					if err := os.Rename(tmp, path); err != nil {
						atomic.AddInt64(&failed, 1)
						log.Printf("[%d/%d] FALHA ao finalizar %s: %v", n, total, rel, err)
						os.Remove(tmp)
						continue
					}

					got := int64(0)
					if fi, err := os.Stat(path); err == nil {
						got = fi.Size()
					}
					atomic.AddInt64(&bytesGot, got)
					done := atomic.AddInt64(&downloaded, 1)
					log.Printf("[%d/%d] OK msg %d -> %s (%s em %s) | %d baixados",
						n, total, j.msgID, rel, humanSize(got), time.Since(start).Round(time.Millisecond), done)
				}
			}(w)
		}

		for _, j := range jobs {
			jobsCh <- j
		}
		close(jobsCh)
		wg.Wait()

		log.Printf("concluído: %d baixados, %d pulados (já existiam), %d falhas | total %s em %s",
			downloaded, skipped, failed, humanSize(bytesGot), outDir)
		if failed > 0 {
			return fmt.Errorf("%d downloads falharam — rode de novo para retomar (pula os já baixados)", failed)
		}
		return nil
	})
}

// jobFromMedia extrai o local de download, o nome do arquivo e o tamanho de uma
// mensagem com mídia. Retorna ok=false para tipos sem arquivo baixável
// (ex.: webpage, geo, contato).
func jobFromMedia(m *tg.Message) (mediaJob, bool) {
	date := time.Unix(int64(m.Date), 0)
	switch media := m.Media.(type) {
	case *tg.MessageMediaPhoto:
		p, ok := media.Photo.(*tg.Photo)
		if !ok {
			return mediaJob{}, false
		}
		thumb, size := largestPhotoSize(p)
		if thumb == "" {
			return mediaJob{}, false
		}
		name := fmt.Sprintf("%d_photo_%d.jpg", m.ID, p.ID)
		return mediaJob{
			msgID: m.ID, date: date,
			loc:  p.AsInputPhotoFileLocation(thumb),
			name: name, size: size, kind: "foto", dir: "fotos",
		}, true

	case *tg.MessageMediaDocument:
		d, ok := media.Document.(*tg.Document)
		if !ok {
			return mediaJob{}, false
		}
		name := documentName(m.ID, d)
		kind := documentKind(d)
		return mediaJob{
			msgID: m.ID, date: date,
			loc:  d.AsInputDocumentFileLocation(""), // "" = arquivo original, sem thumb
			name: name, size: d.Size, kind: kind, dir: dirForKind(kind),
		}, true
	}
	return mediaJob{}, false
}

// largestPhotoSize escolhe o maior tamanho disponível de uma foto (maior área),
// que é a "qualidade máxima". Ignora os thumbs stripped/cached embutidos.
func largestPhotoSize(p *tg.Photo) (thumbType string, size int64) {
	bestArea := -1
	for _, s := range p.Sizes {
		switch v := s.(type) {
		case *tg.PhotoSize:
			if area := v.W * v.H; area > bestArea {
				bestArea, thumbType, size = area, v.Type, int64(v.Size)
			}
		case *tg.PhotoSizeProgressive:
			mx := 0
			for _, n := range v.Sizes {
				if n > mx {
					mx = n
				}
			}
			if area := v.W * v.H; area > bestArea {
				bestArea, thumbType, size = area, v.Type, int64(mx)
			}
		// PhotoStrippedSize / PhotoCachedSize são previews minúsculos: ignora.
		}
	}
	return thumbType, size
}

// documentName monta um nome de arquivo único e legível: prefixa com o msg id
// (para não colidir e facilitar rastrear) e usa o nome original se houver.
func documentName(msgID int, d *tg.Document) string {
	orig := ""
	for _, a := range d.Attributes {
		if fn, ok := a.(*tg.DocumentAttributeFilename); ok {
			orig = fn.FileName
			break
		}
	}
	if orig == "" {
		ext := extFromMime(d.MimeType)
		orig = fmt.Sprintf("doc_%d%s", d.ID, ext)
	}
	return fmt.Sprintf("%d_%s", msgID, sanitize(orig))
}

// documentKind dá um rótulo curto para o log conforme os atributos do documento.
func documentKind(d *tg.Document) string {
	for _, a := range d.Attributes {
		switch a.(type) {
		case *tg.DocumentAttributeVideo:
			return "vídeo"
		case *tg.DocumentAttributeAudio:
			return "áudio"
		case *tg.DocumentAttributeSticker:
			return "sticker"
		case *tg.DocumentAttributeImageSize:
			return "imagem"
		}
	}
	return "documento"
}

// dirForKind mapeia o rótulo do tipo para o nome da subpasta de saída.
func dirForKind(kind string) string {
	switch kind {
	case "vídeo":
		return "videos"
	case "áudio":
		return "audios"
	case "sticker":
		return "stickers"
	case "imagem":
		return "imagens"
	default:
		return "documentos"
	}
}

func extFromMime(mime string) string {
	switch strings.ToLower(strings.TrimSpace(mime)) {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	case "video/mp4":
		return ".mp4"
	case "video/webm":
		return ".webm"
	case "video/quicktime":
		return ".mov"
	case "audio/mpeg":
		return ".mp3"
	case "audio/ogg":
		return ".ogg"
	case "application/pdf":
		return ".pdf"
	case "application/zip":
		return ".zip"
	}
	return ".bin"
}

// sanitize troca separadores de caminho por "_" para não escapar da pasta.
func sanitize(name string) string {
	name = strings.TrimSpace(name)
	name = strings.NewReplacer("/", "_", "\\", "_", "\x00", "").Replace(name)
	if name == "" || name == "." || name == ".." {
		return "arquivo"
	}
	return name
}

func humanSize(n int64) string {
	if n <= 0 {
		return "? B"
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
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
