# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

A set of Go command-line tools that operate on a Telegram account's **Saved Messages**
(the `InputPeerSelf` "peer") via MTProto (the `gotd/td` library), plus a companion
importer for Notion. The pipeline is: export messages/links to `.txt` → optionally
delete them from Telegram → optionally import them into a Notion database. The `.txt`
format is the shared contract that ties the tools together.

Note: all code comments and CLI help strings are in **Portuguese** — match that when
editing.

## Commands

Each tool has a `run-*.sh` wrapper that does `go build` then runs the binary with sane
defaults. Prefer these; they build fast when already compiled.

```bash
./run.sh              # main.go → processtelegram: export plain-text msgs to saved_messages.txt
./run-links.sh        # cmd/savelinks → export only link messages to saved_links.txt
./run-notion.sh -in saved_messages.txt   # cmd/tonotion → import a .txt into Notion
./run-delete.sh       # cmd/deletesaved on saved_messages.txt (DRY-RUN by default)
./run-delete-links.sh # cmd/deletesaved on saved_links.txt (DRY-RUN by default)
```

Direct build / run of a single sub-command:

```bash
go build -o processtelegram .            # root package = the main exporter
go build -o savelinks   ./cmd/savelinks
go build -o tonotion    ./cmd/tonotion
go build -o deletesaved ./cmd/deletesaved
go vet ./...
```

There are **no tests** in this repo.

### First-time setup for Notion

```bash
./run-notion.sh -create-under <PAGE_ID>   # creates the database, prints its ID
# copy printed ID into .env as NOTION_DATABASE_ID, then:
./run-notion.sh -in saved_links.txt
```

### Deleting is permanent

`deletesaved` defaults to **dry-run** (only lists IDs). Pass `--confirm` to actually
delete. It never re-scans Telegram — it deletes exactly the IDs parsed from the `.txt`,
so you only remove what you already inspected.

## Configuration

Credentials come from a `.env` file (see `.env.example`); real env vars take
precedence. Required: `TG_API_ID`, `TG_API_HASH`, `TG_PHONE` for the Telegram tools;
`NOTION_TOKEN` and `NOTION_DATABASE_ID` for `tonotion`. `.env` and `session.json` are
git-ignored and must never be committed.

Telegram login happens once and is persisted to `session.json` (via `gotd`'s
`session.FileStorage`); the first run prompts for the SMS/app code and optional 2FA
password on the terminal.

## Architecture

### The `.txt` interchange format

Every message block is:

```
----- msg <id> | <YYYY-MM-DD HH:MM:SS> -----
<body, possibly multi-line>

```

- `processtelegram` / `savelinks` **write** this format.
- `deletesaved` parses the `<id>` from each header line (regex, deduped) — that's its
  only input.
- `tonotion` parses id + date + body back into `msgItem`s.

If you change the header format, update the regexes in all three consumers
(`cmd/deletesaved/main.go`, `cmd/tonotion/main.go`) to match.

### Shared Telegram plumbing (duplicated on purpose)

`main.go`, `cmd/savelinks`, and `cmd/deletesaved` each carry their **own copy** of the
same boilerplate — the `termAuth` terminal auth flow, the `sessionStorageFile` alias,
`.env` loading, and the `telegram.NewClient` setup with two middlewares:
`floodwait.NewSimpleWaiter()` (auto-waits on `FLOOD_WAIT`) and a `ratelimit` limiter
(~10 req/s, burst 5). These are copy-pasted, not shared in a package. A change to auth
or rate limiting must be applied to all three files.

History is read sequentially with `query.Messages(api).GetHistory(&tg.InputPeerSelf{})`
— the sequential, rate-limited read is the real bottleneck (Telegram API limits).

- **processtelegram**: keeps only messages with `Media == nil` and non-empty text. Does
  per-message work in a worker pool (default 8 workers) via the `processText` hook
  (currently identity — this is the extension point for translation/cleanup/etc.), then
  sorts back to original order before writing.
- **savelinks**: keeps messages that are links — either `MessageMediaWebPage` previews
  (which processtelegram discards as "media") or plain text matching a URL regex.
  Writes in stream order, no worker pool.
- **deletesaved**: batches `MessagesDeleteMessages` in chunks of 100 (the API max),
  `Revoke: true`.

### Notion importer (`cmd/tonotion`)

Self-contained Notion REST client (no SDK). Key design points:

- **Rate limiting**: Notion caps ~3 req/s per integration. A worker pool (default 5)
  overlaps latency, but all workers share one global `rate.Limiter`. Each request
  retries with exponential backoff (capped 30s) on 429/5xx, honoring the `Retry-After`
  header. This is the write-side bottleneck.
- **Checkpointing**: `<in>.notion-done` (git-ignored `*.notion-done`) appends each
  created msg id. Re-running resumes and skips already-created pages, so imports are
  idempotent/resumable.
- **Schema by type, not name**: `databaseSchema` maps Notion property *types*
  (title/date/number/url/select) to whatever the user named the columns, so import works
  regardless of column names. The database created by `-create-under` uses columns
  Name/URL/Date/Msg ID/Source.
- **Notion limits handled**: title/rich_text truncated to 2000 runes; page body split
  into ≤100 paragraph blocks of ≤2000 runes each. The `Source` select is set to the
  input filename (e.g. `saved_links`).

## Committed binaries

`deletesaved` and `savelinks` binaries are checked into git (large, ~20MB each) even
though `.gitignore` ignores `/processtelegram` and `/tonotion`. `saved_links.txt` and
`saved_messages.txt` are also git-ignored generated output.
