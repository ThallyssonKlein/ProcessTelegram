#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

# Compila (rápido se já estiver buildado) e importa um arquivo exportado do
# Telegram para o Notion. Pega NOTION_TOKEN e NOTION_DATABASE_ID do .env.
#
#   # 1ª vez: crie a database e copie o ID pro .env
#   ./run-notion.sh -create-under <PAGE_ID>
#
#   # importa (retoma de onde parou via checkpoint <in>.notion-done)
#   ./run-notion.sh -in saved_messages.txt
#   ./run-notion.sh -in saved_links.txt
#
go build -o tonotion ./cmd/tonotion

./tonotion "$@"
