#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

# Compila (rápido se já estiver buildado) e executa o exportador de links.
go build -o savelinks ./cmd/savelinks

# Exporta APENAS as mensagens que são links do Saved Messages para um .txt.
# O formato é o mesmo do processtelegram, então o arquivo gerado também serve
# de fonte de IDs para o deletesaved:
#
#   ./run-links.sh                        # gera saved_links.txt
#   ./run-links.sh --out outros.txt       # usa outro arquivo de saída
#
./savelinks --out saved_links.txt "$@"
