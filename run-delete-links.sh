#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

# Compila (rápido se já estiver buildado) e executa o apagador.
go build -o deletesaved ./cmd/deletesaved

# Apaga do Saved Messages exatamente os IDs listados no .txt.
#
# ATENÇÃO: apagar é PERMANENTE. Por padrão roda em dry-run (só lista).
# Passe --confirm para apagar de verdade:
#
#   ./run-delete.sh                       # dry-run: só lista o que apagaria
#   ./run-delete.sh --confirm             # apaga de verdade
#   ./run-delete.sh --in outro.txt        # usa outro arquivo de IDs
#
./deletesaved --in saved_links.txt "$@"
