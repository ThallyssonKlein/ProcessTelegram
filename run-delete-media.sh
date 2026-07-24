#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

# Compila (rápido se já estiver buildado) e apaga do Saved Messages as mensagens
# de cada mídia baixada pelo savemedia (lê o msg id do prefixo do nome do arquivo).
#
# ATENÇÃO: apagar é PERMANENTE. Por padrão roda em dry-run (só lista).
# Passe --confirm para apagar de verdade:
#
#   ./run-delete-media.sh                 # dry-run: só lista o que apagaria
#   ./run-delete-media.sh --confirm       # apaga de verdade
#   ./run-delete-media.sh --dir fotos     # varre outra pasta
#
go build -o deletemedia ./cmd/deletemedia

./deletemedia --dir media "$@"
