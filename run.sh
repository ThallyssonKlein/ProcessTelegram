#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

# Compila (rápido se já estiver buildado) e executa.
go build -o processtelegram .

# Lê credenciais do .env automaticamente.
# Na 1ª vez o Telegram manda um código no app — digite quando pedir.
./processtelegram --out saved_messages.txt --workers 8 "$@"
