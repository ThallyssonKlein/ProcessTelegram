#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

# Compila (rápido se já estiver buildado) e baixa todas as mídias do Saved
# Messages na maior qualidade disponível para uma pasta local (ignorada no git).
#
#   ./run-media.sh                        # baixa para ./media
#   ./run-media.sh --out fotos            # outra pasta de saída
#   ./run-media.sh --workers 6 --threads 4  # mais paralelismo
#
# Re-rodar retoma de onde parou: pula os arquivos que já existem com o tamanho
# esperado.
go build -o savemedia ./cmd/savemedia

./savemedia --out media "$@"
