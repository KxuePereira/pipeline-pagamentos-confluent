#!/usr/bin/env bash
# =====================================================================
# setup.sh — sobe o pipeline do zero, uma camada por vez.
#
#   ./setup.sh          # camadas 1 a 4 (5 roda depois, com tráfego)
#   ./setup.sh 3        # só a camada 3
#   ./setup.sh 2 4      # da camada 2 até a 4
#
# Pré-requisitos: confluent CLI (com `confluent login --save`), jq, psql,
# python3 com `pip install -r generator/requirements.txt`, e o .env preenchido.
# Todos os passos são re-executáveis: o que já existe é reaproveitado.
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")"

FROM="${1:-1}"
TO="${2:-${1:-4}}"
LAYERS=(
  "scripts/01_fundacao.sh"
  "scripts/02_contrato.sh"
  "scripts/03_ingestao.sh"
  "scripts/04_processamento.sh"
  "scripts/05_operacao.sh"
)

for ((i = FROM; i <= TO; i++)); do
  echo
  echo "################################################################"
  echo "#  Camada $i — ${LAYERS[$((i - 1))]}"
  echo "################################################################"
  bash "${LAYERS[$((i - 1))]}"
done

echo
echo "Pronto (camadas $FROM–$TO). Evidências em ./evidencias/ — cole no README."
echo "Para derrubar tudo: ./teardown.sh"
