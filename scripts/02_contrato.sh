#!/usr/bin/env bash
# =====================================================================
# Camada 2 — Contrato
# Registra o contrato canônico da transação (schemas/transaction.v1.avsc)
# no Schema Registry com compatibilidade BACKWARD e testa três mudanças:
#   v2  campo novo opcional com default   -> deve ser ACEITO
#   v3  campo novo obrigatório sem default -> deve ser REJEITADO
#   v4  transaction_id long -> int         -> deve ser REJEITADO
# Usa a API REST do Schema Registry (estável entre versões do CLI).
# =====================================================================
source "$(dirname "$0")/lib.sh"
load_env
require curl jq
: "${SR_URL:?rode antes a camada 1}" "${SR_ADMIN_KEY:?rode antes a camada 1}"

SUBJECT="${CC_PREFIX}.contract.transactions-value"
EV="02-contrato.txt"
SCH="$ROOT/schemas"

body() { jq -n --rawfile s "$1" '{schemaType: "AVRO", schema: $s}'; }

compat() {  # compat arquivo rótulo
  local r
  r="$(sr_curl POST "/compatibility/subjects/$SUBJECT/versions/latest?verbose=true" "$(body "$1")")"
  printf '%-34s -> is_compatible=%s\n' "$2" "$(jq -r '.is_compatible' <<<"$r")"
  jq -r '.messages[]? | "     motivo: " + .' <<<"$r" | head -5
}

register() {  # register arquivo -> imprime resposta (id ou erro)
  sr_curl POST "/subjects/$SUBJECT/versions" "$(body "$1")"; echo
}

step "Schema Registry respondendo?"
wait_for "Schema Registry aceitando a chave pessoal" 240 sr_ok "/subjects"

evid_header "$EV" "Camada 2 — Contrato ($SUBJECT)"
{
  echo; echo "### Compatibilidade padrão do ambiente"
  sr_curl GET "/config"; echo

  echo; echo "### Fixa BACKWARD no subject"
  sr_curl PUT "/config/$SUBJECT" '{"compatibility": "BACKWARD"}'; echo

  echo; echo "### Registra v1 (contrato inicial: decimal(15,2), timestamp-millis, tags PII/PCI)"
  register "$SCH/transaction.v1.avsc"

  echo; echo "### Teste de compatibilidade contra a última versão (v1)"
  compat "$SCH/transaction.v2-campo-opcional.avsc"   "v2 campo opcional com default"

  echo; echo "### v2 passou: registra"
  register "$SCH/transaction.v2-campo-opcional.avsc"

  echo; echo "### Mudanças que quebram (contra a v2)"
  compat "$SCH/transaction.v3-campo-obrigatorio.avsc" "v3 campo obrigatório sem default"
  compat "$SCH/transaction.v4-tipo-estreitado.avsc"   "v4 transaction_id long -> int"

  echo; echo "### Tentar registrar a v3 mesmo assim (o registry tem que recusar com 409)"
  register "$SCH/transaction.v3-campo-obrigatorio.avsc"

  echo; echo "### Versões que ficaram no subject"
  sr_curl GET "/subjects/$SUBJECT/versions"; echo

  echo; echo "### Tags PII/PCI preservadas na versão registrada"
  sr_curl GET "/subjects/$SUBJECT/versions/latest" \
    | jq -r '.schema | fromjson | .fields[] | select(has("confluent:tags")) | "\(.name): \(.["confluent:tags"] | join(","))"'
} 2>&1 | tee -a "$EVID_DIR/$EV"

ok "Camada 2 pronta -> evidencias/$EV"
