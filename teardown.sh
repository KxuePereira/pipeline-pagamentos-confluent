#!/usr/bin/env bash
# =====================================================================
# teardown.sh — derruba TUDO que o setup criou, na ordem certa:
#   statements Flink -> compute pool -> conector -> slot/publication no
#   Postgres -> chaves de API pessoais -> service accounts -> cluster ->
#   ambiente. No fim, confere que não sobrou nada (evidência 99).
#
#   ./teardown.sh         # pede confirmação
#   ./teardown.sh --yes   # sem perguntar
# =====================================================================
source "$(dirname "$0")/scripts/lib.sh"
load_env
require confluent jq psql
require_login

P="$CC_PREFIX"
EV="99-teardown.txt"

if [[ "${1:-}" != "--yes" ]]; then
  echo "Isto apaga o ambiente '${CC_ENV_NAME}' (${ENV_ID:-?}), o cluster, o conector, os statements,"
  echo "as service accounts $P-* e o slot de replicação no Postgres."
  read -r -p "Digite 'derrubar' para confirmar: " ans
  [[ "$ans" == "derrubar" ]] || die "cancelado"
fi

try() { "$@" >/dev/null 2>&1 && ok "$*" || warn "ignorado (já não existe?): $*"; }

if [[ -n "${ENV_ID:-}" ]]; then
  confluent environment use "$ENV_ID" >/dev/null 2>&1 || true

  step "Statements Flink"
  for name in $(confluent flink statement list --environment "$ENV_ID" -o json 2>/dev/null \
                | jq -r --arg p "$P-" '.[]? | select(.name | startswith($p)) | .name'); do
    try confluent flink statement delete "$name" --environment "$ENV_ID" --force
  done

  step "Compute pool"
  [[ -n "${POOL_ID:-}" ]] && try confluent flink compute-pool delete "$POOL_ID" --environment "$ENV_ID" --force

  step "Conector"
  [[ -n "${CONNECTOR_ID:-}" && -n "${KAFKA_ID:-}" ]] \
    && try confluent connect cluster delete "$CONNECTOR_ID" --cluster "$KAFKA_ID" --environment "$ENV_ID" --force
fi

step "Postgres: slot de replicação e publication"
# o slot só pode ser removido quando o conector soltar a conexão
for i in $(seq 1 12); do
  ACTIVE="$(psql_db -tA -c "SELECT active FROM pg_replication_slots WHERE slot_name = 'desafio_slot'" || echo "")"
  [[ "$ACTIVE" == "t" ]] || break
  echo "  … slot ainda ativo, aguardando o conector desligar ($i/12)"; sleep 10
done
psql_db -f "$ROOT/sql/postgres/99_cleanup.sql" >/dev/null && ok "slot e publication removidos" \
  || warn "não consegui limpar o Postgres — rode sql/postgres/99_cleanup.sql à mão"

step "Chaves de API pessoais"
[[ -n "${SR_ADMIN_KEY:-}" ]] && try confluent api-key delete "$SR_ADMIN_KEY" --force

step "Service accounts (apagam junto as chaves e role bindings delas)"
for sa in "${PRODUCER_SA_ID:-}" "${CONSUMER_SA_ID:-}" "${FLINK_SA_ID:-}" "${METRICS_SA_ID:-}"; do
  [[ -n "$sa" ]] && try confluent iam service-account delete "$sa" --force
done

# --- conferência ANTES de apagar o ambiente (depois dele não há onde listar)
evid_header "$EV" "Teardown — conferência"
if [[ -n "${ENV_ID:-}" && -n "${KAFKA_ID:-}" ]]; then
  evid "$EV" "Conectores restantes (esperado: nenhum)" confluent connect cluster list --cluster "$KAFKA_ID" --environment "$ENV_ID"
  evid "$EV" "Statements restantes (esperado: nenhum)" confluent flink statement list --environment "$ENV_ID"
fi

if [[ -n "${ENV_ID:-}" ]]; then
  step "Cluster e ambiente"
  [[ -n "${KAFKA_ID:-}" ]] && try confluent kafka cluster delete "$KAFKA_ID" --environment "$ENV_ID" --force
  try confluent environment delete "$ENV_ID" --force
fi

evid "$EV" "Service accounts $P-* restantes (esperado: nenhuma)" bash -c "confluent iam service-account list | grep '$P-' || echo 'None found.'"
evid "$EV" "Ambiente $CC_ENV_NAME (esperado: não aparece)" bash -c "confluent environment list | grep '$CC_ENV_NAME' || echo 'None found.'"
evid "$EV" "Slot no Postgres (esperado: 0 linhas)" psql_db -c "SELECT slot_name FROM pg_replication_slots WHERE slot_name = 'desafio_slot'"

# guarda os IDs antigos só para referência e limpa o estado
mv "$IDS_FILE" "$STATE_DIR/ids.derrubado.env" 2>/dev/null || true
: > "$SECRETS_FILE"
ok "Tudo derrubado. Daqui a 1-3 dias rode './scripts/05_operacao.sh custo' para fechar o custo."
