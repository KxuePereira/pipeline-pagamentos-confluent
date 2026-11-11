#!/usr/bin/env bash
# =====================================================================
# Camada 5 — Operação: métricas, acesso e custo
#   * Metrics API com a identidade <prefixo>-metrics (MetricsViewer, nada mais)
#       - received_bytes por tópico (pico na última hora)
#       - consumer lag por grupo
#   * Quem pode o quê (ACLs + role bindings)
#   * Custo: confluent billing cost list (o billing atrasa ~1-3 dias)
# Rode com o pipeline ligado e, de novo, um ou dois dias depois do teardown
# para pegar o custo fechado:  ./scripts/05_operacao.sh custo
# =====================================================================
source "$(dirname "$0")/lib.sh"
load_env
require confluent jq curl
require_login

P="$CC_PREFIX"
EV="05-operacao.txt"
MODE="${1:-tudo}"
METRICS_URL="https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"

metrics() {  # metrics métrica agrupar_por
  local body
  body="$(jq -n --arg m "$1" --arg g "$2" --arg k "$KAFKA_ID" --arg i "$(iso_ago 1)/$(date -u +%Y-%m-%dT%H:%M:%SZ)" '{
    aggregations: [{metric: $m}],
    filter: {field: "resource.kafka.id", op: "EQ", value: $k},
    granularity: "PT1M",
    group_by: [$g],
    intervals: [$i],
    limit: 1000 }')"
  curl -sS -u "${METRICS_CLOUD_KEY}:${METRICS_CLOUD_SECRET}" -H 'Content-Type: application/json' \
       -X POST "$METRICS_URL" --data "$body"
}

if [[ "$MODE" != "custo" ]]; then
  : "${KAFKA_ID:?rode antes a camada 1}"
  evid_header "$EV" "Camada 5 — Operação"

  step "Observabilidade (Metrics API, identidade $METRICS_SA_ID)"
  {
    echo; echo "### Pico de received_bytes por tópico (bytes/min, última hora)"
    metrics "io.confluent.kafka.server/received_bytes" "metric.topic" \
      | jq -r '(.data // []) | group_by(."metric.topic")
               | map({topico: .[0]."metric.topic", pico_bytes_min: (map(.value) | max), total_bytes: (map(.value) | add)})
               | sort_by(-.total_bytes)[] | "\(.topico)\tpico=\(.pico_bytes_min)\ttotal=\(.total_bytes)"'
    echo; echo "### Consumer lag por grupo (offsets, último ponto)"
    metrics "io.confluent.kafka.server/consumer_lag_offsets" "metric.consumer_group_id" \
      | jq -r '(.data // []) | group_by(."metric.consumer_group_id")
               | map({grupo: .[0]."metric.consumer_group_id", lag: (sort_by(.timestamp) | last | .value)})[]
               | "\(.grupo)\tlag=\(.lag)"'
  } 2>&1 | tee -a "$EVID_DIR/$EV"

  step "Segurança: quem pode o quê"
  for sa in "$PRODUCER_SA_ID" "$CONSUMER_SA_ID"; do
    evid "$EV" "ACLs de $sa" confluent kafka acl list --service-account "$sa" --cluster "$KAFKA_ID" --environment "$ENV_ID"
  done
  evid "$EV" "Role bindings do Flink ($FLINK_SA_ID)" confluent iam rbac role-binding list --principal "User:$FLINK_SA_ID" --environment "$ENV_ID" --inclusive
  evid "$EV" "Chaves de API existentes (só IDs, nunca segredos)" bash -c "confluent api-key list | grep -E 'Key|----|$P-|${SR_ID:-lsrc}|${KAFKA_ID}'"

  step "Saúde do pipeline"
  evid "$EV" "Conector" confluent connect cluster describe "$CONNECTOR_ID" --cluster "$KAFKA_ID"
  evid "$EV" "Statements Flink" confluent flink statement list --environment "$ENV_ID"
  evid "$EV" "Slot de replicação no Postgres (lag de WAL)" psql_db -c \
    "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS wal_retido FROM pg_replication_slots"
fi

step "Custo (confluent billing cost list)"
evid "$EV" "Custos dos últimos 7 dias" confluent billing cost list --start-date "$(day_ago 7)" --end-date "$(date -u +%Y-%m-%d)"
warn "o billing fecha com atraso: rode './scripts/05_operacao.sh custo' de novo 1-3 dias depois e cole no README"
ok "Camada 5 -> evidencias/$EV"
