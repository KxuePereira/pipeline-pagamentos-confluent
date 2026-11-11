#!/usr/bin/env bash
# =====================================================================
# Camada 4 — Processamento (Flink SQL)
# Cria o compute pool e submete, em ordem, cada arquivo de sql/flink/:
#   NN_ddl_*.sql  -> rodam com a sua conta (criam/alteram tabelas, uma vez)
#   NN_dml_*.sql  -> rodam com a service account <prefixo>-flink (contínuos)
# Cabeçalhos aceitos nos arquivos:
#   -- run-as: user | flink
#   -- property: chave=valor
# Depois lê o tópico de alertas e confere contra o gabarito do seed.
# =====================================================================
source "$(dirname "$0")/lib.sh"
load_env
require confluent jq python3
require_login
: "${KAFKA_ID:?rode antes a camada 1}" "${FLINK_SA_ID:?rode antes a camada 1}"

P="$CC_PREFIX"
EV="04-processamento.txt"
confluent environment use "$ENV_ID" >/dev/null

# ---------------------------------------------------------------- compute pool
step "Compute pool $CC_POOL_NAME (max ${CC_POOL_MAX_CFU} CFU)"
POOL_ID="$(id_by_name "$(confluent flink compute-pool list --environment "$ENV_ID" -o json)" "$CC_POOL_NAME")"
if [[ -z "$POOL_ID" ]]; then
  POOL_ID="$(confluent flink compute-pool create "$CC_POOL_NAME" --cloud "$CC_CLOUD" --region "$CC_REGION" \
             --max-cfu "$CC_POOL_MAX_CFU" --environment "$ENV_ID" -o json | jq -r .id)"
  ok "criado $POOL_ID"
else ok "já existe $POOL_ID"; fi
state_set POOL_ID "$POOL_ID"
pool_ready() {
  confluent flink compute-pool describe "$POOL_ID" --environment "$ENV_ID" -o json \
    | jq -e '(.status // .phase // "") | test("PROVISIONED|RUNNING|READY")'
}
wait_for "compute pool pronto" 600 pool_ready
confluent flink compute-pool use "$POOL_ID" >/dev/null

# ---------------------------------------------------------------- statements
statement_phase() {
  confluent flink statement describe "$1" --environment "$ENV_ID" -o json 2>/dev/null \
    | jq -r '.status // .phase // empty' || true   # inexistente -> vazio
}

submit() {  # submit arquivo.sql
  local file="$1" stem name runas sql props=() args=() phase
  stem="$(basename "$file" .sql)"
  name="$P-${stem//_/-}"
  runas="$(sed -n 's/^-- run-as: *//p' "$file" | head -1)"
  while IFS= read -r p; do props+=("$p"); done < <(sed -n 's/^-- property: *//p' "$file")
  # tira comentários de linha e troca o prefixo se você mudou CC_PREFIX
  sql="$(grep -v '^[[:space:]]*--' "$file" | sed "s/\`desafio\./\`$P./g")"

  phase="$(statement_phase "$name")"
  if [[ "$phase" =~ ^(RUNNING|COMPLETED|PENDING)$ ]]; then ok "$name já está $phase"; return; fi
  if [[ -n "$phase" ]]; then
    warn "$name existe em estado $phase — recriando"
    confluent flink statement delete "$name" --environment "$ENV_ID" --force >/dev/null
  fi

  args=(--sql "$sql" --compute-pool "$POOL_ID" --database "$KAFKA_ID" --environment "$ENV_ID" --wait)
  [[ "$runas" == "flink" ]] && args+=(--service-account "$FLINK_SA_ID")
  for p in ${props[@]+"${props[@]}"}; do args+=(--property "$p"); done

  echo "  → $name (run-as: ${runas:-user})"
  confluent flink statement create "$name" "${args[@]}" -o json > "$STATE_DIR/stmt-$name.json" \
    || { cat "$STATE_DIR/stmt-$name.json" >&2; die "falhou: $file"; }
  phase="$(statement_phase "$name")"
  [[ "$phase" == "FAILED" ]] && { confluent flink statement describe "$name" --environment "$ENV_ID" >&2; die "$name FAILED"; }
  ok "$name -> $phase"
}

step "DDL (sua conta)"
for f in "$ROOT"/sql/flink/0*_ddl_*.sql; do submit "$f"; done

step "DML contínuo (service account $FLINK_SA_ID)"
for f in "$ROOT"/sql/flink/1*_dml_*.sql; do
  submit "$f"
  sleep 20   # deixa o statement anterior popular o tópico antes do próximo ler
done

# ---------------------------------------------------------------- evidência
step "Esperando o Flink processar o histórico do seed"
sleep 60

evid_header "$EV" "Camada 4 — Processamento (Flink SQL)"
evid "$EV" "Statements" confluent flink statement list --environment "$ENV_ID" --compute-pool "$POOL_ID"
evid "$EV" "Tópicos do pipeline" bash -c "confluent kafka topic list --cluster $KAFKA_ID | grep -E 'Name|----|$P\.'"

step "Alertas em $P.fraud.detected -> evidencias/04-alertas.jsonl"
rm -f "$EVID_DIR/04-alertas.jsonl"
python3 "$ROOT/consumer/consume.py" --topic "$P.fraud.detected" --timeout 45 --out "$EVID_DIR/04-alertas.jsonl"
evid "$EV" "Conferência com o gabarito do seed" python3 "$ROOT/consumer/compare_alerts.py" "$EVID_DIR/04-alertas.jsonl"

cat <<EOF

${_BOLD}Próximo passo (ao vivo):${_RST}
  terminal 1:  python3 generator/stream_transactions.py
  terminal 2:  python3 consumer/consume.py --topic $P.fraud.detected --latest --timeout 300 --out evidencias/04-alertas-ao-vivo.jsonl
  depois:      python3 consumer/compare_alerts.py --ao-vivo evidencias/04-alertas-ao-vivo.jsonl
EOF
ok "Camada 4 pronta"
