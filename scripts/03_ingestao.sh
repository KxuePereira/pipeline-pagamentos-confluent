#!/usr/bin/env bash
# =====================================================================
# Camada 3 — Ingestão (CDC)
# 1. Prepara o Postgres: schema, seed determinístico (seed=42) e o que o
#    conector exige (wal_level=logical, publication, REPLICA IDENTITY FULL,
#    usuário só-leitura com REPLICATION).
# 2. Sobe o PostgresCdcSourceV2 com a identidade <prefixo>-producer.
# 3. Roda insert/update/delete na conta 9001 e captura os 3 envelopes.
# =====================================================================
source "$(dirname "$0")/lib.sh"
load_env
require confluent jq psql python3
require_login
: "${KAFKA_ID:?rode antes a camada 1}"

P="$CC_PREFIX"
EV="03-ingestao.txt"
CONNECTOR_NAME="$(jq -r .name "$ROOT/connectors/cdc.json")"
confluent environment use "$ENV_ID" >/dev/null
confluent kafka cluster use "$KAFKA_ID" >/dev/null

# ---------------------------------------------------------------- banco
step "Postgres: pré-requisitos do CDC"
WAL="$(psql_db -tA -c 'SHOW wal_level')"
[[ "$WAL" == "logical" ]] || die "wal_level=$WAL. No Neon: Project settings → Logical Replication → Enable."
ok "wal_level=logical"

REPL="$(psql_db -tA -c "SELECT rolreplication OR pg_has_role(oid, 'neon_superuser', 'MEMBER')
                        FROM pg_roles WHERE rolname = '$CDC_DB_USER'" 2>/dev/null \
        || psql_db -tA -c "SELECT rolreplication FROM pg_roles WHERE rolname = '$CDC_DB_USER'")"
[[ -n "$REPL" ]] || die "Papel $CDC_DB_USER não existe. Crie no console do Neon (Roles → New role)."
[[ "$REPL" == "t" ]] || warn "$CDC_DB_USER sem REPLICATION aparente — o conector pode falhar ao abrir o slot."
ok "papel $CDC_DB_USER encontrado"

step "Schema e seed"
psql_db -f "$ROOT/sql/postgres/01_schema.sql" 2>/dev/null
TX_COUNT="$(psql_db -tA -c 'SELECT count(*) FROM transactions')"
if [[ "$TX_COUNT" == "0" ]]; then
  psql_db -f "$ROOT/sql/postgres/02_seed.sql" >/dev/null
  ok "seed carregado (seed=42)"
else
  ok "tabela transactions já tem $TX_COUNT linhas — seed não reaplicado"
fi

step "Publication, REPLICA IDENTITY FULL e grants do $CDC_DB_USER"
psql_db -f "$ROOT/sql/postgres/03_cdc_setup.sql" >/dev/null
ok "desafio_pub pronta"

# ---------------------------------------------------------------- conector
step "Conector $CONNECTOR_NAME (PostgresCdcSourceV2)"
RENDERED="$STATE_DIR/cdc.rendered.json"   # tem a senha do banco: fica em .state/, fora do Git
export PRODUCER_SA_ID CC_PREFIX CDC_DB_HOST CDC_DB_PORT CDC_DB_USER CDC_DB_PASSWORD CDC_DB_NAME
jq 'with_entries(.value |= (if type == "string" and test("^\\$\\{[A-Z_]+\\}$")
                            then ($ENV[.[2:-1]] // error("variável \(.) não definida")) else . end))' \
   "$ROOT/connectors/cdc.json" > "$RENDERED"
chmod 600 "$RENDERED"

CONNECTOR_ID="$(id_by_name "$(confluent connect cluster list --cluster "$KAFKA_ID" -o json)" "$CONNECTOR_NAME")"
if [[ -z "$CONNECTOR_ID" ]]; then
  CONNECTOR_ID="$(confluent connect cluster create --config-file "$RENDERED" --cluster "$KAFKA_ID" -o json | jq -r .id)"
  ok "criado $CONNECTOR_ID"
else ok "já existe $CONNECTOR_ID"; fi
state_set CONNECTOR_ID "$CONNECTOR_ID"

connector_status() {
  confluent connect cluster describe "$CONNECTOR_ID" --cluster "$KAFKA_ID" -o json \
    | jq -r '.connector.status // .status // "?"'
}
connector_running() {
  local s; s="$(connector_status)"
  [[ "$s" == "FAILED" ]] && { confluent connect cluster describe "$CONNECTOR_ID" --cluster "$KAFKA_ID" >&2; die "conector FAILED (veja o trace acima)"; }
  [[ "$s" == "RUNNING" ]]
}
wait_for "conector RUNNING" 900 connector_running

topics_ready() {
  confluent kafka topic list --cluster "$KAFKA_ID" -o json \
    | jq -e --arg p "$P.public." '[.[] | select(.name | startswith($p))] | length >= 5'
}
wait_for "5 tópicos $P.public.* criados pelo snapshot" 600 topics_ready

# ---------------------------------------------------------------- demo I/U/D
step "Demo de CDC: insert -> update -> delete na conta 9001"
psql_db -f "$ROOT/sql/postgres/04_cdc_demo.sql" >/dev/null
ok "3 operações executadas no Postgres"
sleep 15

evid_header "$EV" "Camada 3 — Ingestão (CDC)"
evid "$EV" "Conector" confluent connect cluster describe "$CONNECTOR_ID" --cluster "$KAFKA_ID"
evid "$EV" "Tópicos criados pelo conector" bash -c "confluent kafka topic list --cluster $KAFKA_ID | grep -E 'Name|----|$P\.|heartbeat'"
evid "$EV" "Linhas no Postgres" psql_db -c "SELECT 'customers' t, count(*) FROM customers UNION ALL SELECT 'accounts', count(*) FROM accounts UNION ALL SELECT 'cards', count(*) FROM cards UNION ALL SELECT 'merchants', count(*) FROM merchants UNION ALL SELECT 'transactions', count(*) FROM transactions"
evid "$EV" "Slot de replicação" psql_db -c "SELECT slot_name, plugin, active, confirmed_flush_lsn FROM pg_replication_slots"

step "Envelopes da conta 9001 (op=c, u, d + tombstone) -> evidencias/03-envelopes-cdc.jsonl"
rm -f "$EVID_DIR/03-envelopes-cdc.jsonl"
python3 "$ROOT/consumer/consume.py" --topic "$P.public.accounts" --match account_id=9001 \
  --timeout 30 --out "$EVID_DIR/03-envelopes-cdc.jsonl" || warn "consumo falhou (chave nova ainda propagando?) — rode de novo: ./setup.sh 3"
ok "Camada 3 pronta"
