#!/usr/bin/env bash
# =====================================================================
# Camada 1 — Fundação
# Ambiente + cluster Basic + Schema Registry + identidades separadas:
#   <prefixo>-producer  escreve (é a identidade do conector de CDC)
#   <prefixo>-consumer  lê      (aplicação que consome os alertas)
#   <prefixo>-flink     processa (roda os statements contínuos do Flink)
#   <prefixo>-metrics   só lê métricas (Metrics API)
# Sua conta pessoal só é usada para criar e destruir (setup/teardown).
# Re-executável: o que já existe é reaproveitado.
# =====================================================================
source "$(dirname "$0")/lib.sh"
load_env
require confluent jq
require_login

P="$CC_PREFIX"
EV="01-fundacao.txt"

# ---------------------------------------------------------------- ambiente
step "Ambiente $CC_ENV_NAME"
ENV_ID="$(id_by_name "$(confluent environment list -o json)" "$CC_ENV_NAME")"
if [[ -z "$ENV_ID" ]]; then
  ENV_ID="$(confluent environment create "$CC_ENV_NAME" --governance-package essentials -o json | jq -r .id)"
  ok "criado $ENV_ID"
else ok "já existe $ENV_ID"; fi
state_set ENV_ID "$ENV_ID"
confluent environment use "$ENV_ID" >/dev/null

# ---------------------------------------------------------------- cluster
step "Cluster Basic $CC_CLUSTER_NAME ($CC_CLOUD/$CC_REGION)"
KAFKA_ID="$(id_by_name "$(confluent kafka cluster list --environment "$ENV_ID" -o json)" "$CC_CLUSTER_NAME")"
if [[ -z "$KAFKA_ID" ]]; then
  KAFKA_ID="$(confluent kafka cluster create "$CC_CLUSTER_NAME" --cloud "$CC_CLOUD" --region "$CC_REGION" \
              --type basic --environment "$ENV_ID" -o json | jq -r .id)"
  ok "criado $KAFKA_ID"
else ok "já existe $KAFKA_ID"; fi
state_set KAFKA_ID "$KAFKA_ID"

cluster_up() {
  confluent kafka cluster describe "$KAFKA_ID" --environment "$ENV_ID" -o json \
    | jq -e '(.status // "") | test("^(UP|PROVISIONED|RUNNING)$")'
}
wait_for "cluster $KAFKA_ID no ar" 900 cluster_up
confluent kafka cluster use "$KAFKA_ID" >/dev/null
BOOTSTRAP="$(confluent kafka cluster describe "$KAFKA_ID" --environment "$ENV_ID" -o json | jq -r .endpoint | sed 's#^SASL_SSL://##')"
state_set BOOTSTRAP "$BOOTSTRAP"
ok "bootstrap $BOOTSTRAP"

# ---------------------------------------------------------------- schema registry
step "Schema Registry (Stream Governance Essentials)"
sr_json() { confluent schema-registry cluster describe --environment "$ENV_ID" -o json; }
wait_for "Schema Registry do ambiente" 600 sr_json
SR="$(sr_json)"
SR_ID="$(jq -r '.cluster // .cluster_id // .id' <<<"$SR")"
SR_URL="$(jq -r '.endpoint_url // .url // .endpoint' <<<"$SR")"
state_set SR_ID "$SR_ID"; state_set SR_URL "$SR_URL"
ok "$SR_ID  $SR_URL"

# ---------------------------------------------------------------- identidades
step "Service accounts"
sa_get_or_create() {  # nome descrição -> id
  local id
  id="$(id_by_name "$(confluent iam service-account list -o json)" "$1")"
  [[ -n "$id" ]] || id="$(confluent iam service-account create "$1" --description "$2" -o json | jq -r .id)"
  echo "$id"
}
PRODUCER_SA_ID="$(sa_get_or_create "$P-producer" "Escreve: conector de CDC Postgres -> $P.public.*")"
CONSUMER_SA_ID="$(sa_get_or_create "$P-consumer" "Le: topicos $P.* (alertas e eventos CDC)")"
FLINK_SA_ID="$(sa_get_or_create "$P-flink" "Processa: statements Flink SQL do pipeline")"
METRICS_SA_ID="$(sa_get_or_create "$P-metrics" "Observabilidade: somente Metrics API")"
state_set PRODUCER_SA_ID "$PRODUCER_SA_ID"; state_set CONSUMER_SA_ID "$CONSUMER_SA_ID"
state_set FLINK_SA_ID "$FLINK_SA_ID";       state_set METRICS_SA_ID "$METRICS_SA_ID"
ok "producer=$PRODUCER_SA_ID consumer=$CONSUMER_SA_ID flink=$FLINK_SA_ID metrics=$METRICS_SA_ID"

# ---------------------------------------------------------------- ACLs (Kafka)
acl() { confluent kafka acl create --allow --cluster "$KAFKA_ID" --environment "$ENV_ID" "$@" >/dev/null 2>&1 \
        || warn "ACL já existe ou falhou: $*"; }

step "ACLs do producer (conector de CDC)"
acl --service-account "$PRODUCER_SA_ID" --operations describe --cluster-scope
# o nome da operação mudou entre versões do CLI (describe-configs / describe_configs)
confluent kafka acl create --allow --cluster "$KAFKA_ID" --environment "$ENV_ID" --service-account "$PRODUCER_SA_ID" \
  --operations describe-configs --cluster-scope >/dev/null 2>&1 \
  || acl --service-account "$PRODUCER_SA_ID" --operations describe_configs --cluster-scope
acl --service-account "$PRODUCER_SA_ID" --operations create,write,describe --topic "$P.public." --prefix
# tópico de heartbeat do Debezium: __debezium-heartbeat.<topic.prefix>
acl --service-account "$PRODUCER_SA_ID" --operations create,write,describe --topic "__debezium-heartbeat" --prefix
ok "WRITE/CREATE só em $P.public.* (e heartbeat)"

step "ACLs do consumer"
acl --service-account "$CONSUMER_SA_ID" --operations read,describe --topic "$P." --prefix
acl --service-account "$CONSUMER_SA_ID" --operations read,describe --consumer-group "$P-" --prefix
ok "READ em $P.* e grupos $P-*"

# ---------------------------------------------------------------- RBAC (Flink e Schema Registry)
rb() { confluent iam rbac role-binding create "$@" >/dev/null 2>&1 || warn "role binding já existe ou falhou: $*"; }
K=(--environment "$ENV_ID" --cloud-cluster "$KAFKA_ID" --kafka-cluster "$KAFKA_ID")
S=(--environment "$ENV_ID" --cloud-cluster "$SR_ID" --schema-registry-cluster "$SR_ID")

step "RBAC do Flink ($FLINK_SA_ID)"
rb --principal "User:$FLINK_SA_ID" --role FlinkDeveloper --environment "$ENV_ID"
rb --principal "User:$FLINK_SA_ID" --role DeveloperRead  "${K[@]}" --resource "Topic:$P." --prefix
for out in "$P.accounts." "$P.transactions." "$P.fraud."; do
  rb --principal "User:$FLINK_SA_ID" --role DeveloperWrite "${K[@]}" --resource "Topic:$out" --prefix
done
rb --principal "User:$FLINK_SA_ID" --role DeveloperRead  "${S[@]}" --resource "Subject:$P." --prefix
rb --principal "User:$FLINK_SA_ID" --role DeveloperRead  "${K[@]}" --resource "Transactional-Id:_confluent-flink_" --prefix
rb --principal "User:$FLINK_SA_ID" --role DeveloperWrite "${K[@]}" --resource "Transactional-Id:_confluent-flink_" --prefix
ok "lê $P.*, escreve só em $P.accounts.* / $P.transactions.* / $P.fraud.*"

step "RBAC do consumer no Schema Registry e do metrics"
rb --principal "User:$CONSUMER_SA_ID" --role DeveloperRead "${S[@]}" --resource "Subject:$P." --prefix
rb --principal "User:$METRICS_SA_ID" --role MetricsViewer
ok "consumer lê schemas $P.*; metrics só vê métricas"

# ---------------------------------------------------------------- chaves de API
step "Chaves de API (guardadas em .state/secrets.env, fora do Git)"
new_key() {  # var_key var_secret recurso [service-account]
  local vk="$1" vs="$2" res="$3" sa="${4:-}" j
  if [[ -n "${!vk:-}" ]]; then ok "$vk já existe"; return; fi
  local args=(--resource "$res" -o json)
  [[ -n "$sa" ]] && args+=(--service-account "$sa")
  [[ "$res" != "cloud" ]] && args+=(--environment "$ENV_ID")
  j="$(confluent api-key create "${args[@]}")"
  secret_set "$vk" "$(jq -r '.api_key // .key' <<<"$j")"
  secret_set "$vs" "$(jq -r '.api_secret // .secret' <<<"$j")"
  ok "$vk criada"
}
new_key CONSUMER_KAFKA_KEY CONSUMER_KAFKA_SECRET "$KAFKA_ID" "$CONSUMER_SA_ID"
new_key CONSUMER_SR_KEY    CONSUMER_SR_SECRET    "$SR_ID"    "$CONSUMER_SA_ID"
new_key METRICS_CLOUD_KEY  METRICS_CLOUD_SECRET  cloud       "$METRICS_SA_ID"
# chave pessoal de Schema Registry: só para o teste de contrato (camada 2) e apagada no teardown
new_key SR_ADMIN_KEY       SR_ADMIN_SECRET       "$SR_ID"
warn "chaves novas levam ~1-2 min para propagar"

# ---------------------------------------------------------------- evidência
step "Evidência -> evidencias/$EV"
evid_header "$EV" "Camada 1 — Fundação"
evid "$EV" "Ambiente e cluster" confluent kafka cluster describe "$KAFKA_ID" --environment "$ENV_ID"
evid "$EV" "Service accounts do desafio" bash -c "confluent iam service-account list | grep -E 'ID|----|$P-'"
for sa in "$PRODUCER_SA_ID" "$CONSUMER_SA_ID"; do
  evid "$EV" "ACLs de $sa" confluent kafka acl list --service-account "$sa" --cluster "$KAFKA_ID" --environment "$ENV_ID"
done
for sa in "$FLINK_SA_ID" "$CONSUMER_SA_ID" "$METRICS_SA_ID"; do
  evid "$EV" "Role bindings de $sa" confluent iam rbac role-binding list --principal "User:$sa" --environment "$ENV_ID" --inclusive
done
ok "Camada 1 pronta"
