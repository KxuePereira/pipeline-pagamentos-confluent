#!/usr/bin/env bash
# =====================================================================
# lib.sh — funções compartilhadas pelos scripts das camadas.
# Não rode direto: os scripts fazem `source scripts/lib.sh`.
#
# Estado gerado (IDs e chaves) fica em .state/ — fora do Git:
#   .state/ids.env      IDs de ambiente, cluster, service accounts, pool...
#   .state/secrets.env  chaves de API (chmod 600)
# =====================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="$ROOT/.state"
IDS_FILE="$STATE_DIR/ids.env"
SECRETS_FILE="$STATE_DIR/secrets.env"
EVID_DIR="$ROOT/evidencias"

mkdir -p "$STATE_DIR" "$EVID_DIR"
touch "$IDS_FILE" "$SECRETS_FILE"
chmod 600 "$SECRETS_FILE"

# ---------- saída ------------------------------------------------------
if [[ -t 1 ]]; then _BOLD=$'\e[1m'; _GRN=$'\e[32m'; _YLW=$'\e[33m'; _RED=$'\e[31m'; _CYN=$'\e[36m'; _RST=$'\e[0m'
else _BOLD=""; _GRN=""; _YLW=""; _RED=""; _CYN=""; _RST=""; fi
step() { echo; echo "${_BOLD}${_CYN}==> $*${_RST}"; }
ok()   { echo "${_GRN}  ✔ $*${_RST}"; }
warn() { echo "${_YLW}  ! $*${_RST}" >&2; }
die()  { echo "${_RED}  ✘ $*${_RST}" >&2; exit 1; }

# ---------- ambiente ---------------------------------------------------
load_env() {
  [[ -f "$ROOT/.env" ]] || die "Faltou o .env. Rode: cp .env.example .env  (e preencha)"
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  # shellcheck disable=SC1090
  source "$IDS_FILE"
  # shellcheck disable=SC1090
  source "$SECRETS_FILE"
  set +a
  : "${CC_PREFIX:?defina CC_PREFIX no .env}"
}

require() {
  local missing=()
  for c in "$@"; do command -v "$c" >/dev/null 2>&1 || missing+=("$c"); done
  ((${#missing[@]} == 0)) || die "Instale antes: ${missing[*]}"
}

require_login() {
  confluent environment list -o json >/dev/null 2>&1 \
    || die "CLI sem login. Rode: confluent login --save"
}

# grava/atualiza CHAVE=valor num arquivo de estado (e exporta)
_kv_set() {
  local file="$1" key="$2" val="$3" tmp
  tmp="$(mktemp)"
  grep -v "^${key}=" "$file" > "$tmp" || true
  printf '%s=%q\n' "$key" "$val" >> "$tmp"
  mv "$tmp" "$file"
  export "$key=$val"
}
state_set()  { _kv_set "$IDS_FILE" "$1" "$2"; }
secret_set() { _kv_set "$SECRETS_FILE" "$1" "$2"; chmod 600 "$SECRETS_FILE"; }

# ---------- utilidades -------------------------------------------------
# wait_for "descrição" segundos_max comando...   (repete até o comando dar 0)
wait_for() {
  local what="$1" max="$2"; shift 2
  local waited=0
  until "$@" >/dev/null 2>&1; do
    ((waited >= max)) && die "Tempo esgotado esperando: $what"
    printf '  … aguardando %s (%ss)\r' "$what" "$waited"
    sleep 10; waited=$((waited + 10))
  done
  printf '%*s\r' 70 ''
  ok "$what"
}

# evid ARQUIVO "título" comando...  -> roda, mostra e anexa a saída em evidencias/ARQUIVO
evid() {
  local file="$EVID_DIR/$1" title="$2"; shift 2
  {
    echo
    echo "### $title"
    echo "\$ $*"
    "$@" 2>&1 || echo "(comando retornou erro $?)"
  } | tee -a "$file"
}

evid_header() {
  local file="$EVID_DIR/$1"
  {
    echo "# $2"
    echo "# gerado em $(date -u +%Y-%m-%dT%H:%M:%SZ) por scripts/$(basename "$0")"
  } > "$file"
}

# id de um recurso pelo nome numa lista JSON do CLI (vazio se não existir)
id_by_name() {  # id_by_name "<json>" "<nome>"
  jq -r --arg n "$2" '[.[]? | select(.name == $n or .display_name == $n)][0].id // empty' <<<"$1"
}

psql_db() { psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -X -q "$@"; }

sr_curl() {  # sr_curl METODO caminho [json]
  local method="$1" path="$2" body="${3:-}"
  local args=(-sS -u "${SR_ADMIN_KEY}:${SR_ADMIN_SECRET}" -X "$method"
              -H "Content-Type: application/vnd.schemaregistry.v1+json" "${SR_URL}${path}")
  [[ -n "$body" ]] && args+=(--data "$body")
  curl "${args[@]}"
}

sr_ok() {  # sr_ok caminho  -> 0 se HTTP 2xx (para esperas)
  curl -sSf -o /dev/null -u "${SR_ADMIN_KEY}:${SR_ADMIN_SECRET}" "${SR_URL}$1"
}

iso_ago() {  # iso_ago 1  -> horário UTC de N horas atrás (GNU e macOS)
  date -u -d "$1 hours ago" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-"$1"H +%Y-%m-%dT%H:%M:%SZ
}
day_ago() {
  date -u -d "$1 days ago" +%Y-%m-%d 2>/dev/null || date -u -v-"$1"d +%Y-%m-%d
}
