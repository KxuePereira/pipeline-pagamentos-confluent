#!/usr/bin/env bash
# =====================================================================
# check_entrega.sh — o checklist de entrega, automatizado.
# Rode antes de submeter:  ./scripts/check_entrega.sh
# =====================================================================
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
fail=0
pass() { echo "  ✔ $*"; }
bad()  { echo "  ✘ $*"; fail=1; }

echo "== Evidências"
if grep -nE 'PENDENTE|<sua evidência aqui>' README.md; then bad "ainda há evidência pendente no README (linhas acima)"
else pass "nenhum marcador PENDENTE no README"; fi
for f in 01-fundacao.txt 02-contrato.txt 03-ingestao.txt 03-envelopes-cdc.jsonl 04-processamento.txt 04-alertas.jsonl 05-operacao.txt 99-teardown.txt; do
  [[ -s "evidencias/$f" ]] && pass "evidencias/$f" || bad "falta evidencias/$f"
done

echo "== Segredos"
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git ls-files --error-unmatch .env >/dev/null 2>&1 && bad ".env está versionado! git rm --cached .env" || pass ".env fora do Git"
  [[ -n "$(git ls-files .state)" ]] && bad ".state/ está versionado!" || pass ".state/ fora do Git"
  [[ -f .env.example ]] && git ls-files --error-unmatch .env.example >/dev/null 2>&1 && pass ".env.example versionado" || bad ".env.example ausente do Git"
  # padrões de segredo em arquivos versionados (chaves Confluent têm 16 chars maiúsculos; segredos 64)
  if git grep -nIE '(postgres(ql)?://[^:@/ ]+:[^@/ $]{6,}@)|(api[_-]?secret["'"'"' :=]+[A-Za-z0-9+/]{40,})|\b[A-Za-z0-9+/]{64}\b' -- . ':!*.avsc' ':!sql/postgres/02_seed.sql' \
       | grep -v 'TROQUE_AQUI'; then
    bad "possível segredo versionado (linhas acima)"
  else pass "nenhum padrão de segredo nos arquivos versionados"; fi
  remote="$(git remote get-url origin 2>/dev/null || true)"
  name="$(basename -s .git "$remote")"
  if [[ -n "$remote" ]]; then
    [[ "$name" =~ ^[a-z0-9._-]+$ ]] && pass "nome do repositório '$name' em minúsculas, sem acento" \
      || bad "nome do repositório '$name' deve ser minúsculo e sem acento"
    url="${remote/#git@github.com:/https://github.com/}"
    echo "  → link para enviar (o do repositório, não de um arquivo): ${url%.git}"
  else bad "sem remote 'origin' (o repositório já está no GitHub?)"; fi
else
  bad "não é um repositório Git (git init)"
fi

echo
((fail == 0)) && echo "Tudo certo para entregar." || echo "Corrija os itens com ✘ antes de entregar."
exit "$fail"
