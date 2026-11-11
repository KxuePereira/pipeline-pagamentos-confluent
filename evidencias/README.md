# Evidências

Tudo aqui é gerado pelos scripts. Rode o pipeline e versione esta pasta junto com o README.

| Arquivo | Gerado por | Prova |
|---|---|---|
| `01-fundacao.txt` | `scripts/01_fundacao.sh` | cluster, service accounts, ACLs e role bindings de cada identidade |
| `02-contrato.txt` | `scripts/02_contrato.sh` | BACKWARD, v2 aceita, v3/v4 recusadas, tentativa de registro com 409 |
| `03-ingestao.txt` | `scripts/03_ingestao.sh` | conector `RUNNING`, tópicos, contagens no Postgres, slot ativo |
| `03-envelopes-cdc.jsonl` | `scripts/03_ingestao.sh` | `op=c`, `op=u`, `op=d` e o tombstone da conta 9001 |
| `04-processamento.txt` | `scripts/04_processamento.sh` | statements `RUNNING` e conferência com o gabarito do seed |
| `04-alertas.jsonl` | `scripts/04_processamento.sh` | alertas lidos de `desafio.fraud.detected` |
| `04-alertas-ao-vivo.jsonl` | `consumer/consume.py --latest` | alertas das fraudes injetadas ao vivo |
| `injecoes-fraude.jsonl` | `generator/stream_transactions.py` | gabarito das fraudes injetadas ao vivo |
| `05-operacao.txt` | `scripts/05_operacao.sh` | métricas, acessos, saúde e custo |
| `99-teardown.txt` | `teardown.sh` | nada sobrou: conector, statements, SAs, ambiente e slot |

Nenhum desses arquivos contém segredo: os scripts listam IDs de chave, nunca o secret. Mesmo assim, dê uma olhada antes de commitar.
