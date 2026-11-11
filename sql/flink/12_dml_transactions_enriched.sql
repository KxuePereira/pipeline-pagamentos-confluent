-- run-as: flink
-- property: sql.tables.scan.idle-timeout=5s
-- Temporal join: cada transação pega a versão da conta vigente em t.created_at.
-- Se a conta for bloqueada às 10:00, a compra das 09:59 continua com ACTIVE —
-- o resultado não muda se o job for reprocessado (diferente de um join comum).
-- idle-timeout: a tabela de contas fica parada por longos períodos; sem isso o
-- watermark dela não anda e o join segura todas as transações.
INSERT INTO `desafio.transactions.enriched`
SELECT
  t.transaction_id,
  t.card_id,
  t.account_id,
  t.merchant_id,
  t.amount,
  t.currency,
  t.channel,
  t.created_at,
  a.customer_id,
  a.status       AS account_status,
  a.credit_limit,
  a.risk_tier
FROM `desafio.transactions.stream` AS t
LEFT JOIN `desafio.accounts.versioned` FOR SYSTEM_TIME AS OF t.created_at AS a
  ON t.account_id = a.account_id;
