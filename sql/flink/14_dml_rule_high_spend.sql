-- run-as: flink
-- RULE_HIGH_SPEND (segunda regra, outra janela e outro critério):
-- 2 ou mais compras no mesmo cartão, dentro de uma janela fixa de 5 min,
-- somando mais de 50% do limite da conta (dado que só existe graças ao
-- enriquecimento). Janela TUMBLE: barata e sem duplicidade, mas cega para
-- rajadas que cruzam a borda (ex.: 04:59 e 05:01) — trade-off documentado.
INSERT INTO `desafio.fraud.detected`
SELECT
  CONCAT('HIGHSPEND-', CAST(card_id AS STRING), '-', DATE_FORMAT(window_start, 'yyyyMMddHHmm')) AS alert_id,
  'RULE_HIGH_SPEND' AS rule_id,
  card_id,
  account_id,
  customer_id,
  risk_tier,
  COUNT(*) AS tx_count,
  CAST(SUM(amount) AS DECIMAL(15, 2)) AS total_amount,
  MIN(created_at) AS first_tx_at,
  MAX(created_at) AS last_tx_at,
  ARRAY_AGG(transaction_id) AS transaction_ids,
  'Gasto acima de 50% do limite em 5 min' AS reason,
  CAST(window_end AS TIMESTAMP_LTZ(3)) AS detected_at
FROM TABLE(
  TUMBLE(TABLE `desafio.transactions.enriched`, DESCRIPTOR(created_at), INTERVAL '5' MINUTES)
)
GROUP BY window_start, window_end, card_id, account_id, customer_id, risk_tier, credit_limit
HAVING COUNT(*) >= 2 AND SUM(amount) > credit_limit * 0.5;
