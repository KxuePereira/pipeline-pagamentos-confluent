-- run-as: flink
-- RULE_VELOCITY: 3 transações no MESMO cartão em menos de 60 s.
-- MATCH_RECOGNIZE avalia evento a evento, por cartão, em ordem de tempo:
--   * janela realmente deslizante (TUMBLE perderia 2+1 na borda; HOP duplicaria alertas);
--   * WITHIN limita o estado: depois de 60 s a sequência parcial é descartada;
--   * SKIP PAST LAST ROW: 4 compras seguidas geram 1 alerta, não 2.
INSERT INTO `desafio.fraud.detected`
SELECT
  CONCAT('VELOCITY-', CAST(card_id AS STRING), '-', CAST(first_tx_id AS STRING)) AS alert_id,
  'RULE_VELOCITY' AS rule_id,
  card_id,
  account_id,
  customer_id,
  risk_tier,
  tx_count,
  CAST(total_amount AS DECIMAL(15, 2)) AS total_amount,
  first_tx_at,
  last_tx_at,
  transaction_ids,
  '3 transacoes no mesmo cartao em menos de 60 s' AS reason,
  last_tx_at AS detected_at
FROM `desafio.transactions.enriched`
MATCH_RECOGNIZE (
  PARTITION BY card_id
  ORDER BY created_at
  MEASURES
    FIRST(T.transaction_id)     AS first_tx_id,
    LAST(T.account_id)          AS account_id,
    LAST(T.customer_id)         AS customer_id,
    LAST(T.risk_tier)           AS risk_tier,
    COUNT(T.transaction_id)     AS tx_count,
    SUM(T.amount)               AS total_amount,
    FIRST(T.created_at)         AS first_tx_at,
    LAST(T.created_at)          AS last_tx_at,
    ARRAY_AGG(T.transaction_id) AS transaction_ids
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (T{3}) WITHIN INTERVAL '60' SECOND
  DEFINE
    T AS TRUE
);
