-- run-as: user
-- Tópico de saída consumido pela aplicação. Append-only: alerta é um fato.
-- alert_id é determinístico (regra + cartão + 1ª transação / janela): se o
-- statement reprocessar, o consumidor deduplica por ele (idempotência).
CREATE TABLE IF NOT EXISTS `desafio.fraud.detected` (
  alert_id        STRING,
  rule_id         STRING,
  card_id         INT,
  account_id      INT,
  customer_id     INT,
  risk_tier       STRING,
  tx_count        BIGINT,
  total_amount    DECIMAL(15, 2),
  first_tx_at     TIMESTAMP_LTZ(3),
  last_tx_at      TIMESTAMP_LTZ(3),
  transaction_ids ARRAY<BIGINT>,
  reason          STRING,
  detected_at     TIMESTAMP_LTZ(3)
) DISTRIBUTED BY (card_id) INTO 1 BUCKETS
WITH (
  'changelog.mode'       = 'append',
  'value.fields-include' = 'all'
);
