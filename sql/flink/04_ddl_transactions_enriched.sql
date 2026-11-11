-- run-as: user
-- Transação + dados da conta na versão válida NO MOMENTO da compra.
CREATE TABLE IF NOT EXISTS `desafio.transactions.enriched` (
  transaction_id BIGINT,
  card_id        INT,
  account_id     INT,
  merchant_id    INT,
  amount         DECIMAL(15, 2),
  currency       STRING,
  channel        STRING,
  created_at     TIMESTAMP_LTZ(3),
  customer_id    INT,
  account_status STRING,
  credit_limit   DECIMAL(15, 2),
  risk_tier      STRING,
  WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND
) DISTRIBUTED BY (card_id) INTO 1 BUCKETS
WITH (
  'changelog.mode'       = 'append',
  'value.fields-include' = 'all'
);
