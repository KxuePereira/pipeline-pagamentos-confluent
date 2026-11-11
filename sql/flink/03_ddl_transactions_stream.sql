-- run-as: user
-- Fluxo append-only de transações, com tempo de evento = hora da compra no banco.
-- Watermark com 5 s de tolerância a atraso. Chave = card_id: todas as compras
-- de um cartão ficam na mesma partição, em ordem.
CREATE TABLE IF NOT EXISTS `desafio.transactions.stream` (
  transaction_id BIGINT,
  card_id        INT,
  account_id     INT,
  merchant_id    INT,
  amount         DECIMAL(15, 2),
  currency       STRING,
  channel        STRING,
  status         STRING,
  created_at     TIMESTAMP_LTZ(3),
  cdc_op         STRING,
  WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND
) DISTRIBUTED BY (card_id) INTO 1 BUCKETS
WITH (
  'changelog.mode'       = 'append',
  'value.fields-include' = 'all'
);
