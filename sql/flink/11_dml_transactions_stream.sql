-- run-as: flink
-- Envelope cru -> fluxo de transações. op='r' = linha do snapshot inicial,
-- op='c' = insert ao vivo. update/delete de transação não entram no fluxo de fraude.
INSERT INTO `desafio.transactions.stream`
SELECT
  `after`.transaction_id,
  `after`.card_id,
  `after`.account_id,
  `after`.merchant_id,
  `after`.amount,
  `after`.currency,
  `after`.channel,
  `after`.status,
  CAST(`after`.created_at AS TIMESTAMP_LTZ(3)) AS created_at,
  `op` AS cdc_op
FROM `desafio.public.transactions`
WHERE `op` IN ('c', 'r');
