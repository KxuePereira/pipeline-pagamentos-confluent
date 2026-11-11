-- run-as: flink
-- CDC de contas (changelog retract, aberto automaticamente pelo Flink a partir do
-- envelope Debezium) -> tabela upsert por account_id. Delete vira tombstone.
INSERT INTO `desafio.accounts.versioned`
SELECT
  account_id,
  customer_id,
  status,
  credit_limit,
  risk_tier,
  CAST(updated_at AS TIMESTAMP_LTZ(3)) AS updated_at
FROM `desafio.public.accounts`;
