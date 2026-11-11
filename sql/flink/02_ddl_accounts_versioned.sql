-- run-as: user
-- Tabela versionada de contas: chave primária + watermark em updated_at.
-- É o lado "dimensão" do temporal join (FOR SYSTEM_TIME AS OF).
CREATE TABLE IF NOT EXISTS `desafio.accounts.versioned` (
  account_id   INT NOT NULL,
  customer_id  INT,
  status       STRING,
  credit_limit DECIMAL(15, 2),
  risk_tier    STRING,
  updated_at   TIMESTAMP_LTZ(3),
  PRIMARY KEY (account_id) NOT ENFORCED,
  WATERMARK FOR updated_at AS updated_at
) DISTRIBUTED BY (account_id) INTO 1 BUCKETS
WITH (
  'changelog.mode'       = 'upsert',
  'value.fields-include' = 'all'
);
