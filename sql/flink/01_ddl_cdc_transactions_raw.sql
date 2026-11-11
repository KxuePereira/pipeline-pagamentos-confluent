-- run-as: user
-- O conector escreve envelopes Debezium. Por padrão o Flink "abre" o envelope e
-- trata o tópico como changelog (retract). Para TRANSAÇÕES isso atrapalha: o
-- MATCH_RECOGNIZE e as janelas exigem um fluxo append-only. Como transação é um
-- fato que só nasce (nunca é editada), lemos o envelope cru, em modo append, e
-- filtramos op IN ('c','r') no próximo passo.
ALTER TABLE `desafio.public.transactions` SET (
  'value.format'   = 'avro-registry',
  'changelog.mode' = 'append'
);
