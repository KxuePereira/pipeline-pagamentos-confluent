-- =====================================================================
-- 03_cdc_setup.sql  —  O que o conector de CDC precisa do banco
-- Rode como o dono do banco (ex.: neondb_owner), DEPOIS do schema e do seed.
--
-- Pré-requisito fora do SQL: no Neon, ligar "Logical Replication"
-- (Project settings → Logical Replication → Enable). Confira com:
--     SHOW wal_level;   -- tem que responder: logical
--
-- E criar o papel cdc_user pelo console/CLI do Neon (papéis criados por lá
-- já recebem o atributo REPLICATION). Aqui só damos o mínimo de leitura.
-- =====================================================================

-- 1) REPLICA IDENTITY FULL: sem isso, o UPDATE e o DELETE chegam com
--    "before" vazio (só a chave), e o Flink não consegue montar o changelog.
ALTER TABLE customers    REPLICA IDENTITY FULL;
ALTER TABLE accounts     REPLICA IDENTITY FULL;
ALTER TABLE cards        REPLICA IDENTITY FULL;
ALTER TABLE merchants    REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;

-- 2) Publication criada por nós (publication.autocreate.mode=disabled):
--    o conector não precisa de CREATE no banco nem ser dono das tabelas.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'desafio_pub') THEN
        CREATE PUBLICATION desafio_pub
            FOR TABLE customers, accounts, cards, merchants, transactions;
    END IF;
END $$;

-- 3) Menor privilégio para o usuário do conector: só ler.
--    Ele NÃO escreve em nada; o snapshot inicial é um SELECT.
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON customers, accounts, cards, merchants, transactions TO cdc_user;

-- Conferência
SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete FROM pg_publication WHERE pubname = 'desafio_pub';
SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = 'desafio_pub' ORDER BY tablename;
SELECT c.relname AS tabela,
       CASE c.relreplident WHEN 'f' THEN 'FULL' WHEN 'd' THEN 'DEFAULT' ELSE c.relreplident::text END AS replica_identity
FROM pg_class c
WHERE c.relname IN ('customers', 'accounts', 'cards', 'merchants', 'transactions')
ORDER BY c.relname;
