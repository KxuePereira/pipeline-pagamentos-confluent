-- =====================================================================
-- 04_cdc_demo.sql  —  Evidência da camada 3: insert, update e delete
-- Rode com o pipeline no ar. Cada comando vira um evento no tópico
-- desafio.public.accounts (op = c, u, d).
-- =====================================================================

-- Conta 9001 é reservada para a demo (o seed vai até 240).
DELETE FROM accounts WHERE account_id = 9001;   -- garante que dá para rodar de novo

-- op=c : nasce a conta (before = null, after = linha nova)
INSERT INTO accounts (account_id, customer_id, status, credit_limit, risk_tier)
VALUES (9001, 1, 'ACTIVE', 5000.00, 'LOW');

SELECT pg_sleep(2);

-- op=u : a conta é bloqueada (before = ACTIVE, after = BLOCKED)
--        o before só vem completo por causa do REPLICA IDENTITY FULL
UPDATE accounts SET status = 'BLOCKED', risk_tier = 'HIGH' WHERE account_id = 9001;

SELECT pg_sleep(2);

-- op=d : a conta some (before = última versão, after = null) + tombstone
DELETE FROM accounts WHERE account_id = 9001;
