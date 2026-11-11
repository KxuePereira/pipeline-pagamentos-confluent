-- =====================================================================
-- 99_cleanup.sql  —  Limpeza do lado do banco (chamado pelo teardown.sh)
--
-- IMPORTANTE: um slot de replicação esquecido segura o WAL para sempre.
-- No Neon isso enche o storage e impede a compute de dormir.
-- =====================================================================

SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE slot_name = 'desafio_slot' AND NOT active;

DROP PUBLICATION IF EXISTS desafio_pub;

-- Conferência: as duas consultas devem voltar vazias
SELECT slot_name, active FROM pg_replication_slots WHERE slot_name = 'desafio_slot';
SELECT pubname FROM pg_publication WHERE pubname = 'desafio_pub';

-- Para apagar também os dados, descomente:
-- DROP TABLE IF EXISTS transactions, cards, merchants, accounts, customers CASCADE;
