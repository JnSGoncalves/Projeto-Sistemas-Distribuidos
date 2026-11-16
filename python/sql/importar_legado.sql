INSERT INTO operacoes (id, tipo, usuario, canal, texto, timestamp, timestamp_solicitacao, contador)
SELECT 'legado/' || $nome || '/login/' || rowid, 'login', usuario, '', '', timestamp, 0, 0 FROM logins
UNION ALL
SELECT 'legado/' || $nome || '/criar/' || rowid, 'criar', usuario, nome, '', timestamp, 0, 0 FROM canais
UNION ALL
SELECT 'legado/' || $nome || '/publicar/' || rowid, 'publicar', usuario, canal, texto, timestamp, timestamp_solicitacao, 0 FROM publicacoes;
