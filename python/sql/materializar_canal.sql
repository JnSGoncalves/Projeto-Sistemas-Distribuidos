INSERT INTO canais (nome, usuario, timestamp)
SELECT canal, usuario, timestamp FROM operacoes
WHERE tipo = 'criar' AND canal = $canal ORDER BY id LIMIT 1
ON CONFLICT(nome) DO UPDATE SET usuario = excluded.usuario, timestamp = excluded.timestamp;
