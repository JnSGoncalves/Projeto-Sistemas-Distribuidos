INSERT INTO operacoes (id, tipo, usuario, canal, texto, timestamp, timestamp_solicitacao, contador)
VALUES ($id, $tipo, $usuario, $canal, $texto, $timestamp, $timestamp_solicitacao, $contador)
ON CONFLICT(id) DO NOTHING;
