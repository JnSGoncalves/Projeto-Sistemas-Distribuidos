CREATE TABLE IF NOT EXISTS operacoes (
    sequencia INTEGER PRIMARY KEY,
    id TEXT NOT NULL UNIQUE,
    tipo TEXT NOT NULL,
    usuario TEXT NOT NULL,
    canal TEXT NOT NULL,
    texto TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    timestamp_solicitacao INTEGER NOT NULL,
    contador INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS operacoes_por_canal ON operacoes (tipo, canal, id);

CREATE TABLE IF NOT EXISTS identidade_replica (
    unica INTEGER PRIMARY KEY CHECK (unica = 1),
    id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cursores_replica (
    servidor TEXT PRIMARY KEY,
    replica_id TEXT NOT NULL,
    cursor INTEGER NOT NULL
);
