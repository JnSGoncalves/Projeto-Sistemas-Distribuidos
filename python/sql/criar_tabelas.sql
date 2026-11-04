CREATE TABLE IF NOT EXISTS logins (
    usuario TEXT NOT NULL,
    timestamp INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS canais (
    nome TEXT PRIMARY KEY,
    usuario TEXT NOT NULL,
    timestamp INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS publicacoes (
    canal TEXT NOT NULL,
    usuario TEXT NOT NULL,
    texto TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    timestamp_solicitacao INTEGER NOT NULL
);
