using Microsoft.Data.Sqlite;

namespace Chat;

class Replicacao {
    private const int Lote = 100;
    private readonly SqliteConnection banco;
    private readonly object trava = new();
    private readonly string replicaId;
    private readonly string inserir;
    private readonly string materializar;
    private readonly string nome;

    public Replicacao(SqliteConnection banco, string nome) {
        this.banco = banco;
        this.nome = nome;
        inserir = LerSql("inserir_operacao.sql");
        materializar = LerSql("materializar_canal.sql");
        using var transacao = banco.BeginTransaction();
        Executar(transacao, LerSql("replicacao.sql"));

        using var consulta = Comando(transacao, "SELECT id FROM identidade_replica WHERE unica = 1");
        if (consulta.ExecuteScalar() is string existente) {
            replicaId = existente;
        } else {
            replicaId = Guid.NewGuid().ToString("N");
            Executar(transacao, LerSql("importar_legado.sql"), ("$nome", nome));
            Executar(transacao, "INSERT INTO identidade_replica VALUES (1, $id)", ("$id", replicaId));
        }
        transacao.Commit();
    }

    private static string LerSql(string arquivo) => File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "sql", arquivo));

    private SqliteCommand Comando(SqliteTransaction? transacao, string sql, params (string Nome, object Valor)[] args) {
        var comando = banco.CreateCommand();
        comando.Transaction = transacao;
        comando.CommandText = sql;
        foreach (var (nome, valor) in args) comando.Parameters.AddWithValue(nome, valor);
        return comando;
    }

    private int Executar(SqliteTransaction transacao, string sql, params (string Nome, object Valor)[] args) {
        using var comando = Comando(transacao, sql, args);
        return comando.ExecuteNonQuery();
    }

    private void Aplicar(SqliteTransaction transacao, Operacao op) {
        if (op.Id.Length == 0 || op.Tipo is not ("login" or "criar" or "publicar"))
            throw new InvalidDataException("Operacao de replica invalida");

        int novas = Executar(transacao, inserir, ("$id", op.Id), ("$tipo", op.Tipo),
            ("$usuario", op.Usuario), ("$canal", op.Canal), ("$texto", op.Texto),
            ("$timestamp", op.Timestamp), ("$timestamp_solicitacao", op.TimestampSolicitacao), ("$contador", op.Contador));
        if (novas == 0) return;

        if (op.Tipo == "login") {
            Executar(transacao, "INSERT INTO logins VALUES ($usuario, $timestamp)",
                ("$usuario", op.Usuario), ("$timestamp", op.Timestamp));
        } else if (op.Tipo == "criar") {
            Executar(transacao, materializar, ("$canal", op.Canal));
        } else {
            Executar(transacao, "INSERT INTO publicacoes VALUES ($canal, $usuario, $texto, $timestamp, $pedido)",
                ("$canal", op.Canal), ("$usuario", op.Usuario), ("$texto", op.Texto),
                ("$timestamp", op.Timestamp), ("$pedido", op.TimestampSolicitacao));
        }
    }

    public bool Registrar(Operacao op) {
        lock (trava) {
            using var transacao = banco.BeginTransaction();
            if (op.Tipo == "criar") {
                using var consulta = Comando(transacao, "SELECT 1 FROM canais WHERE nome = $canal", ("$canal", op.Canal));
                if (consulta.ExecuteScalar() is not null) return false;
            }
            Aplicar(transacao, op);
            transacao.Commit();
            return true;
        }
    }

    public Resposta Listar() {
        lock (trava) {
            var resposta = new Resposta { Sucesso = true, Mensagem = "Lista de canais" };
            using var comando = Comando(null, "SELECT nome FROM canais ORDER BY nome");
            using var leitor = comando.ExecuteReader();
            while (leitor.Read()) resposta.Canais.Add(leitor.GetString(0));
            return resposta;
        }
    }

    public Resposta Historico(string canal) {
        lock (trava) {
            var resposta = new Resposta { Sucesso = true, Mensagem = "Historico" };
            using var comando = Comando(null, """
                SELECT id, usuario, canal, texto, timestamp, timestamp_solicitacao, contador
                FROM operacoes WHERE tipo = 'publicar' AND ($canal = '' OR canal = $canal)
                ORDER BY timestamp, id
                """, ("$canal", canal));
            using var leitor = comando.ExecuteReader();
            while (leitor.Read()) {
                resposta.Historico.Add(new Publicacao {
                    Id = leitor.GetString(0), Usuario = leitor.GetString(1), Canal = leitor.GetString(2),
                    Texto = leitor.GetString(3), Timestamp = leitor.GetInt64(4),
                    TimestampSolicitacao = leitor.GetInt64(5), ContadorSolicitacao = leitor.GetInt64(6)
                });
            }
            return resposta;
        }
    }

    public MensagemServidor? Atender(MensagemServidor pedido) {
        if (pedido.Acao is not ("replicar" or "buscar_operacoes")) return null;

        var resposta = new MensagemServidor { Acao = pedido.Acao, Nome = nome, ReplicaId = replicaId };
        try {
            lock (trava) {
                if (pedido.Acao == "replicar") {
                    if (pedido.Operacoes.Count != 1) return resposta;
                    using var transacao = banco.BeginTransaction();
                    Aplicar(transacao, pedido.Operacoes[0]);
                    transacao.Commit();
                } else {
                    resposta.Cursor = pedido.ReplicaId == replicaId ? Math.Max(0, pedido.Cursor) : 0;
                    using var comando = Comando(null, """
                        SELECT sequencia, id, tipo, usuario, canal, texto, timestamp, timestamp_solicitacao, contador
                        FROM operacoes WHERE sequencia > $cursor ORDER BY sequencia LIMIT $lote
                        """, ("$cursor", resposta.Cursor), ("$lote", Lote));
                    using var leitor = comando.ExecuteReader();
                    while (leitor.Read()) {
                        resposta.Cursor = leitor.GetInt64(0);
                        resposta.Operacoes.Add(new Operacao {
                            Id = leitor.GetString(1), Tipo = leitor.GetString(2), Usuario = leitor.GetString(3),
                            Canal = leitor.GetString(4), Texto = leitor.GetString(5), Timestamp = leitor.GetInt64(6),
                            TimestampSolicitacao = leitor.GetInt64(7), Contador = leitor.GetInt64(8)
                        });
                    }
                }
                resposta.Sucesso = true;
            }
        } catch (Exception erro) when (erro is SqliteException or InvalidDataException) {
            Console.WriteLine($"Erro na replicacao: {erro.Message}");
        }
        return resposta;
    }

    public bool Confirmar(Operacao op, IEnumerable<ServidorInfo> servidores, Coordenacao coordenacao) {
        foreach (var servidor in servidores.Where(s => s.Nome != nome)) {
            if (coordenacao.Consultar(servidor.Nome, "replicar", p => p.Operacoes.Add(op)) is not null)
                return true;
        }
        return false;
    }

    public void Buscar(IEnumerable<ServidorInfo> servidores, Coordenacao coordenacao) {
        foreach (var servidor in servidores.Where(s => s.Nome != nome)) {
            string identidade = "";
            long cursor = 0;
            lock (trava) {
                using var consulta = Comando(null, "SELECT replica_id, cursor FROM cursores_replica WHERE servidor = $nome",
                    ("$nome", servidor.Nome));
                using var leitor = consulta.ExecuteReader();
                if (leitor.Read()) {
                    identidade = leitor.GetString(0);
                    cursor = leitor.GetInt64(1);
                }
            }

            var resultado = coordenacao.Consultar(servidor.Nome, "buscar_operacoes", pedido => {
                pedido.ReplicaId = identidade;
                pedido.Cursor = cursor;
            });
            if (resultado is not { } lote) continue;
            var resposta = lote.Resposta;
            if (resposta.ReplicaId == identidade && resposta.Cursor == cursor && resposta.Operacoes.Count == 0) continue;

            lock (trava) {
                using var transacao = banco.BeginTransaction();
                foreach (var op in resposta.Operacoes) Aplicar(transacao, op);
                Executar(transacao, """
                    INSERT INTO cursores_replica VALUES ($nome, $id, $cursor)
                    ON CONFLICT(servidor) DO UPDATE SET replica_id = excluded.replica_id, cursor = excluded.cursor
                    """, ("$nome", servidor.Nome), ("$id", resposta.ReplicaId), ("$cursor", resposta.Cursor));
                transacao.Commit();
            }
        }
    }
}
