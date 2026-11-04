using Google.Protobuf;
using Microsoft.Data.Sqlite;
using NetMQ;
using NetMQ.Sockets;

namespace Chat;

class Servidor {
    public void Executar() {
        using var banco = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = Environment.GetEnvironmentVariable("BANCO") ?? "/data/parte1.db" }.ToString());
        banco.Open();


        using (var comando = banco.CreateCommand()) {
            comando.CommandText = """
                CREATE TABLE IF NOT EXISTS logins (usuario TEXT NOT NULL, timestamp INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS canais (nome TEXT PRIMARY KEY, usuario TEXT NOT NULL, timestamp INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS publicacoes (
                    canal TEXT NOT NULL, usuario TEXT NOT NULL, texto TEXT NOT NULL,
                    timestamp INTEGER NOT NULL, timestamp_solicitacao INTEGER NOT NULL);
                """;
            comando.ExecuteNonQuery();
        }

        using var publicador = new PublisherSocket();
        publicador.Connect(Environment.GetEnvironmentVariable("PROXY_PUBLICACOES") ?? "tcp://proxy:5557");

        Resposta ProcessarSolicitacao(Solicitacao solicitacao) {
            var resposta = new Resposta();

            using var comando = banco.CreateCommand();

            if (solicitacao.Timestamp <= 0)
                resposta.Mensagem = "Timestamp invalido";

            else if (string.IsNullOrWhiteSpace(solicitacao.Usuario) || solicitacao.Usuario.Length > 64)
                resposta.Mensagem = "Usuario deve ter de 1 a 64 caracteres e nao ser vazio";

            else if (solicitacao.Acao == "login") {
                comando.CommandText = "INSERT INTO logins VALUES ($usuario, $timestamp)";
                comando.Parameters.AddWithValue("$usuario", solicitacao.Usuario);
                comando.Parameters.AddWithValue("$timestamp", solicitacao.Timestamp);
                comando.ExecuteNonQuery();
                resposta.Sucesso = true;
                resposta.Mensagem = "Login realizado";

            } else if (solicitacao.Acao == "criar") {
                if (string.IsNullOrWhiteSpace(solicitacao.Canal) || solicitacao.Canal.Length > 64)
                    resposta.Mensagem = "Canal deve ter de 1 a 64 caracteres e nao ser vazio";

                else {
                    comando.CommandText = "INSERT OR IGNORE INTO canais VALUES ($canal, $usuario, $timestamp)";
                    comando.Parameters.AddWithValue("$canal", solicitacao.Canal);
                    comando.Parameters.AddWithValue("$usuario", solicitacao.Usuario);
                    comando.Parameters.AddWithValue("$timestamp", solicitacao.Timestamp);
                    resposta.Sucesso = comando.ExecuteNonQuery() == 1;
                    resposta.Mensagem = resposta.Sucesso ? "Canal criado" : "Canal ja existe";
                }

            } else if (solicitacao.Acao == "listar") {
                comando.CommandText = "SELECT nome FROM canais ORDER BY nome";

                using var leitor = comando.ExecuteReader();

                while (leitor.Read()) resposta.Canais.Add(leitor.GetString(0));
                resposta.Sucesso = true;
                resposta.Mensagem = "Lista de canais";

            } else if (solicitacao.Acao == "publicar") {
                if (string.IsNullOrWhiteSpace(solicitacao.Canal) || solicitacao.Canal.Length > 64)
                    resposta.Mensagem = "Canal deve ter de 1 a 64 caracteres e nao ser vazio";
                    
                else if (string.IsNullOrWhiteSpace(solicitacao.Texto))
                    resposta.Mensagem = "Mensagem nao pode ser vazia";

                else {
                    var publicacao = new Publicacao { Canal = solicitacao.Canal, Usuario = solicitacao.Usuario,
                        Texto = solicitacao.Texto, Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() };
                    comando.CommandText = "INSERT INTO publicacoes VALUES ($canal, $usuario, $texto, $timestamp, $pedido)";
                    comando.Parameters.AddWithValue("$canal", publicacao.Canal);
                    comando.Parameters.AddWithValue("$usuario", publicacao.Usuario);
                    comando.Parameters.AddWithValue("$texto", publicacao.Texto);
                    comando.Parameters.AddWithValue("$timestamp", publicacao.Timestamp);
                    comando.Parameters.AddWithValue("$pedido", solicitacao.Timestamp);
                    comando.ExecuteNonQuery();
                    // O terminador impede que a inscricao no canal "a" receba o canal "ab".
                    publicador.SendMoreFrame(publicacao.Canal + "\0").SendFrame(publicacao.ToByteArray());
                    Console.WriteLine($"Publicada: canal=[{publicacao.Canal}] usuario=[{publicacao.Usuario}] texto=[{publicacao.Texto}] timestamp={publicacao.Timestamp}");
                    resposta.Sucesso = true;
                    resposta.Mensagem = "Publicacao enviada";
                }
            } else resposta.Mensagem = "Acao invalida";
            return resposta;
        }

        using var socket = new ResponseSocket();
        socket.Connect(Environment.GetEnvironmentVariable("BROKER_SERVIDORES") ?? "tcp://broker:5556");

        while (true) {
            byte[] dados = socket.ReceiveFrameBytes();
            Resposta resposta;
            try {
                var solicitacao = Solicitacao.Parser.ParseFrom(dados);
                Console.WriteLine($"Recebida: acao={solicitacao.Acao} usuario=[{solicitacao.Usuario}] canal=[{solicitacao.Canal}] texto=[{solicitacao.Texto}] timestamp={solicitacao.Timestamp}");
                resposta = ProcessarSolicitacao(solicitacao);

            } catch (InvalidProtocolBufferException) {
                Console.WriteLine($"Protobuf invalido recebido: {Convert.ToHexString(dados)}");
                resposta = new Resposta { Mensagem = "Protobuf invalido" };

            } catch (SqliteException erro) {
                Console.WriteLine($"Erro SQLite: {erro.Message}");
                resposta = new Resposta { Mensagem = "Erro ao acessar banco" };

            } catch (NetMQException erro) {
                Console.WriteLine($"Erro de publicacao: {erro.Message}");
                resposta = new Resposta { Mensagem = "Erro ao publicar mensagem" };
            }
            
            resposta.Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            Console.WriteLine($"Enviada: sucesso={resposta.Sucesso} mensagem=[{resposta.Mensagem}] canais=[{string.Join(", ", resposta.Canais)}] timestamp={resposta.Timestamp}");
            socket.SendFrame(resposta.ToByteArray());
        }
    }
}
