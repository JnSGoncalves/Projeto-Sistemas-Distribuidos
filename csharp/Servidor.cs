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

        var relogio = new Relogio();
        string nome = Environment.GetEnvironmentVariable("NOME_SERVIDOR") ?? Environment.MachineName;
        var replicacao = new Replicacao(banco, nome);
        var coordenacao = new Coordenacao(nome, relogio) { AtenderReplica = replicacao.Atender };
        var replicas = new List<ServidorInfo>();
        var intervaloReplica = System.Diagnostics.Stopwatch.StartNew();
        var servidores = new List<ServidorInfo>();
        int recebidas = 0;
        int trocadas = 0;

        bool ConsultarReferencia(string acao) {
            using var referencia = new RequestSocket();
            referencia.Options.Linger = TimeSpan.Zero;
            referencia.Connect(Environment.GetEnvironmentVariable("REFERENCIA_ENDPOINT") ?? "tcp://referencia:5559");
            var pedido = new PedidoReferencia { Acao = acao, Nome = nome,
                Timestamp = relogio.Agora(), Contador = relogio.Enviar() };

            Console.WriteLine($"Referencia enviada: {pedido}");
            referencia.SendFrame(pedido.ToByteArray());
            if (!referencia.TryReceiveFrameBytes(TimeSpan.FromSeconds(3), out var dados)) {
                Console.WriteLine("Referencia sem resposta");
                return false;
            }

            var resposta = RespostaReferencia.Parser.ParseFrom(dados);
            relogio.Receber(resposta.Contador);
            Console.WriteLine($"Referencia recebida: {resposta}");
            if (resposta.Sucesso) {
                if (acao == "listar") servidores = resposta.Servidores.ToList();
                else if (acao == "listar_replicas") replicas = resposta.Servidores.ToList();
                else coordenacao.Rank = resposta.Rank;
            }
            return resposta.Sucesso;
        }

        while (!ConsultarReferencia("registrar")) Thread.Sleep(1000);

        using var assinante = new SubscriberSocket();
        assinante.Subscribe(Coordenacao.Topico);
        assinante.Connect(Environment.GetEnvironmentVariable("PROXY_INSCRICOES") ?? "tcp://proxy:5558");
        coordenacao.Iniciar();
        ConsultarReferencia("listar");
        coordenacao.Atualizar(servidores, publicador);

        Resposta ProcessarSolicitacao(Solicitacao solicitacao) {
            Resposta Erro(string mensagem) => new() { Mensagem = mensagem };

            if (solicitacao.Timestamp <= 0) return Erro("Timestamp invalido");
            if (string.IsNullOrWhiteSpace(solicitacao.Usuario) || solicitacao.Usuario.Length > 64)
                return Erro("Usuario deve ter de 1 a 64 caracteres e nao ser vazio");

            if (solicitacao.Acao == "listar") return replicacao.Listar();
            if (solicitacao.Acao == "historico") return replicacao.Historico(solicitacao.Canal);
            if (solicitacao.Acao is not ("login" or "criar" or "publicar")) return Erro("Acao invalida");
            if (solicitacao.Acao != "login" && (string.IsNullOrWhiteSpace(solicitacao.Canal) || solicitacao.Canal.Length > 64))
                return Erro("Canal deve ter de 1 a 64 caracteres e nao ser vazio");
            if (solicitacao.Acao == "publicar" && string.IsNullOrWhiteSpace(solicitacao.Texto))
                return Erro("Mensagem nao pode ser vazia");

            if (replicas.Count < 2) ConsultarReferencia("listar_replicas");
            if (!replicas.Any(s => s.Nome != nome)) return Erro("Nenhuma replica disponivel");

            var operacao = new Operacao {
                Id = Guid.NewGuid().ToString("N"), Tipo = solicitacao.Acao, Usuario = solicitacao.Usuario,
                Canal = solicitacao.Canal, Texto = solicitacao.Texto, Contador = solicitacao.Contador,
                Timestamp = solicitacao.Acao == "publicar" ? relogio.Agora() : solicitacao.Timestamp,
                TimestampSolicitacao = solicitacao.Timestamp
            };
            if (!replicacao.Registrar(operacao)) return Erro("Canal ja existe");
            if (!replicacao.Confirmar(operacao, replicas, coordenacao))
                return Erro("Escrita local sem confirmacao de outra replica");

            if (solicitacao.Acao == "publicar") {
                var publicacao = new Publicacao {
                    Id = operacao.Id, Canal = operacao.Canal, Usuario = operacao.Usuario, Texto = operacao.Texto,
                    Timestamp = operacao.Timestamp, TimestampSolicitacao = operacao.TimestampSolicitacao,
                    ContadorSolicitacao = operacao.Contador, Contador = relogio.Enviar()
                };
                publicador.SendMoreFrame(publicacao.Canal + "\0").SendFrame(publicacao.ToByteArray());
                trocadas++;
                Console.WriteLine($"Publicada: {publicacao}");
            }

            return new Resposta { Sucesso = true, Mensagem = solicitacao.Acao switch {
                "login" => "Login realizado", "criar" => "Canal criado", _ => "Publicacao enviada"
            } };
        }

        using var socket = new ResponseSocket();
        socket.Connect(Environment.GetEnvironmentVariable("BROKER_SERVIDORES") ?? "tcp://broker:5556");

        while (true) {
            coordenacao.ReceberAnuncios(assinante);
            if (intervaloReplica.ElapsedMilliseconds >= 2000) {
                ConsultarReferencia("listar_replicas");
                try {
                    replicacao.Buscar(replicas, coordenacao);
                } catch (Exception erro) when (erro is SqliteException or InvalidDataException) {
                    Console.WriteLine($"Erro ao recuperar replica: {erro.Message}");
                }
                intervaloReplica.Restart();
            }

            if (coordenacao.EleicaoPendente() || trocadas >= 15) {
                trocadas %= 15;
                ConsultarReferencia("listar");
                coordenacao.Atualizar(servidores, publicador);
            }

            if (!socket.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(100), out var dados)) continue;

            trocadas++;
            Resposta resposta;
            try {
                var solicitacao = Solicitacao.Parser.ParseFrom(dados);
                relogio.Receber(solicitacao.Contador);
                Console.WriteLine($"Recebida: acao={solicitacao.Acao} usuario=[{solicitacao.Usuario}] canal=[{solicitacao.Canal}] texto=[{solicitacao.Texto}] timestamp={solicitacao.Timestamp} contador={solicitacao.Contador}");
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

            resposta.Timestamp = relogio.Agora();
            resposta.Contador = relogio.Enviar();
            Console.WriteLine($"Enviada: sucesso={resposta.Sucesso} mensagem=[{resposta.Mensagem}] canais=[{string.Join(", ", resposta.Canais)}] timestamp={resposta.Timestamp} contador={resposta.Contador}");
            if (resposta.Historico.Count > 0) Console.WriteLine($"Historico enviado: {resposta}");
            socket.SendFrame(resposta.ToByteArray());
            trocadas++;

            recebidas++;
            if (recebidas % 10 == 0) ConsultarReferencia("heartbeat");
        }
    }
}
