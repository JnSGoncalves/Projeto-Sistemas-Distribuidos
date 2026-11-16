using System.Diagnostics;
using System.Text;
using Google.Protobuf;
using NetMQ;
using NetMQ.Sockets;

namespace Chat;

class Cliente {
    public void Executar(string usuario, string? historico = null) {
        string broker = Environment.GetEnvironmentVariable("BROKER_CLIENTES") ?? "tcp://broker:5555";

        using var assinante = new SubscriberSocket();
        assinante.Connect(Environment.GetEnvironmentVariable("PROXY_INSCRICOES") ?? "tcp://proxy:5558");

        var inscricoes = new HashSet<string>();
        var relogio = new Relogio();

        Resposta Enviar(string acao, string canal = "", string texto = "") {
            using var socket = new RequestSocket();
            socket.Options.Linger = TimeSpan.Zero;
            socket.Connect(broker);

            var solicitacao = new Solicitacao { Acao = acao, Usuario = usuario, Canal = canal, Texto = texto,
                Timestamp = relogio.Agora(), Contador = relogio.Enviar() };
            Console.WriteLine($"Enviada: acao={solicitacao.Acao} usuario=[{solicitacao.Usuario}] canal=[{solicitacao.Canal}] texto=[{solicitacao.Texto}] timestamp={solicitacao.Timestamp} contador={solicitacao.Contador}");
            socket.SendFrame(solicitacao.ToByteArray());
            if (!socket.TryReceiveFrameBytes(TimeSpan.FromSeconds(5), out var dados)) {
                Console.WriteLine($"Sem resposta para {acao} apos 5 segundos");
                return new Resposta { Mensagem = "Tempo limite de resposta" };
            }

            var resposta = Resposta.Parser.ParseFrom(dados);
            relogio.Receber(resposta.Contador);
            Console.WriteLine($"Recebida: sucesso={resposta.Sucesso} mensagem=[{resposta.Mensagem}] canais=[{string.Join(", ", resposta.Canais)}] timestamp={resposta.Timestamp} contador={resposta.Contador}");
            foreach (var publicacao in resposta.Historico) Console.WriteLine($"Historico: {publicacao}");
            return resposta;
        }

        if (historico is not null) {
            if (!Enviar("historico", historico).Sucesso) Environment.ExitCode = 1;
            return;
        }

        void ReceberDurante(long milissegundos) {
            var tempo = Stopwatch.StartNew();
            long restante;

            while ((restante = milissegundos - tempo.ElapsedMilliseconds) > 0) {
                if (!assinante.TryReceiveFrameString(TimeSpan.FromMilliseconds(restante), Encoding.UTF8, out var topico)) break;
                var publicacao = Publicacao.Parser.ParseFrom(assinante.ReceiveFrameBytes());
                relogio.Receber(publicacao.Contador);
                long recebidaEm = relogio.Agora();
                if (topico == publicacao.Canal + "\0" && inscricoes.Contains(publicacao.Canal)) {
                    Console.WriteLine($"Publicacao recebida: canal=[{publicacao.Canal}] usuario=[{publicacao.Usuario}] texto=[{publicacao.Texto}] "
                        + $"envio={publicacao.Timestamp} recebimento={recebidaEm} contador={publicacao.Contador} id={publicacao.Id} "
                        + $"timestamp_solicitacao={publicacao.TimestampSolicitacao} contador_solicitacao={publicacao.ContadorSolicitacao}");
                }
            }
        }

        while (!Enviar("login").Sucesso)
            ReceberDurante(1000);

        while (true) {
            var resposta = Enviar("listar");

            if (!resposta.Sucesso) { ReceberDurante(1000); continue; }

            var canais = resposta.Canais.ToList();
            if (canais.Count < 5) {
                string novoCanal = $"canal-{Guid.NewGuid().ToString("N")[..8]}";
                if (Enviar("criar", novoCanal).Sucesso) canais.Add(novoCanal);
            }

            if (canais.Count == 0) { ReceberDurante(1000); continue; }

            var disponiveis = canais.Where(canal => !inscricoes.Contains(canal)).ToList();
            if (inscricoes.Count < 3 && disponiveis.Count > 0) {
                string inscricao = disponiveis[Random.Shared.Next(disponiveis.Count)];
                assinante.Subscribe(inscricao + "\0", Encoding.UTF8);
                inscricoes.Add(inscricao);
                Console.WriteLine($"Inscrito no canal [{inscricao}]");
            }

            string canal = canais[Random.Shared.Next(canais.Count)];
            for (int i = 0; i < 10; i++) {
                var intervalo = Stopwatch.StartNew();
                Enviar("publicar", canal, $"Mensagem {Random.Shared.Next(1000, 10000)}");
                ReceberDurante(1000 - intervalo.ElapsedMilliseconds);
            }
        }
    }
}
