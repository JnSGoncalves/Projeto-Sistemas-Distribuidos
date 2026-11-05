using Google.Protobuf;
using NetMQ;
using NetMQ.Sockets;

namespace Chat;

class Cliente {
    public void Executar(string usuario) {
        using var socket = new RequestSocket();
        socket.Connect(Environment.GetEnvironmentVariable("BROKER_CLIENTES") ?? "tcp://broker:5555");

        using var assinante = new SubscriberSocket();
        assinante.Connect(Environment.GetEnvironmentVariable("PROXY_INSCRICOES") ?? "tcp://proxy:5558");

        var inscricoes = new HashSet<string>();
        var relogio = new Relogio();

        Resposta Enviar(string acao, string canal = "", string texto = "") {
            var solicitacao = new Solicitacao { Acao = acao, Usuario = usuario, Canal = canal, Texto = texto,
                Timestamp = relogio.Agora(), Contador = relogio.Enviar() };
            Console.WriteLine($"Enviada: acao={solicitacao.Acao} usuario=[{solicitacao.Usuario}] canal=[{solicitacao.Canal}] texto=[{solicitacao.Texto}] timestamp={solicitacao.Timestamp} contador={solicitacao.Contador}");
            socket.SendFrame(solicitacao.ToByteArray());
            var resposta = Resposta.Parser.ParseFrom(socket.ReceiveFrameBytes());
            relogio.Receber(resposta.Contador);
            Console.WriteLine($"Recebida: sucesso={resposta.Sucesso} mensagem=[{resposta.Mensagem}] canais=[{string.Join(", ", resposta.Canais)}] timestamp={resposta.Timestamp} contador={resposta.Contador}");
            return resposta;
        }

        void ReceberDurante(int milissegundos) {
            var tempo = System.Diagnostics.Stopwatch.StartNew();

            while (tempo.ElapsedMilliseconds < milissegundos) {
                if (!assinante.TryReceiveFrameString(TimeSpan.FromMilliseconds(100), out var topico))
                    continue;
                var publicacao = Publicacao.Parser.ParseFrom(assinante.ReceiveFrameBytes());
                relogio.Receber(publicacao.Contador);
                long recebidaEm = relogio.Agora();
                if (topico == publicacao.Canal + "\0" && inscricoes.Contains(publicacao.Canal))
                    Console.WriteLine($"Publicacao recebida: canal=[{publicacao.Canal}] usuario=[{publicacao.Usuario}] texto=[{publicacao.Texto}] envio={publicacao.Timestamp} recebimento={recebidaEm} contador={publicacao.Contador}");
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
                ReceberDurante(1000);
                if (Enviar("criar", novoCanal).Sucesso) canais.Add(novoCanal);
            }

            if (canais.Count == 0) { ReceberDurante(1000); continue; }

            var disponiveis = canais.Where(canal => !inscricoes.Contains(canal)).ToList();
            if (inscricoes.Count < 3 && disponiveis.Count > 0) {
                string inscricao = disponiveis[Random.Shared.Next(disponiveis.Count)];
                assinante.Subscribe(inscricao + "\0");
                inscricoes.Add(inscricao);
                Console.WriteLine($"Inscrito no canal [{inscricao}]");
            }

            ReceberDurante(1000);
            
            string canal = canais[Random.Shared.Next(canais.Count)];
            for (int i = 0; i < 10; i++) {
                Enviar("publicar", canal, $"Mensagem {Random.Shared.Next(1000, 10000)}");
                ReceberDurante(1000);
            }
        }
    }
}
