using Google.Protobuf;
using NetMQ;
using NetMQ.Sockets;
using System.Diagnostics;

namespace Chat;

class Coordenacao(string nome, Relogio relogio) {
    public const string Topico = "servers";
    private volatile string coordenador = "";
    private int rankCoordenador;
    private int eleicaoPendente;
    public volatile int Rank;

    private MensagemServidor Mensagem(string acao, bool sucesso = false, long hora = 0) => new() {
        Acao = acao, Nome = nome, Rank = Rank, Sucesso = sucesso, Hora = hora,
        Timestamp = relogio.Agora(), Contador = relogio.Enviar()
    };

    public void Iniciar() {
        new Thread(AtenderServidores) { IsBackground = true }.Start();
    }

    private void AtenderServidores() {
        using var socket = new ResponseSocket();
        socket.Bind("tcp://*:5560");

        while (true) {
            var dados = socket.ReceiveFrameBytes();
            MensagemServidor resposta;
            try {
                var pedido = MensagemServidor.Parser.ParseFrom(dados);
                relogio.Receber(pedido.Contador);
                Console.WriteLine($"Servidor recebeu: {pedido}");

                bool sucesso = pedido.Acao switch {
                    "eleicao" => Rank > pedido.Rank,
                    "amostra" => true,
                    "relogio" => coordenador == nome,
                    _ => false
                };
                if (sucesso && pedido.Acao == "eleicao" && coordenador != nome)
                    Interlocked.Exchange(ref eleicaoPendente, 1);

                long hora = sucesso && pedido.Acao is "amostra" or "relogio" ? relogio.Agora() : 0;
                resposta = Mensagem(pedido.Acao, sucesso, hora);
            } catch (InvalidProtocolBufferException) {
                resposta = Mensagem("erro");
            }

            Console.WriteLine($"Servidor enviou: {resposta}");
            socket.SendFrame(resposta.ToByteArray());
        }
    }

    private (MensagemServidor Resposta, long Duracao)? Consultar(string destino, string acao) {
        try {
            using var socket = new RequestSocket();
            socket.Options.Linger = TimeSpan.Zero;
            socket.Connect($"tcp://{destino}:5560");
            var pedido = Mensagem(acao);
            Console.WriteLine($"Servidor enviou para {destino}: {pedido}");

            var tempo = Stopwatch.StartNew();
            socket.SendFrame(pedido.ToByteArray());
            if (!socket.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(500), out var dados)) return null;

            var resposta = MensagemServidor.Parser.ParseFrom(dados);
            relogio.Receber(resposta.Contador);
            Console.WriteLine($"Servidor recebeu de {destino}: {resposta}");
            return resposta.Sucesso && resposta.Nome == destino ? (resposta, tempo.ElapsedMilliseconds) : null;
        } catch (Exception erro) when (erro is NetMQException or System.Net.Sockets.SocketException
            or ArgumentException or InvalidProtocolBufferException) {
            return null;
        }
    }

    public bool EleicaoPendente() => Interlocked.Exchange(ref eleicaoPendente, 0) == 1;

    public void ReceberAnuncios(SubscriberSocket assinante) {
        NetMQMessage? quadros = null;
        while (assinante.TryReceiveMultipartMessage(TimeSpan.Zero, ref quadros)) {
            if (quadros!.FrameCount != 2 || quadros[0].ConvertToString() != Topico) continue;
            try {
                var aviso = MensagemServidor.Parser.ParseFrom(quadros[1].ToByteArray());
                relogio.Receber(aviso.Contador);
                Console.WriteLine($"Anuncio recebido: {aviso}");
                if (aviso.Acao == "coordenador" && aviso.Rank >= Math.Max(Rank, rankCoordenador))
                    DefinirCoordenador(aviso.Nome, aviso.Rank);
            } catch (InvalidProtocolBufferException) {
                Console.WriteLine("Anuncio invalido");
            }
        }
    }

    private void DefinirCoordenador(string eleito, int rank) {
        if (coordenador != eleito) Console.WriteLine($"Coordenador: {eleito} rank={rank}");
        coordenador = eleito;
        rankCoordenador = rank;
    }

    public void Atualizar(IEnumerable<ServidorInfo> servidores, PublisherSocket publicador) {
        var pares = servidores.Where(s => s.Nome != nome).OrderByDescending(s => s.Rank).ToList();
        foreach (var par in pares.Where(s => s.Rank > Rank)) {
            if (Consultar(par.Nome, "eleicao") is null) continue;

            DefinirCoordenador(par.Nome, par.Rank);
            if (Consultar(par.Nome, "relogio") is { } hora) {
                relogio.Sincronizar(hora.Resposta.Hora, hora.Duracao);
                Console.WriteLine($"Relogio sincronizado por {par.Nome}: {relogio.Agora()}");
            }
            return;
        }

        long soma = 0;
        int quantidade = 1; // A amostra do coordenador tem desvio zero.
        foreach (var par in pares) {
            if (Consultar(par.Nome, "amostra") is not { } amostra) continue;
            soma += amostra.Resposta.Hora + amostra.Duracao / 2 - relogio.Agora();
            quantidade++;
        }

        relogio.Ajustar(soma / quantidade);
        DefinirCoordenador(nome, Rank);
        Console.WriteLine($"Berkeley: coordenador={nome} amostras={quantidade} hora={relogio.Agora()}");

        var aviso = Mensagem("coordenador", true);
        publicador.SendMoreFrame(Topico).SendFrame(aviso.ToByteArray());
        Console.WriteLine($"Anuncio enviado: {aviso}");
    }
}
