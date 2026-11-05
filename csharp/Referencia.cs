using Google.Protobuf;
using NetMQ;
using NetMQ.Sockets;
using System.Diagnostics;

namespace Chat;

class Referencia {
    public void Executar() {
        var relogio = new Relogio();
        var tempo = Stopwatch.StartNew();
        long limite = long.Parse(Environment.GetEnvironmentVariable("HEARTBEAT_TIMEOUT_SECONDS") ?? "60") * 1000;
        var ranks = new Dictionary<string, int>();
        var ativos = new Dictionary<string, long>();

        using var socket = new ResponseSocket();
        socket.Bind(Environment.GetEnvironmentVariable("REFERENCIA_BIND") ?? "tcp://*:5559");

        while (true) {
            foreach (var nome in ativos.Keys.ToList()) {
                if (tempo.ElapsedMilliseconds - ativos[nome] > limite) {
                    ativos.Remove(nome);
                    Console.WriteLine($"Servidor removido por falta de heartbeat: {nome}");
                }
            }

            if (!socket.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(250), out var dados))
                continue;

            var resposta = new RespostaReferencia();
            try {
                var pedido = PedidoReferencia.Parser.ParseFrom(dados);
                relogio.Receber(pedido.Contador);
                Console.WriteLine($"Referencia recebida: {pedido}");

                if (pedido.Timestamp <= 0 || string.IsNullOrWhiteSpace(pedido.Nome)) {
                    resposta.Mensagem = "Nome e timestamp obrigatorios";
                } else if (pedido.Acao is "registrar" or "heartbeat") {
                    if (!ranks.ContainsKey(pedido.Nome)) ranks[pedido.Nome] = ranks.Count + 1;
                    ativos[pedido.Nome] = tempo.ElapsedMilliseconds;
                    resposta.Rank = ranks[pedido.Nome];
                    resposta.Sucesso = true;
                } else if (pedido.Acao == "listar") {
                    foreach (var nome in ativos.Keys.Order())
                        resposta.Servidores.Add(new ServidorInfo { Nome = nome, Rank = ranks[nome] });
                    resposta.Sucesso = true;
                } else {
                    resposta.Mensagem = "Acao invalida";
                }
            } catch (InvalidProtocolBufferException) {
                resposta.Mensagem = "Protobuf invalido";
            }

            resposta.Timestamp = relogio.Agora();
            resposta.Contador = relogio.Enviar();
            Console.WriteLine($"Referencia enviada: {resposta}");
            socket.SendFrame(resposta.ToByteArray());
        }
    }
}
