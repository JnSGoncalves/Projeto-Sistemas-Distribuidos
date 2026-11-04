using Google.Protobuf;
using NetMQ;
using NetMQ.Sockets;

namespace Chat;

class Cliente
{
    public void Executar(string usuario)
    {
        using var socket = new RequestSocket();
        socket.Connect(Environment.GetEnvironmentVariable("BROKER_CLIENTES") ?? "tcp://broker:5555");

        Resposta Enviar(string acao, string canal = "")
        {
            var solicitacao = new Solicitacao { Acao = acao, Usuario = usuario, Canal = canal,
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() };
            Console.WriteLine($"Enviada: acao={solicitacao.Acao} usuario=[{solicitacao.Usuario}] canal=[{solicitacao.Canal}] timestamp={solicitacao.Timestamp}");
            socket.SendFrame(solicitacao.ToByteArray());
            var resposta = Resposta.Parser.ParseFrom(socket.ReceiveFrameBytes());
            Console.WriteLine($"Recebida: sucesso={resposta.Sucesso} mensagem=[{resposta.Mensagem}] canais=[{string.Join(", ", resposta.Canais)}] timestamp={resposta.Timestamp}");
            return resposta;
        }

        while (!Enviar("login").Sucesso)
            Thread.Sleep(1000);

        while (true)
        {
            var resposta = Enviar("listar");
            string canal = $"canal-{usuario}";
            if (resposta.Sucesso && !resposta.Canais.Contains(canal))
                Enviar("criar", canal);
            Thread.Sleep(2000);
        }
    }
}
