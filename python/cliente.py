import os
import random
import sys
from socket import gethostname
import time
import zmq
import chat_pb2
from relogio import Relogio

relogio = Relogio()


def enviar(context, usuario, acao, canal="", texto=""):
    with context.socket(zmq.REQ) as socket:
        socket.linger = 0
        socket.connect(os.getenv("BROKER_CLIENTES", "tcp://broker:5555"))
        solicitacao = chat_pb2.Solicitacao(
            acao=acao, usuario=usuario, canal=canal, texto=texto,
            timestamp=relogio.agora(), contador=relogio.enviar())
        print(f"Enviada: acao={solicitacao.acao} usuario={solicitacao.usuario!r} "
              f"canal={solicitacao.canal!r} texto={solicitacao.texto!r} "
              f"timestamp={solicitacao.timestamp} contador={solicitacao.contador}", flush=True)
        socket.send(solicitacao.SerializeToString())
        if not socket.poll(5000):
            print(f"Sem resposta para {acao} apos 5 segundos", flush=True)
            return chat_pb2.Resposta(mensagem="Tempo limite de resposta")

        resposta = chat_pb2.Resposta.FromString(socket.recv())

    relogio.receber(resposta.contador)
    print(f"Recebida: sucesso={resposta.sucesso} mensagem={resposta.mensagem!r} "
          f"canais={list(resposta.canais)} timestamp={resposta.timestamp} contador={resposta.contador}", flush=True)
    for publicacao in resposta.historico:
        print(f"Historico: {publicacao}", flush=True)
    return resposta


def realizar_login(context, usuario):
    while not enviar(context, usuario, "login").sucesso:
        time.sleep(1)


def listar_canais(context, usuario):
    return enviar(context, usuario, "listar")


def criar_canal(context, usuario, canal):
    return enviar(context, usuario, "criar", canal)


def inscrever_canal(assinante, inscricoes, canais):
    disponiveis = [canal for canal in canais if canal not in inscricoes]
    if len(inscricoes) < 3 and disponiveis:
        canal = random.choice(disponiveis)
        assinante.setsockopt(zmq.SUBSCRIBE, (canal + "\0").encode("utf-8"))
        inscricoes.add(canal)
        print(f"Inscrito no canal [{canal}]", flush=True)


def receber_durante(assinante, inscricoes, segundos):
    limite = time.monotonic() + segundos
    while (restante := limite - time.monotonic()) > 0:
        if not assinante.poll(max(1, int(restante * 1000))):
            break
        topico, dados = assinante.recv_multipart()
        publicacao = chat_pb2.Publicacao.FromString(dados)
        relogio.receber(publicacao.contador)
        recebida_em = relogio.agora()
        if topico == (publicacao.canal + "\0").encode("utf-8") and publicacao.canal in inscricoes:
            print(f"Publicacao recebida: canal={publicacao.canal!r} usuario={publicacao.usuario!r} "
                  f"texto={publicacao.texto!r} envio={publicacao.timestamp} "
                  f"recebimento={recebida_em} contador={publicacao.contador} id={publicacao.id} "
                  f"timestamp_solicitacao={publicacao.timestamp_solicitacao} "
                  f"contador_solicitacao={publicacao.contador_solicitacao}", flush=True)


def enviar_mensagens(context, usuario, canal, assinante, inscricoes):
    for _ in range(10):
        inicio = time.monotonic()
        enviar(context, usuario, "publicar", canal, f"Mensagem {random.randint(1000, 9999)}")
        receber_durante(assinante, inscricoes, inicio + 1 - time.monotonic())


def executar(usuario, historico=None):
    with zmq.Context() as context:
        if historico is not None:
            return enviar(context, usuario, "historico", historico).sucesso

        with context.socket(zmq.SUB) as assinante:
            assinante.connect(os.getenv("PROXY_INSCRICOES", "tcp://proxy:5558"))
            inscricoes = set()
            realizar_login(context, usuario)
            while True:
                resposta = listar_canais(context, usuario)
                if not resposta.sucesso:
                    receber_durante(assinante, inscricoes, 1)
                    continue
                canais = list(resposta.canais)
                if len(canais) < 5:
                    canal = f"canal-{random.getrandbits(32):08x}"
                    if criar_canal(context, usuario, canal).sucesso:
                        canais.append(canal)
                if not canais:
                    receber_durante(assinante, inscricoes, 1)
                    continue
                inscrever_canal(assinante, inscricoes, canais)
                enviar_mensagens(context, usuario, random.choice(canais), assinante, inscricoes)


if __name__ == "__main__":
    usuario = sys.argv[1] if len(sys.argv) > 1 else f"python-{gethostname()}"
    historico = (sys.argv[3] if len(sys.argv) > 3 else "") if sys.argv[2:3] == ["historico"] else None
    if not executar(usuario, historico):
        sys.exit(1)
