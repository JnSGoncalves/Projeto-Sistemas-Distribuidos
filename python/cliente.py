import os
import random
import sys
from socket import gethostname
import time
import zmq
import chat_pb2


def enviar(socket, usuario, acao, canal="", texto=""):
    solicitacao = chat_pb2.Solicitacao(
        acao=acao, usuario=usuario, canal=canal, texto=texto,
        timestamp=time.time_ns() // 1_000_000)
    print(f"Enviada: acao={solicitacao.acao} usuario={solicitacao.usuario!r} "
          f"canal={solicitacao.canal!r} texto={solicitacao.texto!r} "
          f"timestamp={solicitacao.timestamp}", flush=True)
    socket.send(solicitacao.SerializeToString())
    resposta = chat_pb2.Resposta.FromString(socket.recv())
    print(f"Recebida: sucesso={resposta.sucesso} mensagem={resposta.mensagem!r} "
          f"canais={list(resposta.canais)} timestamp={resposta.timestamp}", flush=True)
    return resposta


def realizar_login(socket, usuario):
    while not enviar(socket, usuario, "login").sucesso:
        time.sleep(1)


def listar_canais(socket, usuario):
    return enviar(socket, usuario, "listar")


def criar_canal(socket, usuario, canal):
    return enviar(socket, usuario, "criar", canal)


def inscrever_canal(assinante, inscricoes, canais):
    disponiveis = [canal for canal in canais if canal not in inscricoes]
    if len(inscricoes) < 3 and disponiveis:
        canal = random.choice(disponiveis)
        assinante.setsockopt(zmq.SUBSCRIBE, (canal + "\0").encode("utf-8"))
        inscricoes.add(canal)
        print(f"Inscrito no canal [{canal}]", flush=True)


def receber_durante(assinante, inscricoes, segundos):
    limite = time.monotonic() + segundos
    while time.monotonic() < limite:
        if not assinante.poll(100):
            continue
        topico, dados = assinante.recv_multipart()
        publicacao = chat_pb2.Publicacao.FromString(dados)
        recebida_em = time.time_ns() // 1_000_000
        if topico == (publicacao.canal + "\0").encode("utf-8") and publicacao.canal in inscricoes:
            print(f"Publicacao recebida: canal={publicacao.canal!r} usuario={publicacao.usuario!r} "
                  f"texto={publicacao.texto!r} envio={publicacao.timestamp} "
                  f"recebimento={recebida_em}", flush=True)


def enviar_mensagens(socket, usuario, canal, assinante, inscricoes):
    for _ in range(10):
        enviar(socket, usuario, "publicar", canal, f"Mensagem {random.randint(1000, 9999)}")
        receber_durante(assinante, inscricoes, 1)


def executar(usuario):
    with zmq.Context() as context:
        with context.socket(zmq.REQ) as socket, context.socket(zmq.SUB) as assinante:
            socket.connect(os.getenv("BROKER_CLIENTES", "tcp://broker:5555"))
            assinante.connect(os.getenv("PROXY_INSCRICOES", "tcp://proxy:5558"))
            inscricoes = set()
            realizar_login(socket, usuario)
            while True:
                resposta = listar_canais(socket, usuario)
                if not resposta.sucesso:
                    receber_durante(assinante, inscricoes, 1)
                    continue
                canais = list(resposta.canais)
                if len(canais) < 5:
                    canal = f"canal-{random.getrandbits(32):08x}"
                    receber_durante(assinante, inscricoes, 1)
                    if criar_canal(socket, usuario, canal).sucesso:
                        canais.append(canal)
                if not canais:
                    receber_durante(assinante, inscricoes, 1)
                    continue
                inscrever_canal(assinante, inscricoes, canais)
                receber_durante(assinante, inscricoes, 1)
                enviar_mensagens(socket, usuario, random.choice(canais), assinante, inscricoes)


if __name__ == "__main__":
    usuario = sys.argv[1] if len(sys.argv) > 1 else f"python-{gethostname()}"
    executar(usuario)
