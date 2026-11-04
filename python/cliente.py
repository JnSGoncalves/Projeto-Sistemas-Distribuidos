import os
import sys
from socket import gethostname
import time
import zmq
import chat_pb2

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect(os.getenv("BROKER_CLIENTES", "tcp://broker:5555"))
usuario = sys.argv[1] if len(sys.argv) > 1 else f"python-{gethostname()}"


def enviar(acao, canal=""):
    solicitacao = chat_pb2.Solicitacao(
        acao=acao, usuario=usuario, canal=canal,
        timestamp=time.time_ns() // 1_000_000)
    print(f"Enviada: acao={solicitacao.acao} usuario={solicitacao.usuario!r} "
          f"canal={solicitacao.canal!r} timestamp={solicitacao.timestamp}", flush=True)
    socket.send(solicitacao.SerializeToString())
    resposta = chat_pb2.Resposta.FromString(socket.recv())
    print(f"Recebida: sucesso={resposta.sucesso} mensagem={resposta.mensagem!r} "
          f"canais={list(resposta.canais)} timestamp={resposta.timestamp}", flush=True)
    return resposta


while not enviar("login").sucesso:
    time.sleep(1)

while True:
    resposta = enviar("listar")
    canal = f"canal-{usuario}"
    if resposta.sucesso and canal not in resposta.canais:
        enviar("criar", canal)
    time.sleep(2)
