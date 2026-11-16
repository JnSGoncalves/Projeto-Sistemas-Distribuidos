import os
import sqlite3
import time
from contextlib import closing
from socket import gethostname
from uuid import uuid4
import zmq
from google.protobuf.message import DecodeError
import chat_pb2
from coordenacao import Coordenacao
from relogio import Relogio
from replicacao import Replicacao, ler_sql, publicacao

relogio = Relogio()


def recuperar_replicas(replicacao, replicas, coordenacao):
    try:
        replicacao.buscar(replicas, coordenacao)
    except (sqlite3.Error, ValueError) as erro:
        print(f"Erro ao recuperar replica: {erro}", flush=True)


def processar_solicitacao(replicacao, solicitacao, publicador, replicas, coordenacao):
    def erro(mensagem):
        return chat_pb2.Resposta(mensagem=mensagem)

    if solicitacao.timestamp <= 0:
        return erro("Timestamp invalido")
    if not solicitacao.usuario.strip() or len(solicitacao.usuario) > 64:
        return erro("Usuario deve ter de 1 a 64 caracteres e nao ser vazio")

    acao = solicitacao.acao
    if acao == "listar":
        return replicacao.listar()
    if acao == "historico":
        return replicacao.historico(solicitacao.canal)
    if acao not in ("login", "criar", "publicar"):
        return erro("Acao invalida")
    if acao != "login" and (not solicitacao.canal.strip() or len(solicitacao.canal) > 64):
        return erro("Canal deve ter de 1 a 64 caracteres e nao ser vazio")
    if acao == "publicar" and not solicitacao.texto.strip():
        return erro("Mensagem nao pode ser vazia")

    if not any(s.nome != coordenacao.nome for s in replicas):
        lista = consultar_referencia(coordenacao.context, coordenacao.nome, "listar_replicas")
        if lista is not None:
            replicas[:] = lista.servidores
    if acao == "publicar" and not replicacao.canal_existe(solicitacao.canal):
        recuperar_replicas(replicacao, replicas, coordenacao)
        if not replicacao.canal_existe(solicitacao.canal):
            return erro("Canal nao encontrado")
    if not any(s.nome != coordenacao.nome for s in replicas):
        return erro("Nenhuma replica disponivel")

    op = chat_pb2.Operacao(
        id=uuid4().hex, tipo=acao, usuario=solicitacao.usuario, canal=solicitacao.canal,
        texto=solicitacao.texto, contador=solicitacao.contador,
        timestamp=relogio.agora() if acao == "publicar" else solicitacao.timestamp,
        timestamp_solicitacao=solicitacao.timestamp,
    )
    if not replicacao.registrar(op):
        return erro("Canal ja existe")
    if not replicacao.confirmar(op, replicas, coordenacao):
        return erro("Escrita local sem confirmacao de outra replica")

    if acao == "publicar":
        mensagem = publicacao(op)
        mensagem.contador = relogio.enviar()
        publicador.send_multipart([(op.canal + "\0").encode("utf-8"), mensagem.SerializeToString()])
        print(f"Publicada: {mensagem}", flush=True)

    mensagens = {"login": "Login realizado", "criar": "Canal criado", "publicar": "Publicacao enviada"}
    return chat_pb2.Resposta(sucesso=True, mensagem=mensagens[acao])


def receber_solicitacao(socket):
    dados = socket.recv()
    try:
        solicitacao = chat_pb2.Solicitacao.FromString(dados)
    except DecodeError:
        print(f"Protobuf invalido recebido: {dados.hex()}", flush=True)
        raise
    relogio.receber(solicitacao.contador)
    print(f"Recebida: acao={solicitacao.acao!r} usuario={solicitacao.usuario!r} "
          f"canal={solicitacao.canal!r} texto={solicitacao.texto!r} "
          f"timestamp={solicitacao.timestamp} contador={solicitacao.contador}", flush=True)
    return solicitacao


def enviar_resposta(socket, resposta):
    resposta.timestamp = relogio.agora()
    resposta.contador = relogio.enviar()
    print(f"Enviada: sucesso={resposta.sucesso} mensagem={resposta.mensagem!r} "
          f"canais={list(resposta.canais)} timestamp={resposta.timestamp} contador={resposta.contador}", flush=True)
    if resposta.historico:
        print(f"Historico enviado: {resposta}", flush=True)
    socket.send(resposta.SerializeToString())


def consultar_referencia(context, nome, acao):
    with context.socket(zmq.REQ) as referencia:
        referencia.linger = 0
        referencia.connect(os.getenv("REFERENCIA_ENDPOINT", "tcp://referencia:5559"))
        pedido = chat_pb2.PedidoReferencia(acao=acao, nome=nome,
                                         timestamp=relogio.agora(), contador=relogio.enviar())
        print(f"Referencia enviada: {pedido}", flush=True)
        referencia.send(pedido.SerializeToString())
        if not referencia.poll(3000):
            print("Referencia sem resposta", flush=True)
            return None

        resposta = chat_pb2.RespostaReferencia.FromString(referencia.recv())
        relogio.receber(resposta.contador)
        print(f"Referencia recebida: {resposta}", flush=True)

        return resposta if resposta.sucesso else None


def atender_clientes(context, replicacao, socket, publicador, assinante, coordenacao):
    servidores = []
    replicas = []
    proxima_busca = 0
    recebidas = 0
    trocadas = 15  # A primeira rodada acontece antes de atender os clientes.

    while True:
        coordenacao.receber_anuncios(assinante)
        if time.monotonic() >= proxima_busca:
            lista = consultar_referencia(context, coordenacao.nome, "listar_replicas")
            if lista is not None:
                replicas[:] = lista.servidores
            recuperar_replicas(replicacao, replicas, coordenacao)
            proxima_busca = time.monotonic() + 2

        if coordenacao.eleicao_pendente() or trocadas >= 15:
            trocadas %= 15
            lista = consultar_referencia(context, coordenacao.nome, "listar")
            if lista is not None:
                servidores = list(lista.servidores)
            coordenacao.atualizar(servidores, publicador)

        if not socket.poll(100):
            continue

        publicou = False
        try:
            solicitacao = receber_solicitacao(socket)
            resposta = processar_solicitacao(replicacao, solicitacao, publicador, replicas, coordenacao)
            publicou = solicitacao.acao == "publicar" and resposta.sucesso
        except DecodeError:
            resposta = chat_pb2.Resposta(mensagem="Protobuf invalido")
        except sqlite3.Error as erro:
            print(f"Erro SQLite: {erro}", flush=True)
            resposta = chat_pb2.Resposta(mensagem="Erro ao acessar banco")
        except zmq.ZMQError as erro:
            print(f"Erro de publicacao: {erro}", flush=True)
            resposta = chat_pb2.Resposta(mensagem="Erro ao publicar mensagem")

        enviar_resposta(socket, resposta)
        trocadas += 2 + int(publicou)
        recebidas += 1

        if recebidas % 10 == 0:
            heartbeat = consultar_referencia(context, coordenacao.nome, "heartbeat")
            if heartbeat is not None:
                coordenacao.rank = heartbeat.rank


def executar():
    with closing(sqlite3.connect(os.getenv("BANCO", "/data/parte1.db"), check_same_thread=False)) as banco:
        banco.executescript(ler_sql("criar_tabelas.sql"))
        with zmq.Context() as context:
            with (
                context.socket(zmq.REP) as socket,
                context.socket(zmq.PUB) as publicador,
                context.socket(zmq.SUB) as assinante,
            ):
                socket.connect(os.getenv("BROKER_SERVIDORES", "tcp://broker:5556"))
                publicador.connect(os.getenv("PROXY_PUBLICACOES", "tcp://proxy:5557"))
                assinante.setsockopt(zmq.SUBSCRIBE, Coordenacao.TOPICO)
                assinante.connect(os.getenv("PROXY_INSCRICOES", "tcp://proxy:5558"))

                nome = os.getenv("NOME_SERVIDOR", gethostname())
                while (registro := consultar_referencia(context, nome, "registrar")) is None:
                    time.sleep(1)

                replicacao = Replicacao(banco, nome)
                coordenacao = Coordenacao(context, nome, relogio)
                coordenacao.atender_replica = replicacao.atender
                coordenacao.rank = registro.rank
                coordenacao.iniciar()
                try:
                    atender_clientes(context, replicacao, socket, publicador, assinante, coordenacao)
                finally:
                    coordenacao.encerrar()


if __name__ == "__main__":
    executar()
