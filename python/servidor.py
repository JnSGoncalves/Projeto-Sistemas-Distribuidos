import os
import sqlite3
import time
from contextlib import closing
from pathlib import Path
import zmq
from google.protobuf.message import DecodeError
import chat_pb2


def carregar_sql(nome):
    return (Path(__file__).parent / "sql" / nome).read_text(encoding="utf-8")


def criar_tabelas(banco):
    banco.executescript(carregar_sql("criar_tabelas.sql"))


def registrar_login(banco, solicitacao):
    banco.execute(carregar_sql("registrar_login.sql"),
                  (solicitacao.usuario, solicitacao.timestamp))
    banco.commit()
    return chat_pb2.Resposta(sucesso=True, mensagem="Login realizado")


def criar_canal(banco, solicitacao):
    if not solicitacao.canal.strip() or len(solicitacao.canal) > 64:
        return chat_pb2.Resposta(mensagem="Canal deve ter de 1 a 64 caracteres e nao ser vazio")
    resultado = banco.execute(carregar_sql("criar_canal.sql"),
                              (solicitacao.canal, solicitacao.usuario, solicitacao.timestamp))
    banco.commit()
    sucesso = resultado.rowcount == 1
    return chat_pb2.Resposta(sucesso=sucesso,
                            mensagem="Canal criado" if sucesso else "Canal ja existe")


def listar_canais(banco):
    canais = [linha[0] for linha in banco.execute(carregar_sql("listar_canais.sql"))]
    return chat_pb2.Resposta(sucesso=True, mensagem="Lista de canais", canais=canais)


def publicar_mensagem(banco, publicador, solicitacao):
    if not solicitacao.canal.strip() or len(solicitacao.canal) > 64:
        return chat_pb2.Resposta(mensagem="Canal deve ter de 1 a 64 caracteres e nao ser vazio")
    if not solicitacao.texto.strip():
        return chat_pb2.Resposta(mensagem="Mensagem nao pode ser vazia")
    publicacao = chat_pb2.Publicacao(canal=solicitacao.canal, usuario=solicitacao.usuario,
                                   texto=solicitacao.texto, timestamp=time.time_ns() // 1_000_000)
    banco.execute(carregar_sql("registrar_publicacao.sql"),
                  (publicacao.canal, publicacao.usuario, publicacao.texto,
                   publicacao.timestamp, solicitacao.timestamp))
    banco.commit()
    publicador.send_multipart([(publicacao.canal + "\0").encode("utf-8"),
                              publicacao.SerializeToString()])
    print(f"Publicada: canal={publicacao.canal!r} usuario={publicacao.usuario!r} "
          f"texto={publicacao.texto!r} timestamp={publicacao.timestamp}", flush=True)
    return chat_pb2.Resposta(sucesso=True, mensagem="Publicacao enviada")


def processar_solicitacao(banco, solicitacao, publicador):
    if solicitacao.timestamp <= 0:
        return chat_pb2.Resposta(mensagem="Timestamp invalido")
    if not solicitacao.usuario.strip() or len(solicitacao.usuario) > 64:
        return chat_pb2.Resposta(mensagem="Usuario deve ter de 1 a 64 caracteres e nao ser vazio")
    if solicitacao.acao == "login":
        return registrar_login(banco, solicitacao)
    if solicitacao.acao == "criar":
        return criar_canal(banco, solicitacao)
    if solicitacao.acao == "listar":
        return listar_canais(banco)
    if solicitacao.acao == "publicar":
        return publicar_mensagem(banco, publicador, solicitacao)
    return chat_pb2.Resposta(mensagem="Acao invalida")


def receber_solicitacao(socket):
    dados = socket.recv()
    try:
        solicitacao = chat_pb2.Solicitacao.FromString(dados)
    except DecodeError:
        print(f"Protobuf invalido recebido: {dados.hex()}", flush=True)
        raise
    print(f"Recebida: acao={solicitacao.acao!r} usuario={solicitacao.usuario!r} "
          f"canal={solicitacao.canal!r} texto={solicitacao.texto!r} "
          f"timestamp={solicitacao.timestamp}", flush=True)
    return solicitacao


def enviar_resposta(socket, resposta):
    resposta.timestamp = time.time_ns() // 1_000_000
    print(f"Enviada: sucesso={resposta.sucesso} mensagem={resposta.mensagem!r} "
          f"canais={list(resposta.canais)} timestamp={resposta.timestamp}", flush=True)
    socket.send(resposta.SerializeToString())


def executar():
    with closing(sqlite3.connect(os.getenv("BANCO", "/data/parte1.db"))) as banco:
        criar_tabelas(banco)
        with zmq.Context() as context:
            with context.socket(zmq.REP) as socket, context.socket(zmq.PUB) as publicador:
                socket.connect(os.getenv("BROKER_SERVIDORES", "tcp://broker:5556"))
                publicador.connect(os.getenv("PROXY_PUBLICACOES", "tcp://proxy:5557"))
                while True:
                    try:
                        solicitacao = receber_solicitacao(socket)
                        resposta = processar_solicitacao(banco, solicitacao, publicador)
                    except DecodeError:
                        resposta = chat_pb2.Resposta(mensagem="Protobuf invalido")
                    except sqlite3.Error as erro:
                        banco.rollback()
                        print(f"Erro SQLite: {erro}", flush=True)
                        resposta = chat_pb2.Resposta(mensagem="Erro ao acessar banco")
                    except zmq.ZMQError as erro:
                        print(f"Erro de publicacao: {erro}", flush=True)
                        resposta = chat_pb2.Resposta(mensagem="Erro ao publicar mensagem")
                    enviar_resposta(socket, resposta)


if __name__ == "__main__":
    executar()
