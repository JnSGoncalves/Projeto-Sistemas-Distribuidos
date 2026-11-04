import os
import sqlite3
import time
import zmq
from google.protobuf.message import DecodeError
import chat_pb2

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect(os.getenv("BROKER_SERVIDORES", "tcp://broker:5556"))
banco = sqlite3.connect(os.getenv("BANCO", "/data/parte1.db"))
banco.executescript("""
    CREATE TABLE IF NOT EXISTS logins (usuario TEXT NOT NULL, timestamp INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS canais (nome TEXT PRIMARY KEY, usuario TEXT NOT NULL, timestamp INTEGER NOT NULL);
""")


def processar_solicitacao(solicitacao):
    resposta = chat_pb2.Resposta()
    if solicitacao.timestamp <= 0:
        resposta.mensagem = "Timestamp invalido"
    elif not solicitacao.usuario.strip() or len(solicitacao.usuario) > 64:
        resposta.mensagem = "Usuario deve ter de 1 a 64 caracteres e nao ser vazio"
    elif solicitacao.acao == "login":
        banco.execute("INSERT INTO logins VALUES (?, ?)",
                      (solicitacao.usuario, solicitacao.timestamp))
        banco.commit()
        resposta.sucesso = True
        resposta.mensagem = "Login realizado"
    elif solicitacao.acao == "criar":
        if not solicitacao.canal.strip() or len(solicitacao.canal) > 64:
            resposta.mensagem = "Canal deve ter de 1 a 64 caracteres e nao ser vazio"
        else:
            resultado = banco.execute("INSERT OR IGNORE INTO canais VALUES (?, ?, ?)",
                                      (solicitacao.canal, solicitacao.usuario, solicitacao.timestamp))
            banco.commit()
            resposta.sucesso = resultado.rowcount == 1
            resposta.mensagem = "Canal criado" if resposta.sucesso else "Canal ja existe"
    elif solicitacao.acao == "listar":
        resposta.canais.extend(linha[0] for linha in banco.execute("SELECT nome FROM canais ORDER BY nome"))
        resposta.sucesso = True
        resposta.mensagem = "Lista de canais"
    else:
        resposta.mensagem = "Acao invalida"
    return resposta


while True:
    dados = socket.recv()
    try:
        solicitacao = chat_pb2.Solicitacao.FromString(dados)
        print(f"Recebida: acao={solicitacao.acao!r} usuario={solicitacao.usuario!r} "
              f"canal={solicitacao.canal!r} timestamp={solicitacao.timestamp}", flush=True)
        resposta = processar_solicitacao(solicitacao)
    except DecodeError:
        print(f"Protobuf invalido recebido: {dados.hex()}", flush=True)
        resposta = chat_pb2.Resposta(mensagem="Protobuf invalido")
    except sqlite3.Error as erro:
        banco.rollback()
        print(f"Erro SQLite: {erro}", flush=True)
        resposta = chat_pb2.Resposta(mensagem="Erro ao acessar banco")
    resposta.timestamp = time.time_ns() // 1_000_000
    print(f"Enviada: sucesso={resposta.sucesso} mensagem={resposta.mensagem!r} "
          f"canais={list(resposta.canais)} timestamp={resposta.timestamp}", flush=True)
    socket.send(resposta.SerializeToString())
