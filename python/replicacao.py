import sqlite3
from pathlib import Path
from threading import Lock
from uuid import uuid4

import chat_pb2


def ler_sql(arquivo):
    return (Path(__file__).parent / "sql" / arquivo).read_text(encoding="utf-8")


def publicacao(op):
    return chat_pb2.Publicacao(
        id=op.id, usuario=op.usuario, canal=op.canal, texto=op.texto,
        timestamp=op.timestamp, timestamp_solicitacao=op.timestamp_solicitacao,
        contador_solicitacao=op.contador,
    )


class Replicacao:
    LOTE = 100

    def __init__(self, banco, nome):
        self.banco = banco
        self.nome = nome
        self.trava = Lock()
        self.inserir = ler_sql("inserir_operacao.sql")
        self.materializar = ler_sql("materializar_canal.sql")
        self.banco.executescript(ler_sql("replicacao.sql"))

        with self.banco:
            linha = self.banco.execute("SELECT id FROM identidade_replica WHERE unica = 1").fetchone()
            if linha:
                self.replica_id = linha[0]
            else:
                self.replica_id = uuid4().hex
                self.banco.execute(ler_sql("importar_legado.sql"), {"nome": nome})
                self.banco.execute("INSERT INTO identidade_replica VALUES (1, ?)", (self.replica_id,))

    def aplicar(self, op):
        if not op.id or op.tipo not in ("login", "criar", "publicar"):
            raise ValueError("Operacao de replica invalida")

        valores = {campo.name: getattr(op, campo.name) for campo in op.DESCRIPTOR.fields}
        if self.banco.execute(self.inserir, valores).rowcount == 0:
            return

        if op.tipo == "login":
            self.banco.execute(ler_sql("registrar_login.sql"), (op.usuario, op.timestamp))
        elif op.tipo == "criar":
            self.banco.execute(self.materializar, {"canal": op.canal})
        else:
            self.banco.execute(ler_sql("registrar_publicacao.sql"),
                               (op.canal, op.usuario, op.texto, op.timestamp, op.timestamp_solicitacao))

    def _canal_existe(self, canal):
        return self.banco.execute("SELECT 1 FROM canais WHERE nome = ?", (canal,)).fetchone() is not None

    def canal_existe(self, canal):
        with self.trava:
            return self._canal_existe(canal)

    def registrar(self, op):
        with self.trava, self.banco:
            if op.tipo == "criar" and self._canal_existe(op.canal):
                return False

            self.aplicar(op)
            return True

    def listar(self):
        with self.trava:
            canais = [linha[0] for linha in self.banco.execute(ler_sql("listar_canais.sql"))]
        return chat_pb2.Resposta(sucesso=True, mensagem="Lista de canais", canais=canais)

    def historico(self, canal):
        with self.trava:
            linhas = self.banco.execute("""
                SELECT id, usuario, canal, texto, timestamp, timestamp_solicitacao, contador
                FROM operacoes WHERE tipo = 'publicar' AND (? = '' OR canal = ?)
                ORDER BY timestamp, id
            """, (canal, canal))
            publicacoes = [chat_pb2.Publicacao(
                id=id_, usuario=usuario, canal=nome, texto=texto, timestamp=timestamp,
                timestamp_solicitacao=solicitacao, contador_solicitacao=contador,
            ) for id_, usuario, nome, texto, timestamp, solicitacao, contador in linhas]
        return chat_pb2.Resposta(sucesso=True, mensagem="Historico", historico=publicacoes)

    def atender(self, pedido):
        if pedido.acao not in ("replicar", "buscar_operacoes"):
            return None

        resposta = chat_pb2.MensagemServidor(acao=pedido.acao, nome=self.nome, replica_id=self.replica_id)
        try:
            with self.trava, self.banco:
                if pedido.acao == "replicar":
                    if len(pedido.operacoes) != 1:
                        return resposta
                    self.aplicar(pedido.operacoes[0])
                else:
                    resposta.cursor = max(0, pedido.cursor) if pedido.replica_id == self.replica_id else 0
                    linhas = self.banco.execute("""
                        SELECT sequencia, id, tipo, usuario, canal, texto, timestamp, timestamp_solicitacao, contador
                        FROM operacoes WHERE sequencia > ? ORDER BY sequencia LIMIT ?
                    """, (resposta.cursor, self.LOTE))
                    for seq, id_, tipo, usuario, canal, texto, tempo, solicitacao, contador in linhas:
                        resposta.cursor = seq
                        resposta.operacoes.add(id=id_, tipo=tipo, usuario=usuario, canal=canal, texto=texto,
                                               timestamp=tempo, timestamp_solicitacao=solicitacao, contador=contador)
            resposta.sucesso = True
        except (sqlite3.Error, ValueError) as erro:
            print(f"Erro na replicacao: {erro}", flush=True)
        return resposta

    def confirmar(self, op, servidores, coordenacao):
        for servidor in servidores:
            if servidor.nome != self.nome and coordenacao.consultar(
                servidor.nome, "replicar", operacoes=[op]
            ) is not None:
                return True
        return False

    def buscar(self, servidores, coordenacao):
        for servidor in servidores:
            if servidor.nome == self.nome:
                continue

            with self.trava:
                linha = self.banco.execute(
                    "SELECT replica_id, cursor FROM cursores_replica WHERE servidor = ?", (servidor.nome,)
                ).fetchone()
            identidade, cursor = linha or ("", 0)
            resultado = coordenacao.consultar(servidor.nome, "buscar_operacoes", replica_id=identidade, cursor=cursor)
            if resultado is None:
                continue
            resposta, _ = resultado
            if (resposta.replica_id, resposta.cursor) == (identidade, cursor) and not resposta.operacoes:
                continue

            with self.trava, self.banco:
                for op in resposta.operacoes:
                    self.aplicar(op)
                self.banco.execute("""
                    INSERT INTO cursores_replica VALUES (?, ?, ?)
                    ON CONFLICT(servidor) DO UPDATE SET replica_id = excluded.replica_id, cursor = excluded.cursor
                """, (servidor.nome, resposta.replica_id, resposta.cursor))
