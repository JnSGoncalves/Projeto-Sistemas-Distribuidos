import time
from socket import gethostbyname
from threading import Event, Lock, Thread

import zmq
from google.protobuf.message import DecodeError

import chat_pb2


class Coordenacao:
    TOPICO = b"servers"

    def __init__(self, context, nome, relogio):
        self.context = context
        self.nome = nome
        self.relogio = relogio
        self.rank = 0
        self.coordenador = ""
        self.rank_coordenador = 0
        self._eleicao_pendente = False
        self._trava = Lock()
        self._enderecos = {}
        self._resolucoes = {}
        self._parar = Event()
        self._atendente = Thread(target=self.atender_servidores, daemon=True)

    def iniciar(self):
        self._atendente.start()

    def encerrar(self):
        self._parar.set()
        self._atendente.join()

    def mensagem(self, acao, sucesso=False, hora=0):
        return chat_pb2.MensagemServidor(
            acao=acao, nome=self.nome, rank=self.rank, sucesso=sucesso, hora=hora,
            timestamp=self.relogio.agora(), contador=self.relogio.enviar(),
        )

    def atender_servidores(self):
        with self.context.socket(zmq.REP) as socket:
            socket.linger = 0
            socket.bind("tcp://*:5560")

            while not self._parar.is_set():
                if not socket.poll(100):
                    continue

                try:
                    pedido = chat_pb2.MensagemServidor.FromString(socket.recv())
                    self.relogio.receber(pedido.contador)
                    print(f"Servidor recebeu: {pedido}", flush=True)

                    sucesso = False
                    hora = 0
                    if pedido.acao == "eleicao" and self.rank > pedido.rank:
                        sucesso = True
                        if self.coordenador != self.nome:
                            with self._trava:
                                self._eleicao_pendente = True
                    elif pedido.acao == "amostra" or (
                        pedido.acao == "relogio" and self.coordenador == self.nome
                    ):
                        sucesso = True
                        hora = self.relogio.agora()

                    resposta = self.mensagem(pedido.acao, sucesso, hora)
                except DecodeError:
                    resposta = self.mensagem("erro")

                print(f"Servidor enviou: {resposta}", flush=True)
                socket.send(resposta.SerializeToString())

    def resolver(self, destino, atualizar=False):
        with self._trava:
            endereco = self._enderecos.get(destino)
            if endereco and not atualizar:
                return endereco

            pronto = self._resolucoes.get(destino)
            if pronto is None:
                pronto = Event()
                self._resolucoes[destino] = pronto
                Thread(target=self.resolver_em_fundo, args=(destino, pronto), daemon=True).start()

        if atualizar:
            return None

        pronto.wait(0.2)
        with self._trava:
            return self._enderecos.get(destino)

    def resolver_em_fundo(self, destino, pronto):
        try:
            endereco = gethostbyname(destino)
        except OSError:
            endereco = None

        with self._trava:
            if endereco:
                self._enderecos[destino] = endereco
            self._resolucoes.pop(destino)
            pronto.set()

    def consultar(self, destino, acao):
        # Um nome fora do ar não deve bloquear as consultas aos outros servidores.
        endereco = self.resolver(destino)
        if endereco is None:
            return None

        try:
            with self.context.socket(zmq.REQ) as socket:
                socket.linger = 0
                socket.connect(f"tcp://{endereco}:5560")
                pedido = self.mensagem(acao)
                print(f"Servidor enviou para {destino}: {pedido}", flush=True)

                inicio = time.monotonic_ns()
                socket.send(pedido.SerializeToString())
                if not socket.poll(500):
                    self.resolver(destino, atualizar=True)
                    return None

                resposta = chat_pb2.MensagemServidor.FromString(socket.recv())
                duracao = (time.monotonic_ns() - inicio) // 1_000_000
                self.relogio.receber(resposta.contador)
                print(f"Servidor recebeu de {destino}: {resposta}", flush=True)
                if resposta.nome == destino:
                    return (resposta, duracao) if resposta.sucesso else None
        except (zmq.ZMQError, DecodeError):
            pass

        self.resolver(destino, atualizar=True)
        return None

    def eleicao_pendente(self):
        with self._trava:
            pendente = self._eleicao_pendente
            self._eleicao_pendente = False
            return pendente

    def definir_coordenador(self, eleito, rank):
        if self.coordenador != eleito:
            print(f"Coordenador: {eleito} rank={rank}", flush=True)
        self.coordenador = eleito
        self.rank_coordenador = rank

    def receber_anuncios(self, assinante):
        while assinante.poll(0):
            quadros = assinante.recv_multipart()
            if len(quadros) != 2 or quadros[0] != self.TOPICO:
                continue

            try:
                aviso = chat_pb2.MensagemServidor.FromString(quadros[1])
                self.relogio.receber(aviso.contador)
                print(f"Anuncio recebido: {aviso}", flush=True)
                if aviso.acao == "coordenador" and aviso.rank >= max(self.rank, self.rank_coordenador):
                    self.definir_coordenador(aviso.nome, aviso.rank)
            except DecodeError:
                print("Anuncio invalido", flush=True)

    def atualizar(self, servidores, publicador):
        pares = sorted((s for s in servidores if s.nome != self.nome),
                       key=lambda s: s.rank, reverse=True)

        for par in pares:
            if par.rank <= self.rank or self.consultar(par.nome, "eleicao") is None:
                continue

            self.definir_coordenador(par.nome, par.rank)
            resultado = self.consultar(par.nome, "relogio")
            if resultado is not None:
                resposta, duracao = resultado
                self.relogio.sincronizar(resposta.hora, duracao)
                print(f"Relogio sincronizado por {par.nome}: {self.relogio.agora()}", flush=True)
            return

        soma = 0
        quantidade = 1  # A amostra do coordenador tem desvio zero.
        for par in pares:
            resultado = self.consultar(par.nome, "amostra")
            if resultado is not None:
                resposta, duracao = resultado
                soma += resposta.hora + duracao // 2 - self.relogio.agora()
                quantidade += 1

        media = soma // quantidade if soma >= 0 else -(-soma // quantidade)
        self.relogio.ajustar(media)
        self.definir_coordenador(self.nome, self.rank)
        print(f"Berkeley: coordenador={self.nome} amostras={quantidade} hora={self.relogio.agora()}", flush=True)

        aviso = self.mensagem("coordenador", True)
        publicador.send_multipart([self.TOPICO, aviso.SerializeToString()])
        print(f"Anuncio enviado: {aviso}", flush=True)
