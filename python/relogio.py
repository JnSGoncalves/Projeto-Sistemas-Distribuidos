import time
from threading import Lock


class Relogio:
    def __init__(self):
        self.contador = 0
        self.ajuste = 0
        self._trava = Lock()

    def enviar(self):
        with self._trava:
            self.contador += 1
            return self.contador

    def receber(self, recebido):
        with self._trava:
            self.contador = max(self.contador, recebido)

    def agora(self):
        with self._trava:
            return time.time_ns() // 1_000_000 + self.ajuste

    def sincronizar(self, hora, duracao):
        with self._trava:
            self.ajuste = hora + duracao // 2 - time.time_ns() // 1_000_000

    def ajustar(self, desvio):
        with self._trava:
            self.ajuste += desvio
