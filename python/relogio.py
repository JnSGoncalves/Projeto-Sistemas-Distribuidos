import time


class Relogio:
    def __init__(self):
        self.contador = 0
        self.ajuste = 0

    def enviar(self):
        self.contador += 1
        return self.contador

    def receber(self, recebido):
        self.contador = max(self.contador, recebido)

    def agora(self):
        return time.time_ns() // 1_000_000 + self.ajuste

    def sincronizar(self, hora, duracao):
        self.ajuste = hora + duracao // 2 - time.time_ns() // 1_000_000
