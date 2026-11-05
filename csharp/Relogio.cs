namespace Chat;

class Relogio {
    private long contador;
    private long ajuste;

    public long Enviar() => ++contador;

    public void Receber(long recebido) {
        contador = Math.Max(contador, recebido);
    }

    public long Agora() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + ajuste;

    public void Sincronizar(long hora, long duracao) {
        ajuste = hora + duracao / 2 - DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }
}
