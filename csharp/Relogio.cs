namespace Chat;

class Relogio {
    private long contador;
    private long ajuste;

    private readonly object trava = new();

    public long Enviar() {
        lock (trava) return ++contador;
    }

    public void Receber(long recebido) {
        lock (trava) contador = Math.Max(contador, recebido);
    }

    public long Agora() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + Interlocked.Read(ref ajuste);

    public void Sincronizar(long hora, long duracao) {
        Interlocked.Exchange(ref ajuste, hora + duracao / 2 - DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
    }

    public void Ajustar(long desvio) => Interlocked.Add(ref ajuste, desvio);
}
