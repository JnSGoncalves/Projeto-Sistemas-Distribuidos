using Chat;

if (args.Length > 0 && args[0] == "cliente")
    new Cliente().Executar(args.Length > 1 ? args[1] : $"csharp-{Environment.MachineName}");
else if (args.Length == 1 && args[0] == "servidor")
    new Servidor().Executar();
else if (args.Length == 1 && args[0] == "referencia")
    new Referencia().Executar();
else
    Console.WriteLine("Uso: Chat cliente [usuario] | servidor | referencia");
