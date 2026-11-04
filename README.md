# Projeto de Sistemas Distribuídos

Sistema de troca de mensagens em canais públicos, com clientes automatizados
(bots) e servidores em C# e Python. A comunicação utiliza ZeroMQ, as mensagens
são serializadas com Protobuf e cada servidor mantém seu próprio banco SQLite.

## Etapas do projeto

| Parte | Objetivo | Situação |
| --- | --- | --- |
| 1 | Login de usuários, criação e listagem de canais, com persistência. | Implementada |
| 2 | Inscrição e publicação de mensagens em canais com Pub/Sub. | Prevista |
| 3 | Relógios lógicos, sincronização física e heartbeat dos servidores. | Prevista |
| 4 | Eleição de coordenador e sincronização dos relógios com Berkeley. | Prevista |
| 5 | Consistência e replicação dos dados entre todos os servidores. | Prevista |

## Estrutura

```text
broker/
  broker.py          encaminhamento entre clientes e servidores
  Dockerfile         imagem do broker
proto/
  chat.proto         contrato compartilhado das mensagens
python/
  cliente.py         bot Python
  servidor.py        servidor Python e persistência SQLite
  requirements.txt   dependências Python
  Dockerfile         imagem dos processos Python
csharp/
  Program.cs         inicialização do cliente ou servidor
  Cliente.cs         bot C#
  Servidor.cs        servidor C# e persistência SQLite
  Chat.csproj        projeto .NET e dependências
  Dockerfile         imagem dos processos C#
docker-compose.yaml  configuração dos serviços e volumes
```

As classes Protobuf das duas linguagens são geradas durante a construção das
imagens a partir de `proto/chat.proto`.

## Execução

É necessário ter Docker e Docker Compose disponíveis, com o Docker Desktop em
modo Linux. Na raiz do projeto, execute:

```sh
docker compose up --build
```

São iniciados nove containers: um broker, quatro bots e quatro servidores,
sendo dois bots e dois servidores de cada linguagem. A comunicação ocorre na
rede do Compose; o broker recebe os clientes na porta 5555 e conecta os
servidores pela porta 5556.

Os bots realizam login e, a cada dois segundos, consultam os canais e solicitam
a criação de seu canal quando necessário. A execução não exige entrada pelo
teclado. Clientes e servidores exibem as mensagens enviadas e recebidas.

Para executar em segundo plano:

```sh
docker compose up -d --build
```

Para acompanhar os serviços e suas mensagens:

```sh
docker compose ps
docker compose logs -f
```

Para encerrar, use Ctrl+C se estiver acompanhando a execução e execute:

```sh
docker compose down
```

Cada servidor armazena seu banco em `/data/parte1.db`, dentro de um volume
exclusivo. Os dados são preservados entre execuções. Para encerrar e apagar os
bancos, utilize `docker compose down -v`.
