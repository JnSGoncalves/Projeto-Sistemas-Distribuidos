# Projeto de Sistemas Distribuídos

Sistema de troca de mensagens em canais públicos, com clientes automatizados
(bots) e servidores em C# e Python. A comunicação utiliza ZeroMQ, as mensagens
são serializadas com Protobuf e cada servidor mantém seu próprio banco SQLite.

## Etapas do projeto

| Parte | Objetivo | Situação |
| --- | --- | --- |
| 1 | Login de usuários, criação e listagem de canais, com persistência. | Implementada |
| 2 | Inscrição e publicação de mensagens em canais com Pub/Sub. | Implementada |
| 3 | Relógios lógicos, sincronização física e heartbeat dos servidores. | Prevista |
| 4 | Eleição de coordenador e sincronização dos relógios com Berkeley. | Prevista |
| 5 | Consistência e replicação dos dados entre todos os servidores. | Prevista |

## Estrutura

```text
broker/
  broker.py          encaminhamento entre clientes e servidores
  Dockerfile         imagem do broker
proxy/
  proxy.py           encaminhamento das publicações e inscrições
  Dockerfile         imagem do proxy Pub/Sub
proto/
  chat.proto         contrato compartilhado das mensagens
python/
  cliente.py         bot Python
  servidor.py        servidor Python e persistência SQLite
  sql/               scripts de criação, consulta e gravação nas tabelas Python
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

São iniciados dez containers: broker, proxy, dois bots e dois servidores de cada
linguagem. A comunicação ocorre na rede do Compose. O broker recebe os
clientes na porta 5555 e conecta os servidores pela porta 5556. O proxy recebe
publicações na porta 5557 (XSUB) e distribui aos assinantes na porta 5558 (XPUB).

Os bots realizam login, criam um canal quando encontram menos de cinco e
adicionam uma inscrição aleatória enquanto possuem menos de três inscrições.
Em seguida, escolhem um canal e enviam um lote de dez mensagens aleatórias,
com intervalo de um segundo entre mensagens. As inscrições e os canais são
consultados novamente a cada lote. As publicações recebidas mostram canal,
usuário, texto e timestamps de envio e recebimento.

A execução não exige entrada pelo teclado. Clientes e servidores exibem as
mensagens enviadas e recebidas. Cada servidor mantém seus canais e publicações
no próprio banco; a replicação entre servidores está prevista na Parte 5.

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
