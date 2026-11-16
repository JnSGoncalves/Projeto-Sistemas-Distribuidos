# Projeto de Sistemas Distribuídos

Sistema de troca de mensagens em canais públicos, com clientes automatizados
(bots) e servidores em C# e Python. A comunicação utiliza ZeroMQ, as mensagens
são serializadas com Protobuf e cada servidor mantém seu próprio banco SQLite.

## Etapas do projeto

| Parte | Objetivo | Situação |
| --- | --- | --- |
| 1 | Login de usuários, criação e listagem de canais, com persistência. | Implementada |
| 2 | Inscrição e publicação de mensagens em canais com Pub/Sub. | Implementada |
| 3 | Relógios lógicos, sincronização física e heartbeat dos servidores. | Implementada |
| 4 | Eleição de coordenador e sincronização dos relógios com Berkeley. | Implementada |
| 5 | Replicação de logins, canais e publicações, com recuperação e consulta do histórico. | Implementada |

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
  coordenacao.py     eleição e sincronização entre servidores
  replicacao.py      registro, confirmação e recuperação das operações
  relogio.py         contador logico e relogio ajustado
  sql/               scripts SQLite, incluindo os usados nas duas linguagens
  requirements.txt   dependências Python
  Dockerfile         imagem dos processos Python
csharp/
  Program.cs         inicialização do cliente, servidor ou serviço de referência
  Cliente.cs         bot C#
  Servidor.cs        servidor C# e persistência SQLite
  Coordenacao.cs     eleição e sincronização entre servidores
  Replicacao.cs      registro, confirmação e recuperação das operações
  Relogio.cs         contador logico e relogio ajustado
  Referencia.cs      ranks e disponibilidade dos servidores
  Chat.csproj        projeto .NET e dependências
  Dockerfile         imagem dos processos C#
docs/
  roteiro-apresentacao.md  roteiro e comandos para a demonstração
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

São iniciados onze containers: broker, proxy, referência, dois bots e dois servidores de cada
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
mensagens enviadas e recebidas, com timestamps e contadores lógicos. Se um
pedido ficar sem resposta por cinco segundos, o cliente encerra a espera. Os
bots continuam com uma nova requisição.

O serviço de referência atende na porta 5559 e mantém os ranks e a lista de
servidores ativos. Os servidores enviam heartbeat a cada dez pedidos recebidos.
O prazo de inatividade padrão é de 60 segundos, configurável por
`HEARTBEAT_TIMEOUT_SECONDS` no serviço `referencia`.

Os servidores se comunicam pela porta interna 5560. O coordenador é anunciado
no tópico `servers` e fornece a hora calculada por Berkeley. A sincronização
ocorre a cada 15 mensagens trocadas com clientes. Os logs mostram os
contadores lógicos, as eleições e os ajustes do relógio.

Para executar em segundo plano:

```sh
docker compose up -d --build
```

Para acompanhar os serviços e suas mensagens:

```sh
docker compose ps
docker compose logs -f
```

Para consultar o histórico de um canal, com o Compose em execução:

```sh
docker compose exec client-python-1 python cliente.py consulta historico canal-exemplo
docker compose exec client-csharp-1 dotnet Chat.dll cliente consulta historico canal-exemplo
```

Substitua `canal-exemplo` pelo nome do canal ou omita o nome para consultar todos.

Para encerrar, use Ctrl+C se estiver acompanhando a execução e execute:

```sh
docker compose down
```

Cada servidor armazena seu banco em `/data/parte1.db`, dentro de um volume
exclusivo. Os dados são preservados entre execuções. Para encerrar e apagar os
bancos, utilize `docker compose down -v`.

## Replicação

O broker distribui os pedidos entre os servidores. Para que cada um possa
consultar o histórico completo, usamos **replicação ativa com consistência
eventual**. Cada servidor aplica as mesmas operações de login, criação de canal
e publicação no seu próprio SQLite. Dados de bancos existentes entram nesse
processo na primeira inicialização.

O servidor que recebe uma escrita grava a operação e os dados na mesma
transação e usa `replicar` para enviar a operação a outro servidor. Ele só
confirma sucesso ao cliente depois que esse servidor grava uma cópia,
protegendo a escrita confirmada da perda de um servidor. Periodicamente,
as réplicas usam `buscar_operacoes` para recuperar lotes de operações faltantes.
O identificador de cada operação evita reaplicações. Se dois servidores criarem
o mesmo canal ao mesmo tempo, as réplicas usam o registro com o menor
identificador em ordem lexicográfica.

Um servidor que entra com banco vazio busca os dados desde o início. A
recuperação atualiza o histórico sem reenviar publicações aos assinantes.
Durante a recuperação, uma consulta pode retornar dados parciais. Se uma escrita
ficar sem confirmação da segunda cópia, ela retorna erro e pode se propagar
depois caso tenha sido gravada localmente.
