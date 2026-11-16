# Replicação ativa e consistência eventual

O broker distribui as requisições entre servidores. Por isso, cada servidor
registra em SQLite as operações de login, criação de canal e publicação, quer
elas cheguem de um cliente, quer venham de outro servidor. As réplicas executam
a mesma operação localmente. Nenhum servidor monta o banco de outro.

Cada operação tem um identificador único e é gravada na mesma transação que
atualiza a tabela correspondente. Se a mesma operação chegar por mais de um
caminho, o identificador evita uma segunda aplicação. Uma publicação só é
enviada ao canal pelo servidor que processa o pedido do cliente; a recuperação
do histórico não dispara novas notificações.

## Confirmação e recuperação

1. O servidor grava a operação localmente e envia um pedido `replicar` pela
   porta interna 5560. Outro servidor confirma após gravar sua própria cópia.
2. Com as duas cópias gravadas, o primeiro servidor pode responder sucesso ao
   cliente. Assim, a perda de um servidor não elimina uma escrita confirmada.
3. Periodicamente, cada servidor pede `buscar_operacoes` aos pares cadastrados
   na referência, mesmo que algum esteja temporariamente sem heartbeat. Cada
   resposta traz até 100 operações novas. A sequência é local ao banco que
   fornece o lote; operações que chegaram de outros servidores também podem ser
   fornecidas. O lote e o novo cursor são gravados na mesma transação.
4. Se um servidor voltar com um banco vazio, ele começa a buscar do início.
   A identidade de cada banco acompanha o cursor para que os outros servidores
   também detectem quando precisam reiniciar a leitura de uma réplica recriada.

Na primeira inicialização, os registros existentes entram no log de replicação
na mesma transação que cria a identidade do banco. Essa importação é feita uma
vez por banco. A chave dos registros antigos usa o nome original do servidor,
a tabela e o `rowid`; os registros novos recebem um UUID.

## Ordem e conflitos

Se dois servidores criarem simultaneamente um canal com o mesmo nome, ambos
podem confirmar suas operações enquanto não conhecem o conflito. Em todas as
réplicas, o menor identificador em ordem lexicográfica define o registro do
canal. As publicações são preservadas, inclusive publicações distintas com
texto idêntico, e o histórico é ordenado por timestamp e identificador.

O protocolo combina envio direto da operação, para confirmar uma segunda
cópia, e recuperação periódica em lotes. Os relógios lógicos e físicos usados
pelo projeto continuam presentes nas mensagens. A escolha do registro de um
canal não depende do relógio físico nem da ordem de chegada.

## Limites

Uma leitura pode retornar dados incompletos enquanto a réplica se atualiza.
Uma escrita sem confirmação de outra réplica retorna erro. Se o servidor já a
gravou localmente, ela pode aparecer após a reconexão; repetir o pedido do
cliente não é necessariamente a mesma operação. O protocolo não impede
escritas concorrentes em partições que ainda tenham dois servidores cada.
A garantia de sobrevivência após confirmação cobre a perda de uma cópia;
enquanto uma escrita só estiver em duas réplicas, a perda de ambas pode
eliminá-la.
