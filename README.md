# Projeto - Sistema Distribuído de Troca de Mensagens

Projeto desenvolvido para a disciplina **CC7261 – Sistemas Distribuídos**, com o objetivo de implementar um sistema simplificado de troca de mensagens inspirado em BBS (Bulletin Board System) e IRC (Internet Relay Chat).

O sistema permite que bots realizem login, criem canais públicos, publiquem mensagens e recebam publicações dos canais nos quais estão inscritos. Além disso, os servidores armazenam as informações em disco e replicam os dados entre si, permitindo que todos mantenham uma cópia das mensagens trocadas.

## Integrantes

|       Integrante         |       RA       |             
| ------------------------ | -------------- |
| **Aline Rocha de Jesus** |  22.123.106-1  |
| **Bianca Silva Oliveira**|  22.123.113-7  |


## 1. Tecnologias utilizadas

### 1.1. Python e JavaScript

O projeto utiliza Python e JavaScript (Node.js) para desenvolver os clientes e servidores. A utilização de duas linguagens atende ao requisito da disciplina e permite demonstrar a comunicação entre aplicações desenvolvidas em ambientes diferentes.

**Python foi escolhido pelos seguintes motivos:**

- Possui uma sintaxe simples, facilitando o desenvolvimento e a manutenção do código.
- Possui bibliotecas que atendem às necessidades do projeto, como PyZMQ para comunicação e Protocol Buffers para serialização.
- Facilita a implementação de processos concorrentes, como o recebimento de mensagens e o serviço de sincronização dos relógios.

**JavaScript foi escolhido pelos seguintes motivos:**

- O Node.js permite executar JavaScript fora do navegador, sendo adequado para desenvolver aplicações que se comunicam pela rede.
- Possui suporte a operações assíncronas, facilitando o tratamento de várias comunicações sem bloquear a execução do servidor.
- Possui bibliotecas equivalentes às utilizadas em Python, permitindo implementar os mesmos protocolos e funcionalidades.

A utilização das duas linguagens também permite verificar se as mensagens são corretamente interpretadas independentemente da linguagem utilizada por quem as enviou.

### 1.2. ZeroMQ

O ZeroMQ foi utilizado para realizar a comunicação entre os bots, servidores e demais serviços do sistema.

Sua escolha se deve principalmente à disponibilidade de diferentes padrões de comunicação, que atendem às necessidades do projeto:

- Permite utilizar Request-Reply para operações que precisam de uma resposta, como login, criação de canais e publicação de mensagens.
- Permite utilizar Publisher-Subscriber para distribuir mensagens aos bots inscritos em determinados canais.
- Possui bibliotecas para Python e JavaScript, facilitando a comunicação entre as duas implementações.

Dessa forma, o ZeroMQ permite organizar a comunicação entre os processos sem a necessidade de implementar manualmente todo o gerenciamento das conexões.

### 1.3. Protocol Buffers

O Protocol Buffers foi escolhido como formato de serialização das mensagens trocadas entre os componentes.

Essa escolha é adequada ao projeto pelos seguintes motivos:

- Atende ao requisito da disciplina de utilizar um formato de serialização binário.
- Permite definir uma estrutura padrão para as mensagens, garantindo que clientes e servidores utilizem o mesmo formato.
- Possui suporte para Python e JavaScript, permitindo a comunicação entre componentes desenvolvidos nas duas linguagens.
- Sua representação binária torna as mensagens mais compactas do que uma representação textual, como JSON.

A estrutura das mensagens é definida no arquivo `mensagem.proto`, contendo os seguintes campos:

| Campo | Descrição |
|---|---|
| `tipo` | Identifica o tipo da mensagem. |
| `timestamp` | Armazena o horário de envio. |
| `origem` | Identifica quem enviou a mensagem. |
| `dados` | Contém as informações da operação. |
| `canais` | Armazena uma lista de canais ou informações relacionadas. |
| `contador` | Armazena o relógio lógico de Lamport. |

### 1.4. JSON para persistência

O JSON foi escolhido para armazenar os dados dos servidores em disco.

Essa escolha é adequada ao projeto pelos seguintes motivos:

- **Estrutura dos dados:** logins, canais e publicações são informações simples, que podem ser organizadas facilmente em listas e objetos JSON.
- **Facilidade de leitura:** por ser um formato textual, permite consultar diretamente os arquivos e verificar quais informações foram armazenadas durante a execução e os testes.
- **Simplicidade:** não exige a instalação ou configuração de um banco de dados adicional, mantendo a estrutura do projeto mais simples.
- **Compatibilidade:** Python e JavaScript possuem suporte nativo à leitura e escrita de JSON, facilitando a utilização do mesmo formato nas duas implementações.

Cada servidor possui seu próprio arquivo `dados.json`, armazenado em um volume Docker. Dessa forma, os dados continuam disponíveis mesmo após a reinicialização do container.

## 2. Arquitetura do sistema

O sistema é composto por bots, servidores, brokers, um proxy de comunicação e serviços de referência.

- **Bots:** realizam login, consultam e criam canais, publicam mensagens e recebem publicações dos canais inscritos.
- **Servidores:** recebem as solicitações dos bots, executam as operações, armazenam os dados e participam da replicação.
- **Brokers:** encaminham as requisições dos bots para os servidores, distribuindo as solicitações entre as instâncias disponíveis.
- **Proxy PUB/SUB:** encaminha as publicações dos canais e as mensagens internas utilizadas na replicação e na eleição do coordenador.
- **Serviços de referência:** mantêm a lista de servidores disponíveis, atribuem ranks e auxiliam no processo de eleição.

Cada implementação possui seu próprio broker e serviço de referência. Entretanto, os servidores Python e JavaScript utilizam o mesmo proxy PUB/SUB, permitindo a troca de mensagens entre as duas implementações.

### 2.1. Docker e Docker Compose

O Docker foi utilizado para executar cada componente em um container separado, permitindo que bots e servidores funcionem simultaneamente e se comuniquem pela mesma rede.

O Docker Compose foi escolhido para facilitar a configuração e inicialização de todos os serviços. Com ele, é possível definir os containers, suas conexões e os volumes utilizados para armazenar os dados em um único arquivo.

Essa organização facilita a execução dos testes e garante que o ambiente de execução seja reproduzível.

## 3. Comunicação entre os componentes

O sistema utiliza dois principais padrões de comunicação do ZeroMQ: Request-Reply e Publisher-Subscriber.

### 3.1. Request-Reply (REQ-REP)

Esse padrão foi escolhido para operações que precisam de uma resposta direta do servidor.

Ele é utilizado principalmente para:

- Realizar o login dos bots.
- Criar e listar canais.
- Solicitar a publicação de mensagens.
- Consultar informações dos serviços de referência.
- Realizar a sincronização dos relógios.

A escolha desse padrão permite que o bot saiba se sua solicitação foi processada corretamente, recebendo uma resposta de sucesso ou erro.

### 3.2. Publisher-Subscriber (PUB-SUB)

Esse padrão foi escolhido para distribuir mensagens a vários destinatários sem que o servidor precise enviar uma requisição individual para cada um.

No projeto, os canais são representados por tópicos. Quando um bot publica uma mensagem, o servidor publica o conteúdo no tópico correspondente, permitindo que todos os bots inscritos naquele canal recebam a publicação.

Além disso, esse padrão também é utilizado para a comunicação interna entre os servidores, principalmente na replicação dos dados e na divulgação do coordenador eleito.

### 3.3. Broker

O broker é responsável por encaminhar as requisições recebidas dos bots para os servidores disponíveis.

Sua utilização permite distribuir as solicitações entre diferentes servidores, evitando que todas as requisições sejam direcionadas para uma única instância.

O balanceamento é realizado utilizando os sockets ROUTER e DEALER do ZeroMQ, que permitem intermediar a comunicação entre vários clientes e servidores.

## 4. Persistência dos dados

Cada servidor armazena seus dados localmente no arquivo `dados.json`. As informações são organizadas em três grupos principais:

- **Logins:** armazenam os nomes dos usuários e os horários em que realizaram login.
- **Canais:** armazenam os canais criados no sistema.
- **Publicações:** armazenam o canal, o conteúdo da mensagem, sua origem e o timestamp.

Quando o servidor inicia, ele carrega os dados existentes no arquivo. Sempre que ocorre uma alteração, o arquivo é atualizado para manter as informações disponíveis nas próximas execuções.

A escolha de manter um arquivo separado para cada servidor permite que cada instância tenha seu próprio armazenamento, sem depender de um arquivo compartilhado. Isso também é importante para demonstrar a replicação dos dados entre servidores.

## 5. Relógios e eleição de coordenador

O projeto utiliza relógios lógicos de Lamport para acompanhar a ordem dos eventos entre os processos.

Cada mensagem enviada contém um contador lógico, que é incrementado antes do envio. Ao receber uma mensagem, o processo atualiza seu contador utilizando o maior valor entre o contador local e o recebido.

Também é realizada a sincronização dos relógios físicos dos servidores utilizando o algoritmo de Berkeley.

Para isso, os servidores enviam mensagens de heartbeat ao serviço de referência, que mantém uma lista dos servidores disponíveis. Cada servidor possui um rank, utilizado durante a eleição para determinar qual será o coordenador.

O servidor com maior rank entre os disponíveis é escolhido como coordenador e passa a fornecer a referência de horário para os demais servidores.

Esses mecanismos permitem acompanhar os eventos do sistema, identificar servidores indisponíveis e manter os relógios físicos dos servidores sincronizados.

## 6. Consistência e replicação dos dados

### 6.1. Problema identificado

Inicialmente, devido ao balanceamento de requisições realizado pelo broker, cada servidor recebe apenas uma parte das mensagens publicadas pelos bots.

Isso gera dois problemas principais:

- Caso um servidor fique indisponível, parte do histórico de mensagens pode deixar de estar acessível.
- Quando um bot solicita o histórico a um servidor, pode receber apenas as mensagens armazenadas naquela instância.

Para resolver esse problema, foi necessário implementar a replicação dos dados, fazendo com que todos os servidores mantenham uma cópia das informações.

### 6.2. Método escolhido

Para resolver esse problema, foi utilizada a abordagem de **réplica passiva**, com propagação das atualizações pelo método *push*.

Essa escolha é adequada ao projeto pelos seguintes motivos:

- **Cópia dos dados:** permite que os servidores mantenham as mesmas informações, mesmo recebendo requisições diferentes dos bots.
- **Atualização automática:** quando um servidor recebe uma nova informação, ele envia essa atualização aos demais, sem que eles precisem solicitá-la.
- **Utilização do PUB/SUB:** aproveita o mecanismo de comunicação já implementado no projeto para distribuir as atualizações entre vários servidores.
- **Independência dos servidores:** cada servidor continua armazenando seus próprios dados, sem depender de um armazenamento centralizado.

### 6.3. Funcionamento da replicação

A replicação funciona da seguinte maneira:

1. Um servidor recebe uma solicitação de login, criação de canal ou publicação de mensagem.
2. O servidor realiza a operação e atualiza seu armazenamento local.
3. Em seguida, publica uma mensagem no tópico `replicacao`, contendo a informação que foi alterada.
4. Os demais servidores, inscritos nesse tópico, recebem a mensagem.
5. Cada servidor aplica a atualização recebida em seu próprio armazenamento.

São replicadas três operações:

- **Login:** replica o usuário e o timestamp do login.
- **Criação de canal:** replica o nome do canal criado.
- **Publicação:** replica o canal, o conteúdo da mensagem, sua origem e o timestamp.

Dessa forma, as atualizações são distribuídas entre os servidores, permitindo que todos mantenham uma cópia dos dados.

### 6.4. Adaptação do método

Para implementar a replicação, foi utilizado o tópico `replicacao` no mecanismo PUB/SUB.

As mensagens de replicação utilizam o mesmo formato Protobuf das demais mensagens do sistema, sendo identificadas pelo tipo `replicar`. Dentro do campo `dados`, é informada a operação que deve ser aplicada e suas respectivas informações.

Também é utilizado o campo `origem` para identificar o servidor responsável pela publicação. Assim, quando um servidor recebe uma mensagem originada por ele mesmo, a atualização é ignorada, evitando que ele processe novamente sua própria publicação.

Essa adaptação permite utilizar o mesmo mecanismo de comunicação já existente para realizar a replicação, sem a necessidade de criar um novo serviço exclusivamente para essa finalidade.

### 6.5. Consistência e limitações

O método utilizado busca manter os dados dos servidores sincronizados por meio da propagação das atualizações.

Essa abordagem se aproxima da consistência eventual, pois os servidores podem receber as atualizações em momentos diferentes, mas tendem a possuir os mesmos dados após a propagação de todas as mensagens.

Entretanto, a implementação possui algumas limitações:

- O PUB/SUB não armazena mensagens para servidores que estejam desconectados. Portanto, atualizações enviadas durante uma indisponibilidade podem ser perdidas.
- Não existe um mecanismo de recuperação automática das mensagens que uma réplica deixou de receber.
- Não existe um mecanismo completo para identificar e remover publicações duplicadas.
- Não foi implementado um mecanismo específico para resolver conflitos entre atualizações concorrentes.

Assim, a replicação atende ao objetivo de distribuir as atualizações entre os servidores durante o funcionamento normal do sistema, mas não garante que uma réplica que ficou indisponível consiga recuperar automaticamente todas as informações perdidas.

## 7. Execução do projeto

Para executar o projeto, é necessário ter Docker e Docker Compose instalados.

Na raiz do repositório, execute:

```bash
docker compose up --build
```

Esse comando constrói as imagens e inicia automaticamente os bots, servidores, brokers, proxy e serviços de referência.

Para encerrar a execução:

```bash
docker compose down
```

Para remover também os volumes que armazenam os dados:

```bash
docker compose down -v
```

**Observação:** a utilização do parâmetro `-v` remove os dados persistidos pelos servidores.
