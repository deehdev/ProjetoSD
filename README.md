<div align="center">

# 💬 **Sistema Distribuído de Troca de Mensagens**

### **ZeroMQ • MessagePack • Lamport Clock • Eleição Bully • Berkeley Sync • Docker*

📡 Mensagens privadas<br>
📨 Canais públicos<br>
🤖 Bots automáticos<br>
🔁 Replicação<br>
⏱ Sincronização  

**Projeto completo para a disciplina CC7261 – Sistemas Distribuídos**

---

</div>

## 🌐 **Visão Geral**

Este documento apresenta o projeto de um sistema distribuído simplificado para troca de mensagens, inspirado em plataformas clássicas como BBS (Bulletin Board System) e IRC (Internet Relay Chat).
O sistema foi desenvolvido como parte da disciplina de Sistemas Distribuídos, com foco nos principais desafios de comunicação distribuída, coordenação, consistência e tolerância a falhas.

O projeto implementa:

Comunicação em tempo real
Interação entre múltiplos clientes via canais públicos e mensagens privadas, utilizando ZeroMQ com PUB/SUB e REQ/REP.

Replicação ativa entre servidores
Cada alteração de estado (usuário, canal, mensagem) é replicada para todos os servidores, garantindo consistência eventual e evitando perda de dados.

Balanceamento de carga via broker
Um broker intermediário distribui as requisições dos clientes entre os servidores utilizando round-robin, aumentando escalabilidade e disponibilidade.

Sincronização de relógios
Implementação de relógios lógicos e sincronização periódica usando o algoritmo de Berkeley para alinhamento temporal.

Persistência em disco
Todo o estado relevanteu: suários, canais, mensagens e metadados—é armazenado localmente em arquivos JSON para permitir recuperação após reinicialização.

Tolerância a falhas com eleição automática
Servidores monitoram uns aos outros via heartbeat e realizam eleição automática (sem líder fixo) para determinar o coordenador responsável pelo clock centralizado.

A arquitetura é composta por **9 containers**, todos conectados através do Docker Compose:

- 🖥 3 servidores distribuídos  
- 📡 1 proxy PUB/SUB  
- 🔄 1 broker REQ/REP  
- 📍 Servidor de referência  
- 🤖 2 bots automáticos  
- 👤 1 cliente interativo  

---

## 🧱 **Estrutura Completa**
<img width="696" height="487" alt="image" src="https://github.com/user-attachments/assets/daa6aa69-1029-41f3-9500-d714b6a7e3a6" />


---
</div>

## ⚙️ **Tecnologias Utilizadas**

| Tecnologia | Uso |
|-----------|-----|
| **Go** | Servidores + REF Server |
| **Node.js** | Cliente interativo |
| **Python** | Bots automáticos |
| **ZeroMQ** | REQ/REP e PUB/SUB distribuído |
| **MessagePack** | Serialização binária compacta |
| **Lamport Clock** | Ordenação causal |
| **Algoritmo Bully** | Eleição do coordenador |
| **Berkeley** | Sincronização de relógio |
| **Docker Compose** | Orquestração dos 9 containers |

---
## ⚙️ Funcionalidades Implementadas
### — Request/Reply

Implementado com **ZeroMQ REQ/REP**:

- Login  
- Listagem de usuários  
- Criação de canais  
- Listagem de canais  
- Persistência dos dados em disco  

---

### — PUB/SUB

Implementado com **Proxy (XSUB/XPUB)**:

- Publicações em canais  
- Mensagens privadas  
- **Bot automático (Python)** que:
  - loga com nome aleatório  
  - envia mensagens para canais aleatórios  

---

### — MessagePack

- Todas as mensagens **clientes ↔ servidores** agora são **binárias (msgpack)**.

---

### — Relógios Lógicos

Todos os processos (clientes, bots e servidores) utilizam um relógio lógico para organizar a ordem das mensagens:
Um contador inicia junto com o processo.
Antes do envio de cada mensagem, o contador é incrementado.
O contador é enviado junto com a mensagem.
Ao receber uma mensagem, o processo compara o seu contador com o valor recebido e atualiza seu contador para o máximo entre os dois.
Dessa forma, todas as mensagens possuem:
 - Timestamp
Valor do relógio lógico do remetente
Isso garante consistência parcial na ordenação de eventos distribuídos.

### Implementação do relógio lógico em:

- **Servidores (Go)**
- **Clientes (Node)**
- **Bots (Python)**
- **Servidor de referência (Go)**

### Regras implementadas:

- Incremento **antes de enviar**  
- `max(local, recebido) + 1` **ao receber**

---

### — Servidor de Referência (rank + heartbeat)

**Algoritmo de Berkeley**
O sistema utiliza um servidor mestre (coordenador) como referência de tempo para sincronizar todos os servidores. O processo segue os seguintes passos:
O mestre consulta periodicamente todos os servidores sobre seus relógios locais.
Cada servidor responde com o seu horário atual.
O mestre calcula a média dos relógios (ou aplica outro critério de compensação).
O mestre envia para cada servidor a diferença (offset) que deve ser aplicada ao seu relógio local.
Cada servidor ajusta seu relógio conforme o offset recebido.
Objetivo: manter todos os relógios dos servidores aproximadamente sincronizados, garantindo que a ordem das operações siga o tempo lógico, sem depender de um relógio físico global.

O processo **reference (Go)** implementa:

### Serviços:

| Serviço    | Função                                  |
|------------|-------------------------------------------|
| `rank`     | servidor envia seu rank e endereço         |
| `list`     | retorna lista de servidores ativos         |
| `heartbeat`| servidores avisam que estão vivos          |

### Métodos implementados:

- Registro de novos servidores  
- Atualização automática de `last_seen`  
- Remoção de servidores inativos  
- Armazenamento de `addr + rank`  

### Trecho real do código (conforme solicitado):
<img width="438" height="140" alt="image" src="https://github.com/user-attachments/assets/5e110551-4838-45e2-99c3-864887dfeb0a" />

## 🗄 Persistência

Cada servidor mantém seus dados salvos em disco, garantindo que informações importantes não sejam perdidas.

<img width="247" height="225" alt="image" src="https://github.com/user-attachments/assets/21e0287a-c7c4-4a68-be04-464a279a9b7b" />

### Dados armazenados:

- Mensagens de canais  
- Mensagens privadas  
- Timestamps  
- Valor do clock lógico  
- Identificação do usuário  

---

## Consistência e Replicação

### Problema

O broker utiliza **round-robin** para balancear a carga entre os servidores. Consequentemente:

- Cada servidor armazena apenas parte das mensagens trocadas.  
- Se um servidor falhar, parte do histórico é perdida.  
- Um cliente consultando o histórico em um servidor recebe apenas os dados armazenados localmente.  

**Solução:** todos os servidores devem possuir **uma cópia completa de todos os dados**.

---

### Método de Replicação

- **Replicação assíncrona via PUB/SUB** usando ZeroMQ.  
- Cada servidor possui:
  - **PUB socket**: publica alterações nos dados (usuários, canais, mensagens).  
  - **SUB socket**: escuta alterações publicadas pelos outros servidores.  
- Ao alterar dados localmente, o servidor:
  1. Atualiza o estado local.  
  2. Persiste a informação no disco.  
  3. Publica a alteração no tópico `replicate` com:
     - **Ação**: `add_user`, `add_channel`, `publish`  
     - **Payload**: dados relevantes  
     - **Timestamp** e **relógio lógico (clock)**  

- Os servidores ouvintes aplicam a alteração e persistem localmente, garantindo que todos tenham **cópia completa e atualizada**.

---

### Consistência

- A replicação é **assíncrona**, não bloqueia operações.  
- Cada alteração inclui um **relógio lógico**, garantindo a ordem parcial dos eventos.  
- O coordenador fornece sincronização de relógios via algoritmo de **Berkeley**, alinhando timestamps.  
- Garante **eventual consistency**: todos os servidores eventualmente possuem o mesmo estado.

---

### Troca de Mensagens entre Servidores

1. Um servidor recebe uma alteração local.  
2. Publica a alteração no tópico `replicate`.  
3. Todos os servidores inscritos recebem a mensagem, aplicam a alteração e persistem.  
4. Opcionalmente, o coordenador sincroniza clocks para manter consistência temporal.  

**Resultado:** cada servidor mantém o histórico completo de usuários, canais e mensagens, evitando perda de dados e permitindo que qualquer servidor responda a consultas de clientes com dados completos.

---

### Replicação Multidirecional

<img width="1418" height="523" alt="Replicação Multidirecional" src="https://github.com/user-attachments/assets/dfd5233b-b4a5-4509-bc06-9858bd46cdda" />

---

## 🚀 Como Executar

### **1. Clone o repositório**
- git clone https://github.com/deehdev/ProjetoSD
- cd SEU_REPO
### **2. Inicie tudo**
- docker-compose up --build
### **3. Abra clientes interativos**
- docker exec -it client /bin/sh
- node client.js
### **4. Comandos do cliente**

| Comando                 | Função                                |
|-------------------------|----------------------------------------|
| `login <nome>`          | Faz login                              |
| `users`                 | Lista usuários                         |
| `channels`              | Lista canais                           |
| `channel <nome>`        | Cria um novo canal                     |
| `subscribe <topico>`    | Inscreve no canal                      |
| `publish <canal> <msg>` | Publica uma mensagem em um canal       |
| `message <user> <msg>`  | Envia uma mensagem privada a um usuário |

## 📚 Exemplo de Execução

### **Cliente:**

 **Login**
 
<img width="634" height="173" alt="image" src="https://github.com/user-attachments/assets/0da1b852-455e-465f-b1b4-ac8a4e5ae34c" />

**users**

<img width="578" height="225" alt="image" src="https://github.com/user-attachments/assets/7306d17d-5b97-4040-83af-a4475b8159a9" />

**channel**

<img width="600" height="124" alt="image" src="https://github.com/user-attachments/assets/20750926-6808-4513-9596-9058f11f3c9a" />

**channels**

<img width="592" height="165" alt="image" src="https://github.com/user-attachments/assets/096de960-cb12-4f67-b6c6-c35cc80295a0" />

**message**

<img width="1324" height="333" alt="image" src="https://github.com/user-attachments/assets/4c578419-fdd7-49ce-9d2a-47975b5ce582" />

**subscribe**

<img width="560" height="364" alt="image" src="https://github.com/user-attachments/assets/0194224e-8709-4c93-8b09-e2a7870b02db" />


## 👑 Exemplo de Eleição 

<img width="692" height="360" alt="image" src="https://github.com/user-attachments/assets/e33b6228-7dc9-4a2d-95d3-8ebc31e04b13" />

 
<img width="698" height="348" alt="image" src="https://github.com/user-attachments/assets/770a3f40-3597-4895-abbc-b748619fdfd0" />

 
<img width="1231" height="351" alt="image" src="https://github.com/user-attachments/assets/76655699-540e-46ac-ad59-1b0b87914254" />

 
<img width="1324" height="333" alt="image" src="https://github.com/user-attachments/assets/a7a57ac8-bbdd-4aaf-b06f-4190fa888424" />

<img width="1181" height="160" alt="image" src="https://github.com/user-attachments/assets/55238dc1-1ea8-49be-adc9-594b024a5b83" />

## 🧪 Testes Incluídos

- Conexão múltipla de clientes
- Envio simultâneo de mensagens
- Falha de servidor + recuperação via replicação
- Mensagens auto-geradas dos bots

## 📄 Caminhos de Código

<img width="652" height="291" alt="image" src="https://github.com/user-attachments/assets/137194a6-02f6-47b6-960c-207f1a96f0ff" />

---

## 👤 Autor: Deise Adriana Silva Araújo
**Projeto de Sistemas Distribuídos**<br>
**Professor:** Leonardo Anjoleto
**Disciplina:**  CC7261 - Sistemas Distribuídos<br>
**Instituição:** FEI – Fundação Educacional Inaciana Padre Sabóia de Medeiros<br>

---

## 🤝 Contribuição

Este projeto demonstra de forma prática os conceitos de sistemas distribuídos:<br>
comunicação em tempo real, replicação de dados, sincronização de relógios, tolerância a falhas e coordenação entre servidores.<br>
Ele serve como base para estudo, experimentação e expansão de sistemas distribuídos confiáveis.<br>

**Contribuições são bem-vindas! Se quiser colaborar, melhorar ou expandir funcionalidades do projeto, fique à vontade para abrir issues ou pull requests."**











