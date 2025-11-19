
<div align="center">

# 💬 **Sistema Distribuído de Troca de Mensagens**
### **ZeroMQ • MessagePack • Lamport Clock • Eleição Bully • Berkeley Sync • Docker*


📡 Mensagens privadas — 📨 Canais públicos — 🤖 Bots automáticos — 🔁 Replicação — ⏱ Sincronização  
**Projeto completo para a disciplina BCSL502 – Sistemas Distribuídos**

---

</div>

## 🌐 **Visão Geral**

Este projeto implementa um sistema distribuído robusto inspirado em IRC/BBS, permitindo:

- Comunicação em tempo real  
- Replicação ativa entre servidores  
- Balanceamento via broker  
- Sincronização de relógios  
- Persistência em disco  
- Tolerância a falhas com eleição automática  

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

## 🗄 **Persistência**

Cada servidor salva seus dados em:

<img width="226" height="225" alt="image" src="https://github.com/user-attachments/assets/b9e066cd-9688-4d51-a1d3-2b6010b350af" />

          

Com:

- Mensagens de canais  
- Mensagens privadas  
- Timestamps  
- Valor do clock lógico  
- Identificação do usuário  

---

## 🔁 Método de Replicação entre Servidores
**Método Escolhido: Replicação via Difusão (Broadcast) usando PUB/SUB**<br>
Para distribuir as mensagens entre todos os servidores, o sistema utiliza um Proxy PUB/SUB do ZeroMQ (XSUB/XPUB).<br>
A estratégia adotada é um modelo de replicação ativa, no qual cada servidor recebe e aplica todas as mensagens, mantendo uma cópia completa do estado.<br>

**Fluxo:**

Um cliente ou bot envia uma mensagem para qualquer servidor usando REQ/REP.<br>
O servidor que recebeu a requisição publica a mensagem no canal correspondente através do socket PUB conectado ao proxy.<br>
O Proxy PUB/SUB distribui essa mensagem para todos os servidores conectados via SUB.<br>
Cada servidor recebe a mesma mensagem, atualiza seu relógio lógico e salva localmente em:<br>

- **data/channels.json**<br>
- **data/messages.json**<br>
- **data/users.json**<br>

Mesmo que um servidor caia e volte, ele possui sua cópia em disco e continuará recebendo as próximas mensagens normalmente.<br>

**Garantia de Ordem (Relógio Lógico de Lamport)**<br>

Como o ZeroMQ não garante ordenação, o sistema utiliza um relógio lógico para ordenar eventos:<br>
Cada mensagem carrega o campo clock.<br>
Servidores atualizam seu clock com base no clock recebido.<br>
A persistência utiliza este clock para garantir ordem causal.<br>
Isso evita problemas de reordenamento entre réplicas.<br>

**Consistência Obtida**<br>

O sistema implementa:<br>
✔ Consistência Eventual<br>
  Todos os servidores recebem todas as publicações e convergem para o mesmo estado.<br>
✔ Replicação Ativa<br>
  Todos aplicam a mesma operação — não há servidor “principal” responsável pelo estado.<br>
✔ Persistência Local<br>
  Cada servidor salva suas mensagens em disco, garantindo sobrevivência a falhas.<br>
  
**Vantagens do Método**

- **Alto desempenho:** ZMQ PUB/SUB é extremamente rápido e leve.
- **Total descentralização:** qualquer servidor pode publicar.
- **Tolerância a falhas:** o coordenador pode cair sem perder mensagens.
- **Implementação simples:** não depende de bancos distribuídos.

**Fluxo resumido:**

1. Cliente → Servidor via REQ/REP  
2. Servidor publica no Proxy (XSUB)  
3. Proxy faz fan-out para todos servidores SUB  
4. Todos atualizam relógio + persistem localmente  

>**Garantias:**
- Consistência eventual  
- Estado idêntico entre servidores  
- Total independência do coordenador

**Conclusão**
O projeto adota replicação ativa via difusão usando PUB/SUB do ZeroMQ, esse método mantém todos os servidores sincronizados.

---

## 👑 Eleição (Bully) + Sincronização Berkeley
- O maior rank vence a eleição.  
- Coordenador divulga no tópico `servers`  
- A cada 10 mensagens → sincronização de relógio físico  
- `docker stop server_c`  
- Veja outro servidor ser eleito coordenador.


## 🚀 Como Executar

//Construir o ambiente<br>
docker-compose build

//Subir os contêineres<br>
docker-compose up



## 🖥 Acessar Cliente

docker exec -it client bash ou<br>
docker compose run --rm client<br>
node client.js<br>
---

## 💻 Comandos do Cliente

| Comando                 | Função                                |
|-------------------------|----------------------------------------|
| `login <nome>`          | Faz login                              |
| `users`                 | Lista usuários                         |
| `channels`              | Lista canais                           |
| `channel <nome>`        | Cria um novo canal                     |
| `subscribe <topico>`    | Inscreve no canal                      |
| `publish <canal> <msg>` | Publica uma mensagem em um canal       |
| `message <user> <msg>`  | Envia uma mensagem privada a um usuário |

---

## 🔍 Ver Logs dos Servidores

```bash
# Construir o ambiente
docker-compose build

# Subir os contêineres
docker-compose up

## 🔍 Ver Logs dos Servidores

-Construir o ambiente
**docker-compose build**
- Subir os contêineres
**docker-compose up**

## 🤖 Bots<br>
**Bots automáticos:** :
- Criam um usuário aleatório
- Escolhem um canal
- Enviam mensagens aleatórias
- Recebem mensagens em segundo plano

## 🧩 **6. Servidor de Referência (Go)**
- Armazena:
  - nomes dos servidores
  - Endereços
  - ranks
- Entrega rank ao servidor
- Monitora heartbeat
- Expira servidores inativos
- Fornece lista de ranks 
- Elege o coordenador
  
## ⏱ Relógio Lógico (Lamport)

"clock": <contador lógico>

**Regras:**
- Antes de enviar → `clock++`  
- Ao receber → `clock = max(local, recebido) + 1`

**Garantias:**
✔ Ordenação causal  
✔ Replicações consistentes  
✔ Logs persistidos na mesma ordem em todos os servidores
---

## 🕒 Sincronização do Relógio Físico (Algoritmo de Berkeley)

- O coordenador consulta outros servidores  
- Calcula média dos desvios  
- Envia ajustes  
- Sincroniza a cada 10 mensagens  
- Se coordenador falhar → eleição ocorre.
---

## 👤 Autor: Deise Adriana Silva Araújo

Projeto desenvolvido para a disciplina  
CC7261 — Sistemas Distribuídos  
Entregue como solução completa das Partes 1 a 5.

---

## 🤝 Contribuição

Contribuições são bem-vindas!  









