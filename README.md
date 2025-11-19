<div align="center">

# 💬 **Sistema Distribuído de Troca de Mensagens**
### **ZeroMQ • MessagePack • Lamport Clock • Eleição Bully • Berkeley Sync • Docker*
<br><br>

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

/app/data/messages.json


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

## ⏱ **Relógio Lógico (Lamport)**

Cada mensagem carrega: "clock": <contador><br> 
Antes de enviar → clock++<br>
Ao receber → clock = max(local, recebido) + 1
Garante ordenação causal em replicações e mensagens distribuídas.

--- 
<div>

## 👑 Eleição (Bully) + Sincronização Berkeley
```bash
- O maior rank vence a eleição.
- Coordenador divulga no tópico servers
- A cada 10 mensagens → sincronização de relógio físico</div>
- docker stop server_c
- Veja outro servidor ser eleito coordenador.<br>
---

<div>
  <h1>▶️ Como Executar</h1>
  <h3>ZeroMQ • MessagePack • Docker • Go</h3>
</div>
<h2> (H2)</h2>

```bash
docker-compose build
docker-compose up


## 🖥 Acessar Cliente

docker exec -it client bash ou
docker compose run --rm client
node client.js
---
```bash
<div>
## 💻 Comandos do Cliente
```bash

| Comando                 | Função              |
|-------------------------|---------------------|
| `login <nome>`          | Faz login           |
| `users`                 | Lista usuários      |
| `channels`              | Lista canais        |
| `channel <nome>`        | Cria canal          |
| `subscribe <topico>`    | Inscreve no canal   |
| `publish <canal> <msg>` | Publica mensagem    |
| `message <user> <msg>`  | Envia mensagem privada |

</div>



<h2>Texto Médio (H2)</h2>
## 🔍 Ver Logs dos Servidores


```bash
docker logs -f server_a
docker logs -f server_b
docker logs -f server_c

## 🤖 Bots<br>
```bash
Bots começam a enviar mensagens automaticamente.
---




</div>
## 👤 Autor<br>
</div>
Projeto desenvolvido para a disciplina
BCSL502 — Sistemas Distribuídos (VTU 2022 Scheme)
Entregue como solução completa das Partes 1 a 5.<br>
<br>

<div align="center">
⭐ Se este repositório te ajudou, considere deixar uma estrela!
</div> ```











<h1 align="center">💬 Sistema Distribuído de Mensagens</h1>
<h3 align="center">ZeroMQ • MessagePack • Docker • Go</h3>







