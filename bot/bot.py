#!/usr/bin/env python3
import os 
import zmq
import msgpack
import random
import threading
import time
from datetime import datetime, timezone
import sys
sys.stdout.reconfigure(line_buffering=True)

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------
REQ_ADDR = os.environ.get("REQ_ADDR", "tcp://broker:5555")
SUB_ADDR = os.environ.get("SUB_ADDR", "tcp://proxy:5558")

# ---------------------------------------------------
# RELÓGIO LÓGICO
# ---------------------------------------------------
logical_clock = 0
clock_lock = threading.Lock()

def inc_clock():
    global logical_clock
    with clock_lock:
        logical_clock += 1
        return logical_clock

def update_clock(recv):
    global logical_clock
    with clock_lock:
        try:
            rc = int(recv)
            logical_clock = max(logical_clock, rc) + 1
        except:
            logical_clock += 1
        return logical_clock

def now_iso():
    return datetime.now(timezone.utc).isoformat()

# ---------------------------------------------------
# REQUEST (REQ → REP)
# ---------------------------------------------------
def send_req(sock, service, data=None, timeout=5.0):
    if data is None:
        data = {}

    env = {
        "service": service,
        "data": data,
        "timestamp": now_iso(),
        "clock": inc_clock()
    }

    encoded = msgpack.packb(env, use_bin_type=True)

    try:
        sock.send(encoded)
    except Exception as e:
        return {"service":"error","data":{"status":str(e)}}

    try:
        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)
        socks = dict(poller.poll(int(timeout*1000)))
        if socks.get(sock) == zmq.POLLIN:
            raw = sock.recv()
            reply = msgpack.unpackb(raw, raw=False)
            update_clock(reply.get("clock", 0))
            return reply
        else:
            return {"service":"error","data":{"status":"timeout"}}
    except Exception as e:
        return {"service":"error","data":{"status":str(e)}}

# ---------------------------------------------------
# SUB LISTENER
# ---------------------------------------------------
def sub_listener(sub):
    while True:
        try:
            parts = sub.recv_multipart()
            if len(parts) < 2:
                continue

            topic = parts[0].decode('utf-8', errors='ignore')
            env = msgpack.unpackb(parts[1], raw=False)
            update_clock(env.get("clock", 0))

            svc = env.get("service", "")
            data = env.get("data", {})
            ts = data.get("timestamp", "sem-timestamp")
            clk = data.get("clock", "?")

            if svc == "publish":
                user = data.get("user") or "?"
                msg = data.get("message") or ""
                print(f"[{topic}] {user}: {msg} (ts={ts}, clk={clk})")

            elif svc == "message":
                src = data.get("src") or "?"
                msg = data.get("message") or ""
                print(f"📩 PRIVADA de {src}: {msg} (ts={ts}, clk={clk})")

        except Exception as e:
            print("Erro no SUB:", e)
            time.sleep(0.5)

# ---------------------------------------------------
# BOT
# ---------------------------------------------------
def main():
    NOMES = [
        "Ana","Pedro","Rafael","Deise","Camila","Victor",
        "Paula","Juliana","Lucas","Marcos","Mateus","João",
        "Carla","Bruno","Renata","Sofia"
    ]

    FRASES = [
        "Alguém viu algum filme bom?",
        "Preciso de uma recomendação urgente.",
        "Esse mês saiu muito filme bom!",
        "Vocês preferem dublado ou legendado?",
        "Interstellar é perfeito.",
        "Quero algo leve!",
        "Alguém entendeu Tenet?",
        "Recomendações de terror psicológico?"
    ]

    username = random.choice(NOMES)
    print(f"BOT iniciado como {username}")

    ctx = zmq.Context()

    req = ctx.socket(zmq.REQ)
    req.setsockopt(zmq.LINGER, 0)
    req.connect(REQ_ADDR)
    time.sleep(0.1)

    sub = ctx.socket(zmq.SUB)
    sub.setsockopt(zmq.LINGER, 0)
    sub.connect(SUB_ADDR)
    time.sleep(0.1)

    # LOGIN
    r = send_req(req, "login", {"user": username})
    print("LOGIN:", r.get("data", {}).get("status"))

    # PRIVATE TOPIC
    sub.setsockopt_string(zmq.SUBSCRIBE, username)

    # LISTAR CANAIS
    r = send_req(req, "channels")
    canais = r.get("data", {}).get("channels", [])

    if not canais:
        send_req(req, "channel", {"channel": "geral"})
        canais = ["geral"]

    escolhido = random.choice(canais)
    sub.setsockopt_string(zmq.SUBSCRIBE, escolhido)

    print("Inscrito no canal:", escolhido)

    threading.Thread(target=sub_listener, args=(sub,), daemon=True).start()

    while True:
        if random.random() < 0.4:
            dest = random.choice([n for n in NOMES if n != username])
            txt = random.choice(FRASES)
            send_req(req, "message", {"src": username, "dst": dest, "message": txt})
            print(f"💌 {username} → {dest}: {txt}")
        else:
            txt = random.choice(FRASES)
            send_req(req, "publish", {"user": username, "channel": escolhido, "message": txt})
            print(f"[{escolhido}] {username}: {txt}")

        time.sleep(random.uniform(3, 6))


if __name__ == "__main__":
    main()
