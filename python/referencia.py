import os
import time
import threading
import zmq

import protocolo as proto

NOME = os.environ.get("NOME_REFERENCIA", "referencia-py")
PORTA = os.environ.get("PORTA_REFERENCIA", "5560")
TIMEOUT = 30  

servidores = {}      
ultimo_hb = {}       
proximo_rank = [0]
lock = threading.Lock()

def dar_rank(msg):
    nome = dict(msg.dados).get("nome", "")
    with lock:
        if nome not in servidores:
            servidores[nome] = proximo_rank[0]
            proximo_rank[0] += 1
        ultimo_hb[nome] = time.time()
        rank = servidores[nome]
    return proto.empacotar(proto.RANK_OK, NOME, {"nome": nome, "rank": rank})

def listar(msg):
    with lock:
        itens = [f"{n}:{r}" for n, r in servidores.items()]
    return proto.empacotar(proto.LISTA_SERVIDORES, NOME, canais=itens)

def heartbeat(msg):
    nome = dict(msg.dados).get("nome", "")
    with lock:
        if nome in servidores:
            ultimo_hb[nome] = time.time()
    return proto.empacotar(proto.HEARTBEAT_OK, NOME, {"ok": "1"})

def limpar():
    while True:
        time.sleep(5)
        agora = time.time()
        with lock:
            mortos = [n for n, t in ultimo_hb.items() if agora - t > TIMEOUT]
            for n in mortos:
                servidores.pop(n, None)
                ultimo_hb.pop(n, None)
                print(f"{NOME}: servidor {n} removido (sem heartbeat)", flush=True)

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind(f"tcp://*:{PORTA}")
print(f"{NOME} rodando na porta {PORTA}", flush=True)

threading.Thread(target=limpar, daemon=True).start()

while True:
    msg = proto.desempacotar(socket.recv())
    if msg.tipo == proto.RANK:
        resp = dar_rank(msg)
    elif msg.tipo == proto.LISTAR_SERVIDORES:
        resp = listar(msg)
    elif msg.tipo == proto.HEARTBEAT:
        resp = heartbeat(msg)
    else:
        resp = proto.empacotar(proto.RANK_OK, NOME, {"motivo": "tipo invalido"})
    print(f"{NOME} respondeu: {proto.formatar(proto.desempacotar(resp))}", flush=True)
    socket.send(resp)