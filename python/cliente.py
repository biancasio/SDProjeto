import os
import time
import zmq

import protocolo as proto

NOME = os.environ.get("NOME_BOT", "bot-py")
CANAL_NOVO = os.environ.get("CANAL_NOVO", "geral")
BROKER_ADDR = os.environ.get("BROKER_ADDR", "tcp://broker-py:5555")


def enviar(socket, raw):
    print(f"{NOME} enviou: {proto.formatar(proto.desempacotar(raw))}", flush=True)
    socket.send(raw)
    resp = proto.desempacotar(socket.recv())
    print(f"{NOME} recebeu: {proto.formatar(resp)}", flush=True)
    return resp


def fazer_login(socket):
    while True:
        resp = enviar(socket, proto.empacotar(proto.LOGIN, NOME, {"usuario": NOME}))
        if resp.tipo == proto.LOGIN_OK:
            return
        print(f"{NOME} login falhou, tentando de novo", flush=True)
        time.sleep(1)


context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect(BROKER_ADDR)

time.sleep(2)
print(f"{NOME} iniciou", flush=True)

fazer_login(socket)
enviar(socket, proto.empacotar(proto.LISTAR_CANAIS, NOME))
enviar(socket, proto.empacotar(proto.CRIAR_CANAL, NOME, {"canal": CANAL_NOVO}))
enviar(socket, proto.empacotar(proto.LISTAR_CANAIS, NOME))

print(f"{NOME} terminou", flush=True)

while True:
    time.sleep(3600)
