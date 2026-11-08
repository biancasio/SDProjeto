import os
import time
import random
import threading
import datetime
import zmq

import protocolo as proto

NOME = os.environ.get("NOME_BOT", "bot-py")
BROKER_ADDR = os.environ.get("BROKER_ADDR", "tcp://broker-py:5555")
PROXY_ADDR = os.environ.get("PROXY_ADDR", "tcp://proxy-py:5558")

context = zmq.Context()


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


def listar_canais(socket):
    resp = enviar(socket, proto.empacotar(proto.LISTAR_CANAIS, NOME))
    return list(resp.canais)


# thread que fica recebendo as mensagens dos canais inscritos
def receber(sub):
    while True:
        topico, corpo = sub.recv_multipart()
        msg = proto.desempacotar(corpo)
        recebido = datetime.datetime.now().strftime("%H:%M:%S")
        enviado = datetime.datetime.fromtimestamp(msg.timestamp / 1000).strftime("%H:%M:%S")
        canal = dict(msg.dados).get("canal", "")
        texto = dict(msg.dados).get("texto", "")
        print(f"{NOME} [canal {canal}] '{texto}' (enviado {enviado}, recebido {recebido})",
              flush=True)


def main():
    socket = context.socket(zmq.REQ)
    socket.connect(BROKER_ADDR)

    sub = context.socket(zmq.SUB)
    sub.connect(PROXY_ADDR)

    time.sleep(2)
    print(f"{NOME} iniciou", flush=True)

    fazer_login(socket)

    canais = listar_canais(socket)

    # 1) se existirem menos de 5 canais, cria um novo
    if len(canais) < 5:
        novo = f"canal-{NOME}-{random.randint(1, 999)}"
        enviar(socket, proto.empacotar(proto.CRIAR_CANAL, NOME, {"canal": novo}))
        canais = listar_canais(socket)

    # 2) se inscrito em menos de 3 canais, inscreve em mais um
    inscritos = []
    while len(inscritos) < 3 and canais:
        escolhido = random.choice(canais)
        if escolhido not in inscritos:
            sub.setsockopt_string(zmq.SUBSCRIBE, escolhido)
            inscritos.append(escolhido)
            print(f"{NOME} inscrito em: {escolhido}", flush=True)
        if len(set(inscritos)) >= len(set(canais)):
            break

    # thread de recebimento das publicacoes
    threading.Thread(target=receber, args=(sub,), daemon=True).start()

    # 3) loop infinito: escolhe um canal e envia 10 mensagens aleatorias (1s entre elas)
    while True:
        canais = listar_canais(socket)
        if not canais:
            time.sleep(1)
            continue
        canal = random.choice(canais)
        for i in range(10):
            texto = f"msg {random.randint(1, 1000)}"
            enviar(socket, proto.empacotar(proto.PUBLICAR, NOME,
                                           {"canal": canal, "texto": texto}))
            time.sleep(1)


main()
