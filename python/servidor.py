import os
import json
import zmq

import protocolo as proto

DATA_FILE = os.environ.get("DATA_FILE", "/data/dados.json")
NOME = os.environ.get("NOME_SERVIDOR", "servidor-py")
BROKER_ADDR = os.environ.get("BROKER_ADDR", "tcp://broker-py:5556")


def carregar():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {"logins": [], "canais": []}


def salvar(dados):
    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)


def login(dados, msg):
    usuario = dict(msg.dados).get("usuario", "").strip()
    if not usuario:
        return proto.empacotar(proto.LOGIN_ERRO, NOME, {"motivo": "usuario vazio"})
    dados["logins"].append({"usuario": usuario, "timestamp": proto.agora_ms()})
    salvar(dados)
    return proto.empacotar(proto.LOGIN_OK, NOME, {"usuario": usuario})


def criar_canal(dados, msg):
    canal = dict(msg.dados).get("canal", "").strip()
    if not canal:
        return proto.empacotar(proto.CANAL_ERRO, NOME, {"motivo": "canal vazio"})
    if canal in dados["canais"]:
        return proto.empacotar(proto.CANAL_ERRO, NOME, {"motivo": "canal ja existe"})
    dados["canais"].append(canal)
    salvar(dados)
    return proto.empacotar(proto.CANAL_OK, NOME, {"canal": canal})


def listar(dados, msg):
    return proto.empacotar(proto.LISTA_CANAIS, NOME, canais=dados["canais"])


context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect(BROKER_ADDR)

dados = carregar()
print(f"{NOME} rodando | canais={dados['canais']}", flush=True)

while True:
    msg = proto.desempacotar(socket.recv())
    print(f"{NOME} recebeu: {proto.formatar(msg)}", flush=True)

    if msg.tipo == proto.LOGIN:
        resp = login(dados, msg)
    elif msg.tipo == proto.CRIAR_CANAL:
        resp = criar_canal(dados, msg)
    elif msg.tipo == proto.LISTAR_CANAIS:
        resp = listar(dados, msg)
    else:
        resp = proto.empacotar(proto.LOGIN_ERRO, NOME, {"motivo": "tipo invalido"})

    print(f"{NOME} respondeu: {proto.formatar(proto.desempacotar(resp))}", flush=True)
    socket.send(resp)
