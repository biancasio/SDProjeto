import os
import json
import time
import zmq

import protocolo as proto

DATA_FILE = os.environ.get("DATA_FILE", "/data/dados.json")
NOME = os.environ.get("NOME_SERVIDOR", "servidor-py")
BROKER_ADDR = os.environ.get("BROKER_ADDR", "tcp://broker-py:5556")
PROXY_ADDR = os.environ.get("PROXY_ADDR", "tcp://proxy-py:5557")


def carregar():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {"logins": [], "canais": [], "publicacoes": []}


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


def publicar(dados, msg, pub):
    canal = dict(msg.dados).get("canal", "").strip()
    texto = dict(msg.dados).get("texto", "")
    if not canal:
        return proto.empacotar(proto.PUB_ERRO, NOME, {"motivo": "canal vazio"})
    if canal not in dados["canais"]:
        dados["canais"].append(canal)

    # publica no proxy: 1o frame = topico (nome do canal), 2o frame = corpo protobuf
    corpo = proto.empacotar(proto.PUBLICAR, msg.origem,
                            {"canal": canal, "texto": texto})
    pub.send_multipart([canal.encode(), corpo])

    # persiste a publicacao
    dados["publicacoes"].append({
        "canal": canal, "texto": texto, "origem": msg.origem,
        "timestamp": proto.agora_ms(),
    })
    salvar(dados)
    return proto.empacotar(proto.PUB_OK, NOME, {"canal": canal})


context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect(BROKER_ADDR)

pub = context.socket(zmq.PUB)
pub.connect(PROXY_ADDR)
time.sleep(1)  # tempo para o PUB conectar antes de publicar

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
    elif msg.tipo == proto.PUBLICAR:
        resp = publicar(dados, msg, pub)
    else:
        resp = proto.empacotar(proto.LOGIN_ERRO, NOME, {"motivo": "tipo invalido"})

    print(f"{NOME} respondeu: {proto.formatar(proto.desempacotar(resp))}", flush=True)
    socket.send(resp)
