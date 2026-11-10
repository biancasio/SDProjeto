import os
import json
import time
import threading
import zmq

import protocolo as proto

DATA_FILE = os.environ.get("DATA_FILE", "/data/dados.json")
NOME = os.environ.get("NOME_SERVIDOR", "servidor-py")
BROKER_ADDR = os.environ.get("BROKER_ADDR", "tcp://broker-py:5556")
PROXY_PUB = os.environ.get("PROXY_PUB", "tcp://proxy-py:5557")   
PROXY_SUB = os.environ.get("PROXY_SUB", "tcp://proxy-py:5558")   
REF_ADDR = os.environ.get("REF_ADDR", "tcp://referencia-py:5560")
PORTA_COORD = os.environ.get("PORTA_COORD", "5561")  

relogio = proto.Relogio()
rank = [None]
coordenador = [None]       
contador_msg = [0]         
hora_offset = [0]          
lock = threading.Lock()

context = zmq.Context()
dados = {"logins": [], "canais": [], "publicacoes": []}


def carregar():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {"logins": [], "canais": [], "publicacoes": []}


def salvar(d):
    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)


def hora_local():
    return proto.agora_ms() + hora_offset[0]

def replicar(pub, acao, info):
    corpo = proto.empacotar(proto.REPLICAR, NOME,
                            dict({"acao": acao}, **info), contador=relogio.evento())
    pub.send_multipart([proto.TOPICO_REPLICACAO.encode(), corpo])


def aplicar_replicacao(msg):
    if msg.origem == NOME:
        return
    d = dict(msg.dados)
    acao = d.get("acao", "")
    with lock:
        if acao == "login":
            dados["logins"].append({"usuario": d.get("usuario", ""),
                                    "timestamp": int(d.get("timestamp", 0))})
        elif acao == "canal":
            canal = d.get("canal", "")
            if canal and canal not in dados["canais"]:
                dados["canais"].append(canal)
        elif acao == "publicacao":
            canal = d.get("canal", "")
            if canal and canal not in dados["canais"]:
                dados["canais"].append(canal)
            dados["publicacoes"].append({
                "canal": canal, "texto": d.get("texto", ""),
                "origem": d.get("quem", ""), "timestamp": int(d.get("timestamp", 0)),
            })
        salvar(dados)
    print(f"{NOME} replicou {acao} de {msg.origem}", flush=True)

def pedir_rank(ref):
    ref.send(proto.empacotar(proto.RANK, NOME, {"nome": NOME}, contador=relogio.evento()))
    resp = proto.desempacotar(ref.recv())
    relogio.receber(resp.contador)
    rank[0] = int(dict(resp.dados).get("rank", 0))
    print(f"{NOME} recebeu rank {rank[0]}", flush=True)


def listar_servidores(ref):
    ref.send(proto.empacotar(proto.LISTAR_SERVIDORES, NOME, contador=relogio.evento()))
    resp = proto.desempacotar(ref.recv())
    relogio.receber(resp.contador)
    out = []
    for item in resp.canais:
        if ":" in item:
            n, r = item.rsplit(":", 1)
            out.append((n, int(r)))
    return out


def enviar_heartbeat(ref):
    ref.send(proto.empacotar(proto.HEARTBEAT, NOME, {"nome": NOME}, contador=relogio.evento()))
    resp = proto.desempacotar(ref.recv())
    relogio.receber(resp.contador)

def eleger(ref, pub):
    servidores = listar_servidores(ref)
    if not servidores:
        coordenador[0] = NOME
        return
    vencedor = max(servidores, key=lambda x: x[1])[0]
    coordenador[0] = vencedor
    print(f"{NOME} acha que o coordenador e {vencedor}", flush=True)
    if vencedor == NOME:
        corpo = proto.empacotar(proto.COORDENADOR, NOME,
                                {"coordenador": NOME}, contador=relogio.evento())
        pub.send_multipart([proto.TOPICO_SERVERS.encode(), corpo])
        print(f"{NOME} se anunciou como coordenador", flush=True)


def sincronizar_relogio():
    if coordenador[0] == NOME or coordenador[0] is None:
        return
    try:
        s = context.socket(zmq.REQ)
        s.setsockopt(zmq.RCVTIMEO, 3000)
        s.setsockopt(zmq.LINGER, 0)
        s.connect(f"tcp://{coordenador[0]}:{PORTA_COORD}")
        t_envio = proto.agora_ms()
        s.send(proto.empacotar(proto.RELOGIO, NOME, contador=relogio.evento()))
        resp = proto.desempacotar(s.recv())
        relogio.receber(resp.contador)
        rtt = proto.agora_ms() - t_envio
        hora_coord = int(dict(resp.dados).get("hora", proto.agora_ms()))
        hora_offset[0] = (hora_coord + rtt // 2) - proto.agora_ms()
        print(f"{NOME} sincronizou com {coordenador[0]}: hora={hora_coord} "
              f"(rtt={rtt}ms, offset={hora_offset[0]}ms)", flush=True)
        s.close()
    except zmq.ZMQError:
        print(f"{NOME}: coordenador {coordenador[0]} indisponivel, pedindo eleicao",
              flush=True)
        raise
def servico_coordenador():
    s = context.socket(zmq.REP)
    s.bind(f"tcp://*:{PORTA_COORD}")
    while True:
        msg = proto.desempacotar(s.recv())
        with lock:
            relogio.receber(msg.contador)
        if msg.tipo == proto.RELOGIO:
            resp = proto.empacotar(proto.RELOGIO_OK, NOME,
                                   {"hora": hora_local()}, contador=relogio.evento())
        elif msg.tipo == proto.ELEICAO:
            resp = proto.empacotar(proto.ELEICAO_OK, NOME, contador=relogio.evento())
        else:
            resp = proto.empacotar(proto.RELOGIO_OK, NOME,
                                   {"motivo": "tipo invalido"}, contador=relogio.evento())
        s.send(resp)

def escutar_pubsub():
    sub = context.socket(zmq.SUB)
    sub.connect(PROXY_SUB)
    sub.setsockopt_string(zmq.SUBSCRIBE, proto.TOPICO_SERVERS)
    sub.setsockopt_string(zmq.SUBSCRIBE, proto.TOPICO_REPLICACAO)
    while True:
        topico, corpo = sub.recv_multipart()
        msg = proto.desempacotar(corpo)
        with lock:
            relogio.receber(msg.contador)
        t = topico.decode()
        if t == proto.TOPICO_SERVERS:
            novo = dict(msg.dados).get("coordenador", "")
            if novo and novo != coordenador[0]:
                coordenador[0] = novo
                print(f"{NOME} soube que o coordenador agora e {novo}", flush=True)
        elif t == proto.TOPICO_REPLICACAO:
            aplicar_replicacao(msg)

def login(msg, pub):
    usuario = dict(msg.dados).get("usuario", "").strip()
    if not usuario:
        return proto.empacotar(proto.LOGIN_ERRO, NOME, {"motivo": "usuario vazio"},
                               contador=relogio.evento())
    ts = hora_local()
    dados["logins"].append({"usuario": usuario, "timestamp": ts})
    salvar(dados)
    replicar(pub, "login", {"usuario": usuario, "timestamp": ts})
    return proto.empacotar(proto.LOGIN_OK, NOME, {"usuario": usuario},
                           contador=relogio.evento())


def criar_canal(msg, pub):
    canal = dict(msg.dados).get("canal", "").strip()
    if not canal:
        return proto.empacotar(proto.CANAL_ERRO, NOME, {"motivo": "canal vazio"},
                               contador=relogio.evento())
    if canal in dados["canais"]:
        return proto.empacotar(proto.CANAL_ERRO, NOME, {"motivo": "canal ja existe"},
                               contador=relogio.evento())
    dados["canais"].append(canal)
    salvar(dados)
    replicar(pub, "canal", {"canal": canal})
    return proto.empacotar(proto.CANAL_OK, NOME, {"canal": canal},
                           contador=relogio.evento())


def listar(msg):
    return proto.empacotar(proto.LISTA_CANAIS, NOME, canais=dados["canais"],
                           contador=relogio.evento())


def publicar(msg, pub):
    canal = dict(msg.dados).get("canal", "").strip()
    texto = dict(msg.dados).get("texto", "")
    if not canal:
        return proto.empacotar(proto.PUB_ERRO, NOME, {"motivo": "canal vazio"},
                               contador=relogio.evento())
    if canal not in dados["canais"]:
        dados["canais"].append(canal)
    corpo = proto.empacotar(proto.PUBLICAR, msg.origem,
                            {"canal": canal, "texto": texto}, contador=relogio.evento())
    pub.send_multipart([canal.encode(), corpo])
    ts = hora_local()
    dados["publicacoes"].append({
        "canal": canal, "texto": texto, "origem": msg.origem, "timestamp": ts,
    })
    salvar(dados)
    replicar(pub, "publicacao",
             {"canal": canal, "texto": texto, "quem": msg.origem, "timestamp": ts})
    return proto.empacotar(proto.PUB_OK, NOME, {"canal": canal},
                           contador=relogio.evento())


socket = context.socket(zmq.REP)
socket.connect(BROKER_ADDR)

pub = context.socket(zmq.PUB)
pub.connect(PROXY_PUB)

ref = context.socket(zmq.REQ)
ref.connect(REF_ADDR)

time.sleep(1)
dados = carregar()

pedir_rank(ref)
threading.Thread(target=servico_coordenador, daemon=True).start()
threading.Thread(target=escutar_pubsub, daemon=True).start()
time.sleep(2)
eleger(ref, pub)
print(f"{NOME} rodando | rank={rank[0]} | coordenador={coordenador[0]} | "
      f"canais={dados['canais']}", flush=True)

while True:
    msg = proto.desempacotar(socket.recv())
    with lock:
        relogio.receber(msg.contador)
    print(f"{NOME} recebeu: {proto.formatar(msg)}", flush=True)

    if msg.tipo == proto.LOGIN:
        resp = login(msg, pub)
    elif msg.tipo == proto.CRIAR_CANAL:
        resp = criar_canal(msg, pub)
    elif msg.tipo == proto.LISTAR_CANAIS:
        resp = listar(msg)
    elif msg.tipo == proto.PUBLICAR:
        resp = publicar(msg, pub)
    else:
        resp = proto.empacotar(proto.LOGIN_ERRO, NOME, {"motivo": "tipo invalido"},
                               contador=relogio.evento())

    print(f"{NOME} respondeu: {proto.formatar(proto.desempacotar(resp))}", flush=True)
    socket.send(resp)

    contador_msg[0] += 1
    if contador_msg[0] % 10 == 0:
        enviar_heartbeat(ref)
    if contador_msg[0] % 15 == 0:
        try:
            sincronizar_relogio()
        except Exception:
            eleger(ref, pub)