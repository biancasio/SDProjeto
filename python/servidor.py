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

ESPERA_ANUNCIO = 10   

relogio = proto.Relogio()
rank = [0]
coordenador = [None]
ranks = {}
hora_offset = [0]
contador_msg = [0]

em_eleicao = [False]
eleicao_pendente = [False]  
anuncio_recebido = [True]
prazo_anuncio = [None]
reanunciar = [False]

lock = threading.Lock()
context = zmq.Context()
dados = {"logins": [], "canais": [], "publicacoes": []}


def rank_de(nome):
    if nome == NOME:
        return rank[0]
    return ranks.get(nome, -1)


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


# --- parte 5: replicacao ---
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


# --- referencia ---
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
            ranks[n] = int(r)
    return out


def enviar_heartbeat(ref):
    ref.send(proto.empacotar(proto.HEARTBEAT, NOME, {"nome": NOME}, contador=relogio.evento()))
    resp = proto.desempacotar(ref.recv())
    relogio.receber(resp.contador)


def chamar_servidor(nome_destino, tipo, dados_msg=None):
    s = context.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, 2000)
    s.setsockopt(zmq.SNDTIMEO, 2000)
    s.setsockopt(zmq.LINGER, 0)
    try:
        s.connect(f"tcp://{nome_destino}:{PORTA_COORD}")
        s.send(proto.empacotar(tipo, NOME, dados_msg, contador=relogio.evento()))
        resp = proto.desempacotar(s.recv())
        relogio.receber(resp.contador)
        return resp
    finally:
        s.close()


def anunciar(pub):
    corpo = proto.empacotar(proto.COORDENADOR, NOME,
                            {"coordenador": NOME, "rank": rank[0]},
                            contador=relogio.evento())
    pub.send_multipart([proto.TOPICO_SERVERS.encode(), corpo])


def virar_coordenador(pub):
    coordenador[0] = NOME
    hora_offset[0] = 0
    anuncio_recebido[0] = True
    prazo_anuncio[0] = None
    print(f"{NOME} se anunciou como coordenador (rank {rank[0]})", flush=True)
    for i in range(2):         
        anunciar(pub)
        if i == 0:
            time.sleep(1)


def eleger(ref, pub):
    eleicao_pendente[0] = False
    if em_eleicao[0]:
        return
    em_eleicao[0] = True
    anuncio_recebido[0] = False
    try:
        print(f"{NOME} iniciou eleicao (rank {rank[0]})", flush=True)
        aviso = proto.empacotar(proto.AVISO_ELEICAO, NOME,
                                {"iniciador": NOME}, contador=relogio.evento())
        pub.send_multipart([proto.TOPICO_ELEICOES.encode(), aviso])

        maiores = [(n, r) for (n, r) in listar_servidores(ref) if r > rank[0] and n != NOME]
        recebeu_ok = False
        for n, r in maiores:
            try:
                resp = chamar_servidor(n, proto.ELEICAO, {"nome": NOME})
                if resp.tipo == proto.ELEICAO_OK:
                    recebeu_ok = True
                    print(f"{NOME} recebeu OK de {n} (rank {r})", flush=True)
            except Exception:
                print(f"{NOME}: {n} (rank {r}) nao respondeu", flush=True)

        if recebeu_ok:
            print(f"{NOME} aguardando anuncio do coordenador", flush=True)
            prazo_anuncio[0] = time.time() + ESPERA_ANUNCIO
        else:
            virar_coordenador(pub)
    except Exception as e:
        print(f"{NOME}: erro na eleicao ({e})", flush=True)
        eleicao_pendente[0] = True
    finally:
        em_eleicao[0] = False


def sincronizar_relogio():
    alvo = coordenador[0]
    if alvo is None:
        eleicao_pendente[0] = True
        return
    if alvo == NOME:
        return
    t0 = proto.agora_ms()
    try:
        resp = chamar_servidor(alvo, proto.RELOGIO)
    except Exception:
        print(f"{NOME}: coordenador {alvo} indisponivel, iniciando eleicao", flush=True)
        eleicao_pendente[0] = True
        return
    t1 = proto.agora_ms()
    d = dict(resp.dados)
    if "hora" not in d:
        print(f"{NOME}: {alvo} nao e o coordenador, iniciando eleicao", flush=True)
        eleicao_pendente[0] = True
        return
    rtt = t1 - t0
    hora_offset[0] = (int(d["hora"]) + rtt // 2) - t1
    print(f"{NOME} sincronizou com {alvo}: rtt={rtt}ms offset={hora_offset[0]}ms", flush=True)


def servico_coordenador():
    s = context.socket(zmq.REP)
    s.bind(f"tcp://*:{PORTA_COORD}")
    while True:
        raw = s.recv()
        try:
            msg = proto.desempacotar(raw)
            with lock:
                relogio.receber(msg.contador)
            if msg.tipo == proto.RELOGIO:
                if coordenador[0] == NOME:
                    resp = proto.empacotar(proto.RELOGIO_OK, NOME,
                                           {"hora": hora_local()}, contador=relogio.evento())
                else:
                    resp = proto.empacotar(proto.RELOGIO_OK, NOME,
                                           {"motivo": "nao_coordenador"}, contador=relogio.evento())
            elif msg.tipo == proto.ELEICAO:
                print(f"{NOME} recebeu pedido de eleicao de {msg.origem}", flush=True)
                if not em_eleicao[0]:
                    eleicao_pendente[0] = True
                resp = proto.empacotar(proto.ELEICAO_OK, NOME, contador=relogio.evento())
            else:
                resp = proto.empacotar(proto.RELOGIO_OK, NOME,
                                       {"motivo": "tipo invalido"}, contador=relogio.evento())
        except Exception as e:
            print(f"{NOME}: erro entre servidores ({e})", flush=True)
            resp = proto.empacotar(proto.RELOGIO_OK, NOME, {"motivo": "erro"})
        s.send(resp)


def tratar_anuncio(msg):
    d = dict(msg.dados)
    novo = d.get("coordenador", "")
    if not novo or novo == NOME:
        return
    rank_novo = int(d.get("rank", ranks.get(novo, -1)))
    ranks[novo] = rank_novo
    atual = coordenador[0]

    if atual is None or novo == atual or rank_novo >= rank_de(atual):
        if novo != atual:
            print(f"{NOME} soube que o coordenador agora e {novo} (rank {rank_novo})", flush=True)
        coordenador[0] = novo
        anuncio_recebido[0] = True
        prazo_anuncio[0] = None
    elif atual == NOME:
        reanunciar[0] = True   


def escutar_pubsub():
    sub = context.socket(zmq.SUB)
    sub.connect(PROXY_SUB)
    sub.setsockopt_string(zmq.SUBSCRIBE, proto.TOPICO_SERVERS)
    sub.setsockopt_string(zmq.SUBSCRIBE, proto.TOPICO_REPLICACAO)
    sub.setsockopt_string(zmq.SUBSCRIBE, proto.TOPICO_ELEICOES)
    while True:
        try:
            topico, corpo = sub.recv_multipart()
            msg = proto.desempacotar(corpo)
            with lock:
                relogio.receber(msg.contador)
            t = topico.decode()
            if t == proto.TOPICO_SERVERS:
                tratar_anuncio(msg)
            elif t == proto.TOPICO_REPLICACAO:
                aplicar_replicacao(msg)
            elif t == proto.TOPICO_ELEICOES:
                iniciador = dict(msg.dados).get("iniciador", "")
                if iniciador != NOME:
                    print(f"{NOME} soube que {iniciador} iniciou uma eleicao", flush=True)
        except Exception as e:
            print(f"{NOME}: erro no pub/sub ({e})", flush=True)


def login(msg, pub):
    usuario = dict(msg.dados).get("usuario", "").strip()
    if not usuario:
        return proto.empacotar(proto.LOGIN_ERRO, NOME, {"motivo": "usuario vazio"},
                               contador=relogio.evento())
    ts = hora_local()
    with lock:
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
    with lock:
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
    corpo = proto.empacotar(proto.PUBLICAR, msg.origem,
                            {"canal": canal, "texto": texto}, contador=relogio.evento())
    pub.send_multipart([canal.encode(), corpo])
    ts = hora_local()
    with lock:
        if canal not in dados["canais"]:
            dados["canais"].append(canal)
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
time.sleep(3)
eleger(ref, pub)
print(f"{NOME} rodando | rank={rank[0]} | coordenador={coordenador[0]}", flush=True)

poller = zmq.Poller()
poller.register(socket, zmq.POLLIN)

while True:
    eventos = dict(poller.poll(500))
    if socket in eventos:
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
            try:
                enviar_heartbeat(ref)
            except Exception as e:
                print(f"{NOME}: heartbeat falhou ({e})", flush=True)
        if contador_msg[0] % 15 == 0:
            sincronizar_relogio()

    if prazo_anuncio[0] is not None and time.time() > prazo_anuncio[0]:
        prazo_anuncio[0] = None
        if not anuncio_recebido[0]:
            print(f"{NOME}: anuncio nao chegou, refazendo eleicao", flush=True)
            eleicao_pendente[0] = True

    if eleicao_pendente[0]:
        eleger(ref, pub)

    if reanunciar[0] and coordenador[0] == NOME:
        reanunciar[0] = False
        anunciar(pub)