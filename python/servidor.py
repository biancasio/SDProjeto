import os
import json
import time
import threading
import zmq

import protocolo as proto

DATA_FILE = os.environ.get("DATA_FILE", "/data/dados.json")
NOME = os.environ.get("NOME_SERVIDOR", "servidor-py")
BROKER_ADDR = os.environ.get("BROKER_ADDR", "tcp://broker-py:5556")
PROXY_PUB = os.environ.get("PROXY_PUB", "tcp://proxy-py:5557")   # publicar
PROXY_SUB = os.environ.get("PROXY_SUB", "tcp://proxy-py:5558")   # assinar
REF_ADDR = os.environ.get("REF_ADDR", "tcp://referencia-py:5560")
PORTA_COORD = os.environ.get("PORTA_COORD", "5561")  # porta REP entre servidores

relogio = proto.Relogio()
rank = [None]
coordenador = [None]       # nome do servidor coordenador
ranks = {}                 # mapa nome -> rank (atualizado pela referencia)
contador_msg = [0]
hora_offset = [0]          # ajuste do relogio fisico (Berkeley)
eleicao_pendente = [False] # marcado quando recebo um pedido de eleicao de rank menor
em_eleicao = [False]       # trava: evita iniciar duas eleicoes ao mesmo tempo
anuncio_recebido = [False] # marca se o anuncio do coordenador ja chegou
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


# --- parte 5: replicacao (consistencia eventual via pub/sub) ---
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


# --- comunicacao com a referencia ---
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
            ranks[n] = int(r)   # guarda o rank de cada servidor conhecido
    return out


def enviar_heartbeat(ref):
    ref.send(proto.empacotar(proto.HEARTBEAT, NOME, {"nome": NOME}, contador=relogio.evento()))
    resp = proto.desempacotar(ref.recv())
    relogio.receber(resp.contador)


# --- chamada REQ para outro servidor (porta 5561) com timeout ---
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


def virar_coordenador(pub):
    coordenador[0] = NOME
    anuncio_recebido[0] = True
    hora_offset[0] = 0
    # anuncia 2x (com intervalo) para garantir a entrega mesmo com o slow joiner do pub/sub
    for _ in range(2):
        corpo = proto.empacotar(proto.COORDENADOR, NOME,
                                {"coordenador": NOME}, contador=relogio.evento())
        pub.send_multipart([proto.TOPICO_SERVERS.encode(), corpo])
        time.sleep(1)
    print(f"{NOME} se anunciou como coordenador (rank {rank[0]})", flush=True)


# --- eleicao por bullying (algoritmo do valentao) ---
# pede eleicao a todos os servidores de rank MAIOR. Se algum responde OK, ele
# assume a eleicao e este servidor espera o anuncio. Se nenhum responde (estao
# todos mortos), este servidor vira o coordenador e avisa pelo topico 'servers'.
def eleger(ref, pub):
    # trava: se ja estou numa eleicao, nao inicio outra (evita o ping-pong)
    if em_eleicao[0]:
        return
    em_eleicao[0] = True
    eleicao_pendente[0] = False
    anuncio_recebido[0] = False
    print(f"{NOME} iniciou eleicao (rank {rank[0]})", flush=True)
    try:
        servidores = listar_servidores(ref)
        maiores = [n for (n, r) in servidores if r > rank[0] and n != NOME]

        recebeu_ok = False
        for nome_maior in maiores:
            try:
                resp = chamar_servidor(nome_maior, proto.ELEICAO, {"nome": NOME})
                if resp.tipo == proto.ELEICAO_OK:
                    recebeu_ok = True
            except zmq.ZMQError:
                # servidor de rank maior nao respondeu: esta morto, ignora
                pass

        if recebeu_ok:
            print(f"{NOME} recebeu OK de rank maior, aguardando anuncio do coordenador",
                  flush=True)
            coordenador[0] = None
            # se o anuncio nao chegar em 10s (o maior pode ter morrido), refaz a eleicao
            def refazer_se_preciso():
                time.sleep(10)
                if not anuncio_recebido[0]:
                    print(f"{NOME}: anuncio nao chegou, refazendo eleicao", flush=True)
                    eleger(ref, pub)
            threading.Thread(target=refazer_se_preciso, daemon=True).start()
        else:
            virar_coordenador(pub)
    finally:
        em_eleicao[0] = False


# --- sincronizacao de relogio (Berkeley) ---
def sincronizar_relogio():
    if coordenador[0] == NOME or coordenador[0] is None:
        return
    resp = chamar_servidor(coordenador[0], proto.RELOGIO)
    t_envio = proto.agora_ms()
    # obs: RTT medido dentro de chamar_servidor seria ideal; aqui aproximamos
    hora_coord = int(dict(resp.dados).get("hora", proto.agora_ms()))
    hora_offset[0] = hora_coord - proto.agora_ms()
    print(f"{NOME} sincronizou com {coordenador[0]}: hora={hora_coord} "
          f"(offset={hora_offset[0]}ms)", flush=True)


# --- socket REP entre servidores (relogio + eleicao) ---
def servico_coordenador():
    s = context.socket(zmq.REP)
    s.bind(f"tcp://*:{PORTA_COORD}")
    while True:
        msg = proto.desempacotar(s.recv())
        with lock:
            relogio.receber(msg.contador)
        if msg.tipo == proto.RELOGIO:
            # so o coordenador responde a hora
            if coordenador[0] == NOME:
                resp = proto.empacotar(proto.RELOGIO_OK, NOME,
                                       {"hora": hora_local()}, contador=relogio.evento())
            else:
                resp = proto.empacotar(proto.RELOGIO_OK, NOME,
                                       {"motivo": "nao_coordenador"}, contador=relogio.evento())
        elif msg.tipo == proto.ELEICAO:
            # recebi pedido de eleicao de alguem de rank menor: respondo OK e,
            # por ter rank maior, marco que preciso rodar minha propria eleicao
            print(f"{NOME} recebeu pedido de eleicao de {msg.origem}", flush=True)
            eleicao_pendente[0] = True
            resp = proto.empacotar(proto.ELEICAO_OK, NOME, contador=relogio.evento())
        else:
            resp = proto.empacotar(proto.RELOGIO_OK, NOME,
                                   {"motivo": "tipo invalido"}, contador=relogio.evento())
        s.send(resp)


# --- thread que escuta os topicos 'servers' (coordenador) e 'replicacao' ---
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
            # so aceita um novo coordenador se ele tiver rank MAIOR OU IGUAL ao
            # do coordenador atual conhecido. Se eu sou o coordenador e chega um
            # anuncio de rank menor, ignoro (desempate por rank evita o ping-pong
            # entre dois servidores que se anunciam ao mesmo tempo).
            if novo and novo != coordenador[0]:
                rank_novo = ranks.get(novo, -1)
                rank_atual = ranks.get(coordenador[0], -1) if coordenador[0] else -1
                # se eu sou o coordenador e o novo tem rank menor que o meu, ignoro
                if coordenador[0] == NOME and rank_novo < (rank[0] or -1):
                    continue
                if rank_novo >= rank_atual:
                    coordenador[0] = novo
                    anuncio_recebido[0] = True
                    print(f"{NOME} soube que o coordenador agora e {novo} "
                          f"(rank {rank_novo})", flush=True)
        elif t == proto.TOPICO_REPLICACAO:
            aplicar_replicacao(msg)


# --- tratadores das mensagens de cliente ---
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

    # se recebi um pedido de eleicao de rank menor, rodo minha propria eleicao
    if eleicao_pendente[0]:
        eleger(ref, pub)

    contador_msg[0] += 1
    if contador_msg[0] % 10 == 0:
        enviar_heartbeat(ref)
    if contador_msg[0] % 15 == 0:
        try:
            sincronizar_relogio()
        except Exception:
            # o coordenador atual falhou: dispara nova eleicao
            print(f"{NOME}: coordenador {coordenador[0]} indisponivel, pedindo eleicao",
                  flush=True)
            eleger(ref, pub)
