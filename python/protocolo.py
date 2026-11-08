import time
import datetime

import mensagem_pb2

# tipos de mensagem
LOGIN = "login"
LOGIN_OK = "login_ok"
LOGIN_ERRO = "login_erro"
CRIAR_CANAL = "criar_canal"
CANAL_OK = "canal_ok"
CANAL_ERRO = "canal_erro"
LISTAR_CANAIS = "listar_canais"
LISTA_CANAIS = "lista_canais"
# parte 2 - publicacao
PUBLICAR = "publicar"
PUB_OK = "pub_ok"
PUB_ERRO = "pub_erro"


def agora_ms():
    return int(time.time() * 1000)


def empacotar(tipo, origem, dados=None, canais=None):
    msg = mensagem_pb2.Mensagem()
    msg.tipo = tipo
    msg.timestamp = agora_ms()
    msg.origem = origem
    if dados:
        for k, v in dados.items():
            msg.dados[k] = str(v)
    if canais:
        msg.canais.extend(canais)
    return msg.SerializeToString()


def desempacotar(raw):
    msg = mensagem_pb2.Mensagem()
    msg.ParseFromString(raw)
    return msg


def formatar(msg):
    dt = datetime.datetime.fromtimestamp(msg.timestamp / 1000).strftime("%H:%M:%S")
    dados = dict(msg.dados)
    extra = f" canais={list(msg.canais)}" if msg.canais else ""
    return f"[{dt}] {msg.tipo} de {msg.origem} dados={dados}{extra}"
