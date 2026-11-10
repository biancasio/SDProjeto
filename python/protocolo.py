import time
import datetime

import mensagem_pb2

LOGIN = "login"
LOGIN_OK = "login_ok"
LOGIN_ERRO = "login_erro"
CRIAR_CANAL = "criar_canal"
CANAL_OK = "canal_ok"
CANAL_ERRO = "canal_erro"
LISTAR_CANAIS = "listar_canais"
LISTA_CANAIS = "lista_canais"

PUBLICAR = "publicar"
PUB_OK = "pub_ok"
PUB_ERRO = "pub_erro"

RANK = "rank"
RANK_OK = "rank_ok"
LISTAR_SERVIDORES = "listar_servidores"
LISTA_SERVIDORES = "lista_servidores"
HEARTBEAT = "heartbeat"
HEARTBEAT_OK = "heartbeat_ok"

RELOGIO = "relogio"          
RELOGIO_OK = "relogio_ok"    
ELEICAO = "eleicao"          
ELEICAO_OK = "eleicao_ok"    
COORDENADOR = "coordenador"  
TOPICO_SERVERS = "servers"   

REPLICAR = "replicar"           
TOPICO_REPLICACAO = "replicacao"

class Relogio:
    def __init__(self):
        self.contador = 0

    def evento(self):
        self.contador += 1
        return self.contador

    def receber(self, recebido):
        self.contador = max(self.contador, recebido)
        return self.contador

def agora_ms():
    return int(time.time() * 1000)

def empacotar(tipo, origem, dados=None, canais=None, contador=0):
    msg = mensagem_pb2.Mensagem()
    msg.tipo = tipo
    msg.timestamp = agora_ms()
    msg.origem = origem
    msg.contador = contador
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
    return f"[{dt} C{msg.contador}] {msg.tipo} de {msg.origem} dados={dados}{extra}"