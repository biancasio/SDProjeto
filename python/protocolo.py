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
# parte 3 - referencia (rank, lista de servidores, heartbeat, hora)
RANK = "rank"
RANK_OK = "rank_ok"
LISTAR_SERVIDORES = "listar_servidores"
LISTA_SERVIDORES = "lista_servidores"
HEARTBEAT = "heartbeat"
HEARTBEAT_OK = "heartbeat_ok"
# parte 4 - eleicao e sincronizacao de relogio (Berkeley)
RELOGIO = "relogio"          # servidor pede a hora ao coordenador
RELOGIO_OK = "relogio_ok"    # coordenador responde a hora correta
ELEICAO = "eleicao"          # servidor pede eleicao aos outros
ELEICAO_OK = "eleicao_ok"    # resposta da eleicao
COORDENADOR = "coordenador"  # anuncio do coordenador (publicado no topico servers)
TOPICO_SERVERS = "servers"   # topico pub/sub para anunciar o coordenador
# parte 5 - replicacao (consistencia eventual via pub/sub)
REPLICAR = "replicar"            # alteracao a ser replicada nos outros servidores
TOPICO_REPLICACAO = "replicacao" # topico pub/sub para replicar os dados


# relogio logico de Lamport
class Relogio:
    def __init__(self):
        self.contador = 0

    def evento(self):
        # incrementa antes de enviar
        self.contador += 1
        return self.contador

    def receber(self, recebido):
        # novo valor = max(meu, recebido)
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
