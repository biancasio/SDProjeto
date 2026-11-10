const path = require("path");
const protobuf = require("protobufjs");

const LOGIN = "login";
const LOGIN_OK = "login_ok";
const LOGIN_ERRO = "login_erro";
const CRIAR_CANAL = "criar_canal";
const CANAL_OK = "canal_ok";
const CANAL_ERRO = "canal_erro";
const LISTAR_CANAIS = "listar_canais";
const LISTA_CANAIS = "lista_canais";

const PUBLICAR = "publicar";
const PUB_OK = "pub_ok";
const PUB_ERRO = "pub_erro";

const RANK = "rank";
const RANK_OK = "rank_ok";
const LISTAR_SERVIDORES = "listar_servidores";
const LISTA_SERVIDORES = "lista_servidores";
const HEARTBEAT = "heartbeat";
const HEARTBEAT_OK = "heartbeat_ok";

const RELOGIO = "relogio";
const RELOGIO_OK = "relogio_ok";
const ELEICAO = "eleicao";
const ELEICAO_OK = "eleicao_ok";
const COORDENADOR = "coordenador";
const TOPICO_SERVERS = "servers";

const REPLICAR = "replicar";
const TOPICO_REPLICACAO = "replicacao";

const root = protobuf.loadSync(path.join(__dirname, "mensagem.proto"));
const Mensagem = root.lookupType("cc7261.Mensagem");

class Relogio {
  constructor() {
    this.contador = 0;
  }
  evento() {
    this.contador += 1; 
    return this.contador;
  }
  receber(recebido) {
    this.contador = Math.max(this.contador, Number(recebido) || 0);
    return this.contador;
  }
}

function agoraMs() {
  return Date.now();
}

function empacotar(tipo, origem, dados = null, canais = null, contador = 0) {
  const msg = Mensagem.create({
    tipo,
    timestamp: agoraMs(),
    origem,
    dados: dados || {},
    canais: canais || [],
    contador,
  });
  return Mensagem.encode(msg).finish();
}

function desempacotar(raw) {
  return Mensagem.decode(raw);
}

function formatar(msg) {
  const dt = new Date(Number(msg.timestamp)).toISOString().substr(11, 8);
  const dados = msg.dados || {};
  const extra = msg.canais && msg.canais.length ? ` canais=${JSON.stringify(msg.canais)}` : "";
  return `[${dt} C${msg.contador}] ${msg.tipo} de ${msg.origem} dados=${JSON.stringify(dados)}${extra}`;
}

module.exports = {
  LOGIN, LOGIN_OK, LOGIN_ERRO,
  CRIAR_CANAL, CANAL_OK, CANAL_ERRO,
  LISTAR_CANAIS, LISTA_CANAIS,
  PUBLICAR, PUB_OK, PUB_ERRO,
  RANK, RANK_OK, LISTAR_SERVIDORES, LISTA_SERVIDORES, HEARTBEAT, HEARTBEAT_OK,
  RELOGIO, RELOGIO_OK, ELEICAO, ELEICAO_OK, COORDENADOR, TOPICO_SERVERS,
  REPLICAR, TOPICO_REPLICACAO,
  Relogio, agoraMs, empacotar, desempacotar, formatar,
};