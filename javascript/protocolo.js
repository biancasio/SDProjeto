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
// parte 2 - publicacao
const PUBLICAR = "publicar";
const PUB_OK = "pub_ok";
const PUB_ERRO = "pub_erro";

const root = protobuf.loadSync(path.join(__dirname, "mensagem.proto"));
const Mensagem = root.lookupType("cc7261.Mensagem");

function agoraMs() {
  return Date.now();
}

function empacotar(tipo, origem, dados = null, canais = null) {
  const msg = Mensagem.create({
    tipo,
    timestamp: agoraMs(),
    origem,
    dados: dados || {},
    canais: canais || [],
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
  return `[${dt}] ${msg.tipo} de ${msg.origem} dados=${JSON.stringify(dados)}${extra}`;
}

module.exports = {
  LOGIN, LOGIN_OK, LOGIN_ERRO,
  CRIAR_CANAL, CANAL_OK, CANAL_ERRO,
  LISTAR_CANAIS, LISTA_CANAIS,
  PUBLICAR, PUB_OK, PUB_ERRO,
  agoraMs, empacotar, desempacotar, formatar,
};
