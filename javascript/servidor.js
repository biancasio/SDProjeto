const fs = require("fs");
const path = require("path");
const zmq = require("zeromq");
const proto = require("./protocolo");

const NOME = process.env.NOME_SERVIDOR || "servidor-js";
const DATA_FILE = process.env.DATA_FILE || "/data/dados.json";
const BROKER_ADDR = process.env.BROKER_ADDR || "tcp://broker-js:5556";

function carregar() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      return JSON.parse(fs.readFileSync(DATA_FILE, "utf-8"));
    }
  } catch (_) {}
  return { logins: [], canais: [] };
}

function salvar(dados) {
  fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true });
  fs.writeFileSync(DATA_FILE, JSON.stringify(dados, null, 2), "utf-8");
}

function login(dados, msg) {
  const usuario = (msg.dados.usuario || "").trim();
  if (!usuario) return proto.empacotar(proto.LOGIN_ERRO, NOME, { motivo: "usuario vazio" });
  dados.logins.push({ usuario, timestamp: proto.agoraMs() });
  salvar(dados);
  return proto.empacotar(proto.LOGIN_OK, NOME, { usuario });
}

function criarCanal(dados, msg) {
  const canal = (msg.dados.canal || "").trim();
  if (!canal) return proto.empacotar(proto.CANAL_ERRO, NOME, { motivo: "canal vazio" });
  if (dados.canais.includes(canal))
    return proto.empacotar(proto.CANAL_ERRO, NOME, { motivo: "canal ja existe" });
  dados.canais.push(canal);
  salvar(dados);
  return proto.empacotar(proto.CANAL_OK, NOME, { canal });
}

function listar(dados) {
  return proto.empacotar(proto.LISTA_CANAIS, NOME, null, dados.canais);
}

async function main() {
  const dados = carregar();
  const socket = new zmq.Reply();
  await socket.connect(BROKER_ADDR);
  console.log(`${NOME} rodando | canais=${JSON.stringify(dados.canais)}`);

  for await (const [raw] of socket) {
    const msg = proto.desempacotar(raw);
    console.log(`${NOME} recebeu: ${proto.formatar(msg)}`);

    let resp;
    if (msg.tipo === proto.LOGIN) resp = login(dados, msg);
    else if (msg.tipo === proto.CRIAR_CANAL) resp = criarCanal(dados, msg);
    else if (msg.tipo === proto.LISTAR_CANAIS) resp = listar(dados);
    else resp = proto.empacotar(proto.LOGIN_ERRO, NOME, { motivo: "tipo invalido" });

    console.log(`${NOME} respondeu: ${proto.formatar(proto.desempacotar(resp))}`);
    await socket.send(resp);
  }
}

main();
