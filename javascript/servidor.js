const fs = require("fs");
const path = require("path");
const zmq = require("zeromq");
const proto = require("./protocolo");

const NOME = process.env.NOME_SERVIDOR || "servidor-js";
const DATA_FILE = process.env.DATA_FILE || "/dacta/dados.json";
const BROKER_ADDR = process.env.BROKER_ADDR || "tcp://broker-js:5556";
const PROXY_PUB = process.env.PROXY_PUB || "tcp://proxy-py:5557";
const PROXY_SUB = process.env.PROXY_SUB || "tcp://proxy-py:5558";
const REF_ADDR = process.env.REF_ADDR || "tcp://referencia-js:5560";
const PORTA_COORD = process.env.PORTA_COORD || "5561";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const relogio = new proto.Relogio();
let rank = null;
let coordenador = null;
let contadorMsg = 0;
let horaOffset = 0;

const ref = new zmq.Request();
const pub = new zmq.Publisher();
let dados = { logins: [], canais: [], publicacoes: [] };

function carregar() {
  try {
    if (fs.existsSync(DATA_FILE)) return JSON.parse(fs.readFileSync(DATA_FILE, "utf-8"));
  } catch (_) {}
  return { logins: [], canais: [], publicacoes: [] };
}

function salvar(d) {
  fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true });
  fs.writeFileSync(DATA_FILE, JSON.stringify(d, null, 2), "utf-8");
}

function horaLocal() {
  return proto.agoraMs() + horaOffset;
}

// --- parte 5: replicacao (consistencia eventual via pub/sub) ---
async function replicar(acao, info) {
  const corpo = proto.empacotar(proto.REPLICAR, NOME,
    Object.assign({ acao }, info), null, relogio.evento());
  await pub.send([proto.TOPICO_REPLICACAO, corpo]);
}

function aplicarReplicacao(msg) {
  if (msg.origem === NOME) return;
  const d = msg.dados;
  const acao = d.acao || "";
  if (acao === "login") {
    dados.logins.push({ usuario: d.usuario || "", timestamp: Number(d.timestamp || 0) });
  } else if (acao === "canal") {
    if (d.canal && !dados.canais.includes(d.canal)) dados.canais.push(d.canal);
  } else if (acao === "publicacao") {
    if (d.canal && !dados.canais.includes(d.canal)) dados.canais.push(d.canal);
    dados.publicacoes.push({
      canal: d.canal || "", texto: d.texto || "",
      origem: d.quem || "", timestamp: Number(d.timestamp || 0),
    });
  }
  salvar(dados);
  console.log(`${NOME} replicou ${acao} de ${msg.origem}`);
}

// --- comunicacao com a referencia ---
async function pedirRank() {
  await ref.send(proto.empacotar(proto.RANK, NOME, { nome: NOME }, null, relogio.evento()));
  const [raw] = await ref.receive();
  const resp = proto.desempacotar(raw);
  relogio.receber(resp.contador);
  rank = Number(resp.dados.rank || 0);
  console.log(`${NOME} recebeu rank ${rank}`);
}

async function listarServidores() {
  await ref.send(proto.empacotar(proto.LISTAR_SERVIDORES, NOME, null, null, relogio.evento()));
  const [raw] = await ref.receive();
  const resp = proto.desempacotar(raw);
  relogio.receber(resp.contador);
  const out = [];
  for (const item of resp.canais || []) {
    const i = item.lastIndexOf(":");
    if (i > 0) out.push([item.slice(0, i), Number(item.slice(i + 1))]);
  }
  return out;
}

async function enviarHeartbeat() {
  await ref.send(proto.empacotar(proto.HEARTBEAT, NOME, { nome: NOME }, null, relogio.evento()));
  const [raw] = await ref.receive();
  relogio.receber(proto.desempacotar(raw).contador);
  // parte 4: a referencia NAO devolve mais a hora (vem do coordenador)
}

// --- eleicao por bullying (maior rank vence) ---
async function eleger() {
  const servidores = await listarServidores();
  if (!servidores.length) {
    coordenador = NOME;
    return;
  }
  let vencedor = servidores[0];
  for (const s of servidores) if (s[1] > vencedor[1]) vencedor = s;
  coordenador = vencedor[0];
  console.log(`${NOME} acha que o coordenador e ${coordenador}`);
  if (coordenador === NOME) {
    const corpo = proto.empacotar(proto.COORDENADOR, NOME, { coordenador: NOME }, null, relogio.evento());
    await pub.send([proto.TOPICO_SERVERS, corpo]);
    console.log(`${NOME} se anunciou como coordenador`);
  }
}

// --- sincronizacao de relogio (Berkeley) ---
async function sincronizarRelogio() {
  if (coordenador === NOME || coordenador === null) return;
  const s = new zmq.Request();
  s.receiveTimeout = 3000;
  s.connect(`tcp://${coordenador}:${PORTA_COORD}`);
  try {
    const tEnvio = proto.agoraMs();
    await s.send(proto.empacotar(proto.RELOGIO, NOME, null, null, relogio.evento()));
    const [raw] = await s.receive();
    const resp = proto.desempacotar(raw);
    relogio.receber(resp.contador);
    // latencia: metade do tempo de ida e volta (RTT), como no slide de Berkeley
    const rtt = proto.agoraMs() - tEnvio;
    const horaCoord = Number(resp.dados.hora || proto.agoraMs());
    // compensa a latencia: a hora do coordenador ja "andou" ~rtt/2 ate chegar
    horaOffset = (horaCoord + Math.floor(rtt / 2)) - proto.agoraMs();
    console.log(`${NOME} sincronizou com ${coordenador}: hora=${horaCoord} (rtt=${rtt}ms, offset=${horaOffset}ms)`);
    s.close();
  } catch (e) {
    s.close();
    console.log(`${NOME}: coordenador ${coordenador} indisponivel, pedindo eleicao`);
    throw e;
  }
}

// --- socket REP do coordenador (responde a hora para os outros) ---
async function servicoCoordenador() {
  const s = new zmq.Reply();
  await s.bind(`tcp://*:${PORTA_COORD}`);
  for await (const [raw] of s) {
    const msg = proto.desempacotar(raw);
    relogio.receber(msg.contador);
    let resp;
    if (msg.tipo === proto.RELOGIO)
      resp = proto.empacotar(proto.RELOGIO_OK, NOME, { hora: String(horaLocal()) }, null, relogio.evento());
    else if (msg.tipo === proto.ELEICAO)
      resp = proto.empacotar(proto.ELEICAO_OK, NOME, null, null, relogio.evento());
    else
      resp = proto.empacotar(proto.RELOGIO_OK, NOME, { motivo: "tipo invalido" }, null, relogio.evento());
    await s.send(resp);
  }
}

// --- escuta os topicos 'servers' (coordenador) e 'replicacao' ---
async function escutarPubsub() {
  const sub = new zmq.Subscriber();
  sub.connect(PROXY_SUB);
  sub.subscribe(proto.TOPICO_SERVERS);
  sub.subscribe(proto.TOPICO_REPLICACAO);
  for await (const [topico, corpo] of sub) {
    const msg = proto.desempacotar(corpo);
    relogio.receber(msg.contador);
    const t = topico.toString();
    if (t === proto.TOPICO_SERVERS) {
      const novo = msg.dados.coordenador || "";
      if (novo && novo !== coordenador) {
        coordenador = novo;
        console.log(`${NOME} soube que o coordenador agora e ${novo}`);
      }
    } else if (t === proto.TOPICO_REPLICACAO) {
      aplicarReplicacao(msg);
    }
  }
}

// --- tratadores das mensagens de cliente ---
async function login(msg) {
  const usuario = (msg.dados.usuario || "").trim();
  if (!usuario) return proto.empacotar(proto.LOGIN_ERRO, NOME, { motivo: "usuario vazio" }, null, relogio.evento());
  const ts = horaLocal();
  dados.logins.push({ usuario, timestamp: ts });
  salvar(dados);
  await replicar("login", { usuario, timestamp: String(ts) });
  return proto.empacotar(proto.LOGIN_OK, NOME, { usuario }, null, relogio.evento());
}

async function criarCanal(msg) {
  const canal = (msg.dados.canal || "").trim();
  if (!canal) return proto.empacotar(proto.CANAL_ERRO, NOME, { motivo: "canal vazio" }, null, relogio.evento());
  if (dados.canais.includes(canal))
    return proto.empacotar(proto.CANAL_ERRO, NOME, { motivo: "canal ja existe" }, null, relogio.evento());
  dados.canais.push(canal);
  salvar(dados);
  await replicar("canal", { canal });
  return proto.empacotar(proto.CANAL_OK, NOME, { canal }, null, relogio.evento());
}

function listar() {
  return proto.empacotar(proto.LISTA_CANAIS, NOME, null, dados.canais, relogio.evento());
}

async function publicar(msg) {
  const canal = (msg.dados.canal || "").trim();
  const texto = msg.dados.texto || "";
  if (!canal) return proto.empacotar(proto.PUB_ERRO, NOME, { motivo: "canal vazio" }, null, relogio.evento());
  if (!dados.canais.includes(canal)) dados.canais.push(canal);
  const corpo = proto.empacotar(proto.PUBLICAR, msg.origem, { canal, texto }, null, relogio.evento());
  await pub.send([canal, corpo]);
  const ts = horaLocal();
  dados.publicacoes.push({ canal, texto, origem: msg.origem, timestamp: ts });
  salvar(dados);
  await replicar("publicacao", { canal, texto, quem: msg.origem, timestamp: String(ts) });
  return proto.empacotar(proto.PUB_OK, NOME, { canal }, null, relogio.evento());
}

async function main() {
  dados = carregar();

  const socket = new zmq.Reply();
  await socket.connect(BROKER_ADDR);
  pub.connect(PROXY_PUB);
  ref.connect(REF_ADDR);

  await sleep(1000);

  await pedirRank();
  servicoCoordenador();
  escutarPubsub();
  await sleep(2000);
  await eleger();
  console.log(`${NOME} rodando | rank=${rank} | coordenador=${coordenador} | canais=${JSON.stringify(dados.canais)}`);

  for await (const [raw] of socket) {
    const msg = proto.desempacotar(raw);
    relogio.receber(msg.contador);
    console.log(`${NOME} recebeu: ${proto.formatar(msg)}`);

    let resp;
    if (msg.tipo === proto.LOGIN) resp = await login(msg);
    else if (msg.tipo === proto.CRIAR_CANAL) resp = await criarCanal(msg);
    else if (msg.tipo === proto.LISTAR_CANAIS) resp = listar();
    else if (msg.tipo === proto.PUBLICAR) resp = await publicar(msg);
    else resp = proto.empacotar(proto.LOGIN_ERRO, NOME, { motivo: "tipo invalido" }, null, relogio.evento());

    console.log(`${NOME} respondeu: ${proto.formatar(proto.desempacotar(resp))}`);
    await socket.send(resp);

    contadorMsg += 1;
    if (contadorMsg % 10 === 0) await enviarHeartbeat();
    if (contadorMsg % 15 === 0) {
      try {
        await sincronizarRelogio();
      } catch (e) {
        await eleger();
      }
    }
  }
}

main();