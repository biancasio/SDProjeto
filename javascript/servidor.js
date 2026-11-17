const fs = require("fs");
const path = require("path");
const zmq = require("zeromq");
const proto = require("./protocolo");

const NOME = process.env.NOME_SERVIDOR || "servidor-js";
const DATA_FILE = process.env.DATA_FILE || "/data/dados.json";
const BROKER_ADDR = process.env.BROKER_ADDR || "tcp://broker-js:5556";
const PROXY_PUB = process.env.PROXY_PUB || "tcp://proxy-py:5557";
const PROXY_SUB = process.env.PROXY_SUB || "tcp://proxy-py:5558";
const REF_ADDR = process.env.REF_ADDR || "tcp://referencia-py:5560";
const PORTA_COORD = process.env.PORTA_COORD || "5561";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const relogio = new proto.Relogio();
let rank = null;
let coordenador = null;
let contadorMsg = 0;
let horaOffset = 0;
let eleicaoPendente = false; // marcado quando recebo pedido de eleicao de rank menor

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
}

// --- chamada REQ para outro servidor (porta 5561) com timeout ---
async function chamarServidor(nomeDestino, tipo, dadosMsg = null) {
  const s = new zmq.Request();
  s.receiveTimeout = 2000;
  s.sendTimeout = 2000;
  s.linger = 0;
  s.connect(`tcp://${nomeDestino}:${PORTA_COORD}`);
  try {
    await s.send(proto.empacotar(tipo, NOME, dadosMsg, null, relogio.evento()));
    const [raw] = await s.receive();
    const resp = proto.desempacotar(raw);
    relogio.receber(resp.contador);
    return resp;
  } finally {
    try { s.close(); } catch (_) {}
  }
}

async function virarCoordenador() {
  coordenador = NOME;
  const corpo = proto.empacotar(proto.COORDENADOR, NOME, { coordenador: NOME }, null, relogio.evento());
  await pub.send([proto.TOPICO_SERVERS, corpo]);
  console.log(`${NOME} se anunciou como coordenador`);
}

// --- eleicao por bullying (algoritmo do valentao) ---
// pede eleicao a todos os servidores de rank MAIOR. Se algum responde OK, ele
// assume a eleicao e este servidor espera o anuncio. Se nenhum responde (estao
// mortos), este servidor vira o coordenador e avisa pelo topico 'servers'.
async function eleger() {
  eleicaoPendente = false;
  console.log(`${NOME} iniciou eleicao (rank ${rank})`);
  const servidores = await listarServidores();
  const maiores = servidores.filter(([n, r]) => r > rank && n !== NOME).map(([n]) => n);

  let recebeuOk = false;
  for (const nomeMaior of maiores) {
    try {
      const resp = await chamarServidor(nomeMaior, proto.ELEICAO, { nome: NOME });
      if (resp.tipo === proto.ELEICAO_OK) recebeuOk = true;
    } catch (e) {
      // servidor de rank maior nao respondeu: esta morto, ignora
    }
  }

  if (recebeuOk) {
    console.log(`${NOME} recebeu OK de rank maior, aguardando anuncio do coordenador`);
    coordenador = null;
  } else {
    await virarCoordenador();
  }
}

// --- sincronizacao de relogio (Berkeley) ---
async function sincronizarRelogio() {
  if (coordenador === NOME || coordenador === null) return;
  const resp = await chamarServidor(coordenador, proto.RELOGIO);
  const horaCoord = Number(resp.dados.hora || proto.agoraMs());
  horaOffset = horaCoord - proto.agoraMs();
  console.log(`${NOME} sincronizou com ${coordenador}: hora=${horaCoord} (offset=${horaOffset}ms)`);
}

// --- socket REP entre servidores (relogio + eleicao) ---
async function servicoCoordenador() {
  const s = new zmq.Reply();
  await s.bind(`tcp://*:${PORTA_COORD}`);
  for await (const [raw] of s) {
    const msg = proto.desempacotar(raw);
    relogio.receber(msg.contador);
    let resp;
    if (msg.tipo === proto.RELOGIO) {
      if (coordenador === NOME)
        resp = proto.empacotar(proto.RELOGIO_OK, NOME, { hora: String(horaLocal()) }, null, relogio.evento());
      else
        resp = proto.empacotar(proto.RELOGIO_OK, NOME, { motivo: "nao_coordenador" }, null, relogio.evento());
    } else if (msg.tipo === proto.ELEICAO) {
      // recebi pedido de eleicao de rank menor: respondo OK e marco que
      // preciso rodar minha propria eleicao (por ter rank maior)
      console.log(`${NOME} recebeu pedido de eleicao de ${msg.origem}`);
      eleicaoPendente = true;
      resp = proto.empacotar(proto.ELEICAO_OK, NOME, null, null, relogio.evento());
    } else {
      resp = proto.empacotar(proto.RELOGIO_OK, NOME, { motivo: "tipo invalido" }, null, relogio.evento());
    }
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

    // se recebi um pedido de eleicao de rank menor, rodo minha propria eleicao
    if (eleicaoPendente) await eleger();

    contadorMsg += 1;
    if (contadorMsg % 10 === 0) await enviarHeartbeat();
    if (contadorMsg % 15 === 0) {
      try {
        await sincronizarRelogio();
      } catch (e) {
        // o coordenador atual falhou: dispara nova eleicao
        console.log(`${NOME}: coordenador ${coordenador} indisponivel, pedindo eleicao`);
        await eleger();
      }
    }
  }
}

main();
