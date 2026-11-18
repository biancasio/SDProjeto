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

const ESPERA_ANUNCIO_MS = 10000;   

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const relogio = new proto.Relogio();
let rank = 0;
let coordenador = null;
const ranks = {};
let horaOffset = 0;
let contadorMsg = 0;
let emEleicao = false;
let anuncioRecebido = true;

const ref = new zmq.Request();
const pub = new zmq.Publisher();
let dados = { logins: [], canais: [], publicacoes: [] };

let filaPub = Promise.resolve();
function publicarFrames(topico, corpo) {
  filaPub = filaPub.then(() => pub.send([topico, corpo])).catch((e) => {
    console.log(`${NOME}: erro ao publicar (${e.message})`);
  });
  return filaPub;
}

let filaRef = Promise.resolve();
function pedirReferencia(tipo, dadosMsg) {
  const p = filaRef.then(async () => {
    await ref.send(proto.empacotar(tipo, NOME, dadosMsg, null, relogio.evento()));
    const [raw] = await ref.receive();
    const resp = proto.desempacotar(raw);
    relogio.receber(resp.contador);
    return resp;
  });
  filaRef = p.catch(() => {});
  return p;
}

const rankDe = (n) => (n === NOME ? rank : (ranks[n] !== undefined ? ranks[n] : -1));

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

async function replicar(acao, info) {
  const corpo = proto.empacotar(proto.REPLICAR, NOME, Object.assign({ acao }, info), null, relogio.evento());
  await publicarFrames(proto.TOPICO_REPLICACAO, corpo);
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

async function pedirRank() {
  const resp = await pedirReferencia(proto.RANK, { nome: NOME });
  rank = Number(resp.dados.rank || 0);
  console.log(`${NOME} recebeu rank ${rank}`);
}

async function listarServidores() {
  const resp = await pedirReferencia(proto.LISTAR_SERVIDORES, null);
  const out = [];
  for (const item of resp.canais || []) {
    const i = item.lastIndexOf(":");
    if (i > 0) {
      const n = item.slice(0, i), r = Number(item.slice(i + 1));
      out.push([n, r]);
      ranks[n] = r;
    }
  }
  return out;
}

async function enviarHeartbeat() {
  await pedirReferencia(proto.HEARTBEAT, { nome: NOME });
}

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

async function anunciar() {
  const corpo = proto.empacotar(proto.COORDENADOR, NOME,
    { coordenador: NOME, rank: String(rank) }, null, relogio.evento());
  await publicarFrames(proto.TOPICO_SERVERS, corpo);
}

async function virarCoordenador() {
  coordenador = NOME;
  horaOffset = 0;
  anuncioRecebido = true;
  console.log(`${NOME} se anunciou como coordenador (rank ${rank})`);
  for (let i = 0; i < 2; i++) {   
    await anunciar();
    if (i === 0) await sleep(1000);
  }
}

async function eleger() {
  if (emEleicao) return;
  emEleicao = true;
  anuncioRecebido = false;
  try {
    console.log(`${NOME} iniciou eleicao (rank ${rank})`);
    const aviso = proto.empacotar(proto.AVISO_ELEICAO, NOME, { iniciador: NOME }, null, relogio.evento());
    await publicarFrames(proto.TOPICO_ELEICOES, aviso);

    const maiores = (await listarServidores()).filter(([n, r]) => r > rank && n !== NOME);
    let recebeuOk = false;
    for (const [n, r] of maiores) {
      try {
        const resp = await chamarServidor(n, proto.ELEICAO, { nome: NOME });
        if (resp.tipo === proto.ELEICAO_OK) {
          recebeuOk = true;
          console.log(`${NOME} recebeu OK de ${n} (rank ${r})`);
        }
      } catch (e) {
        console.log(`${NOME}: ${n} (rank ${r}) nao respondeu`);
      }
    }

    if (recebeuOk) {
      console.log(`${NOME} aguardando anuncio do coordenador`);
      setTimeout(() => {
        if (!anuncioRecebido) {
          console.log(`${NOME}: anuncio nao chegou, refazendo eleicao`);
          eleger();
        }
      }, ESPERA_ANUNCIO_MS);
    } else {
      await virarCoordenador();
    }
  } catch (e) {
    console.log(`${NOME}: erro na eleicao (${e.message})`);
  } finally {
    emEleicao = false;
  }
}

async function sincronizarRelogio() {
  const alvo = coordenador;
  if (alvo === null) { eleger(); return; }
  if (alvo === NOME) return;
  const t0 = proto.agoraMs();
  let resp;
  try {
    resp = await chamarServidor(alvo, proto.RELOGIO);
  } catch (e) {
    console.log(`${NOME}: coordenador ${alvo} indisponivel, iniciando eleicao`);
    eleger();
    return;
  }
  const t1 = proto.agoraMs();
  if (resp.dados.hora === undefined || resp.dados.hora === "") {
    console.log(`${NOME}: ${alvo} nao e o coordenador, iniciando eleicao`);
    eleger();
    return;
  }
  const rtt = t1 - t0;
  horaOffset = (Number(resp.dados.hora) + Math.floor(rtt / 2)) - t1;
  console.log(`${NOME} sincronizou com ${alvo}: rtt=${rtt}ms offset=${horaOffset}ms`);
}

async function servicoCoordenador() {
  const s = new zmq.Reply();
  await s.bind(`tcp://*:${PORTA_COORD}`);
  for await (const [raw] of s) {
    let resp;
    let iniciar = false;
    try {
      const msg = proto.desempacotar(raw);
      relogio.receber(msg.contador);
      if (msg.tipo === proto.RELOGIO) {
        resp = coordenador === NOME
          ? proto.empacotar(proto.RELOGIO_OK, NOME, { hora: String(horaLocal()) }, null, relogio.evento())
          : proto.empacotar(proto.RELOGIO_OK, NOME, { motivo: "nao_coordenador" }, null, relogio.evento());
      } else if (msg.tipo === proto.ELEICAO) {
        // responde OK e, como tenho rank maior, faco a minha eleicao
        console.log(`${NOME} recebeu pedido de eleicao de ${msg.origem}`);
        resp = proto.empacotar(proto.ELEICAO_OK, NOME, null, null, relogio.evento());
        iniciar = true;
      } else {
        resp = proto.empacotar(proto.RELOGIO_OK, NOME, { motivo: "tipo invalido" }, null, relogio.evento());
      }
    } catch (e) {
      console.log(`${NOME}: erro entre servidores (${e.message})`);
      resp = proto.empacotar(proto.RELOGIO_OK, NOME, { motivo: "erro" });
    }
    await s.send(resp);
    if (iniciar) eleger();
  }
}

function tratarAnuncio(msg) {
  const novo = msg.dados.coordenador || "";
  if (!novo || novo === NOME) return;
  let rankNovo = Number(msg.dados.rank);
  if (msg.dados.rank === undefined || msg.dados.rank === "" || isNaN(rankNovo)) rankNovo = rankDe(novo);
  ranks[novo] = rankNovo;
  const atual = coordenador;

  if (atual === null || novo === atual || rankNovo >= rankDe(atual)) {
    if (novo !== atual) console.log(`${NOME} soube que o coordenador agora e ${novo} (rank ${rankNovo})`);
    coordenador = novo;
    anuncioRecebido = true;
  } else if (atual === NOME) {
    anunciar();   
  }
}

async function escutarPubsub() {
  const sub = new zmq.Subscriber();
  sub.connect(PROXY_SUB);
  sub.subscribe(proto.TOPICO_SERVERS);
  sub.subscribe(proto.TOPICO_REPLICACAO);
  sub.subscribe(proto.TOPICO_ELEICOES);
  for await (const [topico, corpo] of sub) {
    try {
      const msg = proto.desempacotar(corpo);
      relogio.receber(msg.contador);
      const t = topico.toString();
      if (t === proto.TOPICO_SERVERS) tratarAnuncio(msg);
      else if (t === proto.TOPICO_REPLICACAO) aplicarReplicacao(msg);
      else if (t === proto.TOPICO_ELEICOES) {
        const iniciador = msg.dados.iniciador || "";
        if (iniciador !== NOME) console.log(`${NOME} soube que ${iniciador} iniciou uma eleicao`);
      }
    } catch (e) {
      console.log(`${NOME}: erro no pub/sub (${e.message})`);
    }
  }
}

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
  await publicarFrames(canal, corpo);
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
  await sleep(3000);
  await eleger();
  console.log(`${NOME} rodando | rank=${rank} | coordenador=${coordenador}`);

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
    if (contadorMsg % 10 === 0) {
      try { await enviarHeartbeat(); } catch (e) { console.log(`${NOME}: heartbeat falhou`); }
    }
    if (contadorMsg % 15 === 0) await sincronizarRelogio();
  }
}

main();