const zmq = require("zeromq");
const proto = require("./protocolo");


const NOME = process.env.NOME_REFERENCIA || "referencia-js";
const PORTA = process.env.PORTA_REFERENCIA || "5560";
const TIMEOUT = 30000; 

const servidores = {};  
const ultimoHb = {};    
let proximoRank = 0;

function darRank(msg) {
  const nome = msg.dados.nome || "";
  if (!(nome in servidores)) {
    servidores[nome] = proximoRank++;
  }
  ultimoHb[nome] = Date.now();
  return proto.empacotar(proto.RANK_OK, NOME, { nome, rank: String(servidores[nome]) });
}

function listar() {
  const itens = Object.entries(servidores).map(([n, r]) => `${n}:${r}`);
  return proto.empacotar(proto.LISTA_SERVIDORES, NOME, null, itens);
}

function heartbeat(msg) {
  const nome = msg.dados.nome || "";
  if (nome in servidores) ultimoHb[nome] = Date.now();
  
  return proto.empacotar(proto.HEARTBEAT_OK, NOME, { ok: "1" });
}


setInterval(() => {
  const agora = Date.now();
  for (const nome of Object.keys(ultimoHb)) {
    if (agora - ultimoHb[nome] > TIMEOUT) {
      delete servidores[nome];
      delete ultimoHb[nome];
      console.log(`${NOME}: servidor ${nome} removido (sem heartbeat)`);
    }
  }
}, 5000);

async function main() {
  const socket = new zmq.Reply();
  await socket.bind(`tcp://*:${PORTA}`);
  console.log(`${NOME} rodando na porta ${PORTA}`);

  for await (const [raw] of socket) {
    const msg = proto.desempacotar(raw);
    let resp;
    if (msg.tipo === proto.RANK) resp = darRank(msg);
    else if (msg.tipo === proto.LISTAR_SERVIDORES) resp = listar();
    else if (msg.tipo === proto.HEARTBEAT) resp = heartbeat(msg);
    else resp = proto.empacotar(proto.RANK_OK, NOME, { motivo: "tipo invalido" });
    console.log(`${NOME} respondeu: ${proto.formatar(proto.desempacotar(resp))}`);
    await socket.send(resp);
  }
}

main();