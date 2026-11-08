const zmq = require("zeromq");
const proto = require("./protocolo");

const NOME = process.env.NOME_BOT || "bot-js";
const BROKER_ADDR = process.env.BROKER_ADDR || "tcp://broker-js:5555";
const PROXY_ADDR = process.env.PROXY_ADDR || "tcp://proxy-js:5558";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const socket = new zmq.Request();
const sub = new zmq.Subscriber();

async function enviar(raw) {
  console.log(`${NOME} enviou: ${proto.formatar(proto.desempacotar(raw))}`);
  await socket.send(raw);
  const [resp] = await socket.receive();
  const msg = proto.desempacotar(resp);
  console.log(`${NOME} recebeu: ${proto.formatar(msg)}`);
  return msg;
}

async function fazerLogin() {
  while (true) {
    const resp = await enviar(proto.empacotar(proto.LOGIN, NOME, { usuario: NOME }));
    if (resp.tipo === proto.LOGIN_OK) return;
    console.log(`${NOME} login falhou, tentando de novo`);
    await sleep(1000);
  }
}

async function listarCanais() {
  const resp = await enviar(proto.empacotar(proto.LISTAR_CANAIS, NOME));
  return resp.canais || [];
}

async function receber() {
  for await (const [topico, corpo] of sub) {
    const msg = proto.desempacotar(corpo);
    const recebido = new Date().toISOString().substr(11, 8);
    const enviado = new Date(Number(msg.timestamp)).toISOString().substr(11, 8);
    const canal = msg.dados.canal || "";
    const texto = msg.dados.texto || "";
    console.log(`${NOME} [canal ${canal}] '${texto}' (enviado ${enviado}, recebido ${recebido})`);
  }
}

async function main() {
  socket.connect(BROKER_ADDR);
  sub.connect(PROXY_ADDR);

  await sleep(2000);
  console.log(`${NOME} iniciou`);

  await fazerLogin();

  let canais = await listarCanais();

  if (canais.length < 5) {
    const novo = `canal-${NOME}-${Math.floor(Math.random() * 999) + 1}`;
    await enviar(proto.empacotar(proto.CRIAR_CANAL, NOME, { canal: novo }));
    canais = await listarCanais();
  }

  const inscritos = [];
  while (inscritos.length < 3 && canais.length) {
    const escolhido = canais[Math.floor(Math.random() * canais.length)];
    if (!inscritos.includes(escolhido)) {
      sub.subscribe(escolhido);
      inscritos.push(escolhido);
      console.log(`${NOME} inscrito em: ${escolhido}`);
    }
    if (new Set(inscritos).size >= new Set(canais).size) break;
  }

  receber(); 

  while (true) {
    canais = await listarCanais();
    if (!canais.length) {
      await sleep(1000);
      continue;
    }
    const canal = canais[Math.floor(Math.random() * canais.length)];
    for (let i = 0; i < 10; i++) {
      const texto = `msg ${Math.floor(Math.random() * 1000) + 1}`;
      await enviar(proto.empacotar(proto.PUBLICAR, NOME, { canal, texto }));
      await sleep(1000);
    }
  }
}

main();
