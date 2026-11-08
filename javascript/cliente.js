const zmq = require("zeromq");
const proto = require("./protocolo");

const NOME = process.env.NOME_BOT || "bot-js";
const CANAL_NOVO = process.env.CANAL_NOVO || "geral";
const BROKER_ADDR = process.env.BROKER_ADDR || "tcp://broker-js:5555";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function enviar(socket, raw) {
  console.log(`${NOME} enviou: ${proto.formatar(proto.desempacotar(raw))}`);
  await socket.send(raw);
  const [resp] = await socket.receive();
  const msg = proto.desempacotar(resp);
  console.log(`${NOME} recebeu: ${proto.formatar(msg)}`);
  return msg;
}

async function fazerLogin(socket) {
  while (true) {
    const resp = await enviar(socket, proto.empacotar(proto.LOGIN, NOME, { usuario: NOME }));
    if (resp.tipo === proto.LOGIN_OK) return;
    console.log(`${NOME} login falhou, tentando de novo`);
    await sleep(1000);
  }
}

async function main() {
  const socket = new zmq.Request();
  socket.connect(BROKER_ADDR);

  await sleep(2000);
  console.log(`${NOME} iniciou`);

  await fazerLogin(socket);
  await enviar(socket, proto.empacotar(proto.LISTAR_CANAIS, NOME));
  await enviar(socket, proto.empacotar(proto.CRIAR_CANAL, NOME, { canal: CANAL_NOVO }));
  await enviar(socket, proto.empacotar(proto.LISTAR_CANAIS, NOME));

  console.log(`${NOME} terminou`);
  await new Promise(() => {});
}

main();
