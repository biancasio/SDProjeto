const zmq = require("zeromq");

// broker: repassa mensagens entre clientes (5555) e servidor (5556)

async function main() {
  const clientSocket = new zmq.Router();
  await clientSocket.bind("tcp://*:5555");

  const serverSocket = new zmq.Dealer();
  await serverSocket.bind("tcp://*:5556");

  console.log("broker rodando (5555 clientes / 5556 servidor)");

  (async () => {
    for await (const frames of clientSocket) {
      await serverSocket.send(frames);
    }
  })();

  (async () => {
    for await (const frames of serverSocket) {
      await clientSocket.send(frames);
    }
  })();
}

main();
