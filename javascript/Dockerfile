FROM node:20-bookworm-slim

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev

COPY mensagem.proto ./
COPY protocolo.js broker.js servidor.js cliente.js referencia.js ./

CMD ["node", "broker.js"]