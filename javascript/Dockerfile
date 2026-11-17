# Node.js em Debian slim (evita problemas de compilação nativa do zeromq
# que podem ocorrer no Alpine por causa da musl libc).
FROM node:20-bookworm-slim

WORKDIR /app

# Instala as dependências primeiro (cache de camadas).
COPY package.json ./
RUN npm install --omit=dev

# Código-fonte + schema .proto (lido em runtime pelo protobufjs).
COPY mensagem.proto ./
COPY protocolo.js broker.js servidor.js cliente.js referencia.js ./

# O arquivo a executar (broker/servidor/cliente) é definido pelo docker-compose.
CMD ["node", "broker.js"]
