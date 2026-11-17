FROM python:3.13.7-alpine3.21

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# pyzmq: troca de mensagens via ZeroMQ
# protobuf: runtime do Protocol Buffers
# grpcio-tools: fornece o compilador protoc (grpc_tools.protoc) para gerar
#               o mensagem_pb2.py a partir do schema .proto
RUN pip install pyzmq protobuf grpcio-tools

# Copia o schema e gera o código Python do Protobuf no build.
COPY mensagem.proto /app/mensagem.proto
RUN python -m grpc_tools.protoc -I/app --python_out=/app /app/mensagem.proto

# O protocolo é comum a broker/servidor/cliente.
COPY protocolo.py /app/protocolo.py

# O arquivo a executar (broker/servidor/cliente) é definido pelo docker-compose.
CMD ["python", "main.py"]
