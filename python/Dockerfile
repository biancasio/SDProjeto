FROM python:3.13.7-alpine3.21

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install pyzmq protobuf grpcio-tools

COPY mensagem.proto /app/mensagem.proto
RUN python -m grpc_tools.protoc -I/app --python_out=/app /app/mensagem.proto

COPY protocolo.py /app/protocolo.py

CMD ["python", "main.py"]
