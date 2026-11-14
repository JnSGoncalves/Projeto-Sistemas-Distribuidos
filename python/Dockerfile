FROM python:3.12-slim

WORKDIR /app

COPY python/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proto/chat.proto .
RUN python -m grpc_tools.protoc -I. --python_out=. chat.proto

COPY python/cliente.py python/servidor.py .
COPY python/relogio.py python/coordenacao.py .
COPY python/sql/ ./sql/

CMD ["python", "servidor.py"]
