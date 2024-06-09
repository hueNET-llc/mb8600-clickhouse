FROM alpine:3.20

COPY . /mb8600-clickhouse

WORKDIR /mb8600-clickhouse

RUN apk update && \
    apk add --no-cache python3 py3-pip tzdata rsync && \
    pip install --no-cache-dir --break-system-packages -r requirements.txt

ENTRYPOINT ["python", "-u", "exporter.py"]