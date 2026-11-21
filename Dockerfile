FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN rm -f /etc/apt/apt.conf.d/docker-clean

RUN apt-get update -y && apt-get install -y --no-install-recommends \
    ca-certificates \
    librdkafka-dev \
    libssl-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY mongocert.pem /usr/local/share/ca-certificates/mongo-ca.crt
RUN update-ca-certificates
COPY requirements.txt .
RUN pip install --no-cache-dir --progress-bar off -r requirements.txt
RUN echo 1

COPY src/ .
