FROM apache/spark:3.5.0
USER root
RUN apt-get update && apt-get install -y python3-pip && pip3 install --no-cache-dir PyYAML==6.0.2 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .