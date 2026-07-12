FROM python:3.11-slim
WORKDIR /app
RUN apt-get update \
 && apt-get install -y --no-install-recommends libssl3 libsasl2-2 libzstd1 netcat-openbsd \
 && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "main.py"]
