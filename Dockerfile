FROM mcr.microsoft.com/playwright/python:v1.52.0-noble

WORKDIR /app

# Install cron + curl
RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Chromium (playwright already available from base image)
RUN playwright install chromium --with-deps

COPY scraper.py cleaner.py storage.py ./
COPY entrypoint.sh /entrypoint.sh
COPY runner.sh ./

RUN chmod +x runner.sh /entrypoint.sh
RUN mkdir -p /app/logs

ENTRYPOINT ["/entrypoint.sh"]
