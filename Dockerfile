FROM apache/airflow:2.9.1

USER root

# 1) Dépendances système (ffmpeg + deps Playwright pour Chromium)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ffmpeg \
      libnss3 \
      libnspr4 \
      libatk1.0-0 \
      libatk-bridge2.0-0 \
      libcups2 \
      libdrm2 \
      libxkbcommon0 \
      libxcomposite1 \
      libxdamage1 \
      libxfixes3 \
      libxrandr2 \
      libgbm1 \
      libasound2 \
      libpango-1.0-0 \
      libcairo2 \
      libatspi2.0-0 \
      libx11-6 \
      libx11-xcb1 \
      libxcb1 \
      libxext6 \
      libxss1 \
      libxtst6 \
      ca-certificates \
      fonts-liberation \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2) Dépendances Python
COPY requirements.txt /requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt

# 3) Installer uniquement le browser Playwright (sans deps système)
RUN python -m playwright install chromium
