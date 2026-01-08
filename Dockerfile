FROM apache/airflow:2.9.1

# 1) Dépendances système
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2) Dépendances Python
COPY requirements.txt /requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt

# 3) ✅ Playwright: installer le navigateur + deps (Chromium seulement)
# Important: chromium launch échoue si les browsers ne sont pas installés
RUN python -m playwright install --with-deps chromium
