FROM python:3.12-slim

# Installation des dépendances système (nécessaires pour psycopg2)
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copie et installation des requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie du pipeline industrialisé
COPY src/pipeline/*.py   src/pipeline/
COPY src/pipeline/common src/pipeline/common
COPY src/pipeline/config src/pipeline/config
COPY superset/superset_views.sql superset/
COPY orchestrateur.py .

# Configuration par défaut
ENV PYTHONUNBUFFERED=1

# L'orchestrateur est le point d'entrée par défaut
ENTRYPOINT ["python", "orchestrateur.py"]
