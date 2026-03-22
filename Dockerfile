FROM python:3.11-slim

WORKDIR /app

# Dépendances système minimales pour psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
 && rm -rf /var/lib/apt/lists/*

# Dépendances Python
COPY src/version_python_dp_etoile/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Scripts du pipeline
COPY src/version_python_dp_etoile/0[0-9]*.py   .
COPY src/version_python_dp_etoile/0[0-9][a-z]*.py .

CMD ["python"]
