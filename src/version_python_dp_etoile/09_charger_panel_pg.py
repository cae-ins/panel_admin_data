# ============================================================
# PANEL ADMIN — ÉTAPE 9 : CHARGEMENT PANEL → POSTGRESQL
# ============================================================
# Charge le fichier Gold panel_complet.parquet dans la table
# `panel` de PostgreSQL (datalab_db).
#
# Prérequis : 06_compiler_panel.py doit avoir été exécuté
#   (s3://gold/panel_admin/panel_complet.parquet doit exister).
#
# Ce script :
#   1. Télécharge panel_complet.parquet depuis MinIO (boto3)
#   2. Le lit avec Polars
#   3. Recrée la table `panel` dans PostgreSQL (DROP + COPY)
#   4. Vérifie le nombre de lignes chargées
#
# Après cette étape :
#   bash src/version_python_dp_etoile/10_refresh_superset.sh
#
# Variables d'environnement requises (.env) :
#   MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, BUCKET_GOLD
#   PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
#
# Dépendances :
#   pip install polars boto3 psycopg2-binary python-dotenv
# ============================================================

import io
import os
import sys
import csv
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import polars as pl
import psycopg2

load_dotenv(".env")

# ============================================================
# CONFIGURATION
# ============================================================

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_GOLD      = os.getenv("BUCKET_GOLD", "gold")

PG_HOST     = os.getenv("PG_HOST")
PG_PORT     = os.getenv("PG_PORT", "5432")
PG_DB       = os.getenv("PG_DB")
PG_USER     = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# Vérification que les variables critiques sont bien définies
_manquantes = [k for k, v in {
    "MINIO_ENDPOINT": MINIO_ENDPOINT, "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY, "PG_HOST": PG_HOST,
    "PG_DB": PG_DB, "PG_USER": PG_USER, "PG_PASSWORD": PG_PASSWORD,
}.items() if not v]
if _manquantes:
    print(f"✗ Variables d'environnement manquantes : {_manquantes}")
    print("  → Vérifier le fichier .env")
    sys.exit(1)

KEY_PANEL_COMPLET = "panel_admin/panel_complet.parquet"
PG_TABLE          = "panel"

# ============================================================
# CLIENT S3
# ============================================================

s3 = boto3.client(
    "s3",
    endpoint_url          = MINIO_ENDPOINT,
    aws_access_key_id     = MINIO_ACCESS_KEY,
    aws_secret_access_key = MINIO_SECRET_KEY,
    config                = Config(signature_version="s3v4"),
    region_name           = "us-east-1",
    verify                = False,
)

# ============================================================
# CHARGEMENT
# ============================================================

print("=" * 70)
print("CHARGEMENT PANEL → POSTGRESQL")
print("=" * 70)
print()
print(f"Source  : s3://{BUCKET_GOLD}/{KEY_PANEL_COMPLET}")
print(f"Cible   : {PG_HOST}:{PG_PORT}/{PG_DB}.{PG_TABLE}")
print()

# --- Étape 1 : téléchargement depuis MinIO ---
print("[1/4] Téléchargement depuis MinIO...")
buf = io.BytesIO()
s3.download_fileobj(BUCKET_GOLD, KEY_PANEL_COMPLET, buf)
buf.seek(0)
taille_mb = len(buf.getvalue()) / 1024**2
print(f"      ✓  {taille_mb:.1f} MB téléchargés")

# --- Étape 2 : lecture Polars ---
print("[2/4] Lecture Polars...")
df = pl.read_parquet(buf)
print(f"      ✓  {len(df):,} lignes × {len(df.columns)} colonnes")

# --- Étape 3 : chargement PostgreSQL ---
print("[3/4] Connexion à PostgreSQL...")
try:
    conn = psycopg2.connect(
        host     = PG_HOST,
        port     = int(PG_PORT),
        dbname   = PG_DB,
        user     = PG_USER,
        password = PG_PASSWORD,
    )
    conn.autocommit = False
    cur = conn.cursor()
    print("      ✓")
except Exception as e:
    print(f"      ✗ Impossible de se connecter à PostgreSQL : {e}")
    sys.exit(1)

print(f"[4/4] Chargement dans {PG_TABLE} (DROP + CREATE + COPY)...")
try:
    # Mapping types Polars → PostgreSQL
    TYPE_MAP = {
        pl.Int8: "SMALLINT", pl.Int16: "SMALLINT", pl.Int32: "INTEGER",
        pl.Int64: "BIGINT",  pl.UInt8: "SMALLINT", pl.UInt16: "INTEGER",
        pl.UInt32: "BIGINT", pl.UInt64: "BIGINT",
        pl.Float32: "REAL",  pl.Float64: "DOUBLE PRECISION",
        pl.Boolean: "BOOLEAN", pl.Date: "DATE", pl.Datetime: "TIMESTAMP",
        pl.Utf8: "TEXT", pl.String: "TEXT",
    }

    col_defs = []
    for name, dtype in zip(df.columns, df.dtypes):
        pg_type = TYPE_MAP.get(type(dtype), "TEXT")
        col_defs.append(f'"{name}" {pg_type}')

    cur.execute(f'DROP TABLE IF EXISTS {PG_TABLE};')
    cur.execute(f'CREATE TABLE {PG_TABLE} ({", ".join(col_defs)});')

    # COPY via buffer CSV en mémoire
    csv_buf = io.StringIO()
    df.write_csv(csv_buf, null_value="")
    csv_buf.seek(0)
    cur.copy_expert(
        f"COPY {PG_TABLE} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')",
        csv_buf,
    )

    conn.commit()
    print("      ✓")

except Exception as e:
    conn.rollback()
    print(f"      ✗ Erreur lors du chargement : {e}")
    cur.close()
    conn.close()
    sys.exit(1)

# --- Vérification ---
cur.execute(f"SELECT COUNT(*) FROM {PG_TABLE};")
nb_lignes = cur.fetchone()[0]

cur.execute(
    f"SELECT MIN(annee || '-' || LPAD(mois::TEXT,2,'0')), "
    f"       MAX(annee || '-' || LPAD(mois::TEXT,2,'0')) "
    f"FROM {PG_TABLE};"
)
periode = cur.fetchone()

cur.close()
conn.close()

print()
print("=" * 70)
print("✓ CHARGEMENT TERMINÉ")
print("=" * 70)
print(f"  Lignes chargées : {nb_lignes:,}")
if periode:
    print(f"  Période         : {periode[0]} → {periode[1]}")
print()
print("Prochaine étape : bash src/version_python_dp_etoile/10_refresh_superset.sh")
print()
