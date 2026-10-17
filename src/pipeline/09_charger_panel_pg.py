# ============================================================
# PANEL ADMIN — ÉTAPE 9 : CHARGEMENT PANEL → POSTGRESQL
# ============================================================
# Charge la base Gold enrichie par les statuts barème dans la table
# `panel` de PostgreSQL (datalab_db).
#
# Prérequis : 07a_imputer_hors_bareme.py doit avoir été exécuté
#   (s3://gold/panel_admin/base_avec_statut_corrigee.parquet doit exister).
#
# Ce script :
#   1. Télécharge la base Gold enrichie depuis MinIO (boto3)
#   2. Le lit avec Polars
#   3. Charge une table staging PostgreSQL
#   4. Vérifie le nombre de lignes puis remplace la table finale en transaction
#
# Apres cette etape :
#   python src/pipeline/10_validate_superset_views.py
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
from common.s3_client import create_s3_client, get_required_env, load_pipeline_env
from common.logging_config import setup_pipeline_logging
import polars as pl
import psycopg2
from psycopg2 import sql

load_pipeline_env()

# ============================================================
# CONFIGURATION
# ============================================================

BUCKET_GOLD      = os.getenv("BUCKET_GOLD", "gold")

PG_HOST     = get_required_env("PG_HOST")
PG_PORT     = os.getenv("PG_PORT", "5432")
PG_DB       = get_required_env("PG_DB")
PG_USER     = get_required_env("PG_USER")
PG_PASSWORD = get_required_env("PG_PASSWORD")
PG_SCHEMA   = os.getenv("PG_SCHEMA", "public")
PG_TABLE    = os.getenv("PG_TABLE", "panel")
PG_STAGING_TABLE = os.getenv("PG_STAGING_TABLE", f"{PG_TABLE}__staging")

# Vérification que les variables critiques sont bien définies
_manquantes = [k for k, v in {
    "PG_HOST": PG_HOST, "PG_DB": PG_DB, "PG_USER": PG_USER, "PG_PASSWORD": PG_PASSWORD,
}.items() if not v]

# ============================================================
# CLIENT S3
# ============================================================

s3 = create_s3_client()
logger, _ = setup_pipeline_logging("09_charger_panel_pg")

if _manquantes:
    logger.error(f"Variables d'environnement manquantes : {_manquantes} — vérifier le fichier .env")
    sys.exit(1)

SOURCE_CANDIDATES = [
    ("panel_admin/base_avec_statut_corrigee.parquet", "base avec statuts corriges"),
    ("panel_admin/base_avec_statut.parquet", "base avec statuts non corrigee"),
]

# ============================================================
# CHARGEMENT
# ============================================================

logger.info("=" * 70)
logger.info("CHARGEMENT PANEL → POSTGRESQL")
logger.info("=" * 70)
logger.info(f"Sources candidates : {', '.join(key for key, _ in SOURCE_CANDIDATES)}")
logger.info(f"Cible   : {PG_HOST}:{PG_PORT}/{PG_DB}.{PG_SCHEMA}.{PG_TABLE}")

# --- Étape 1 : téléchargement depuis MinIO ---
logger.info("[1/4] Téléchargement depuis MinIO...")
buf = io.BytesIO()
source_key = None
source_label = None
last_error = None
for key, label in SOURCE_CANDIDATES:
    try:
        buf = io.BytesIO()
        s3.download_fileobj(BUCKET_GOLD, key, buf)
        source_key = key
        source_label = label
        break
    except Exception as exc:
        last_error = exc

if source_key is None:
    logger.error(f"      ✗ Aucune source Gold disponible : {last_error}")
    sys.exit(1)

buf.seek(0)
taille_mb = len(buf.getvalue()) / 1024**2
logger.info(f"      ✓  {taille_mb:.1f} MB téléchargés depuis s3://{BUCKET_GOLD}/{source_key} ({source_label})")

# --- Étape 2 : lecture Polars ---
logger.info("[2/4] Lecture Polars...")
df = pl.read_parquet(buf)
logger.info(f"      ✓  {len(df):,} lignes × {len(df.columns)} colonnes")
for col_requise in ["Statut", "Statut_Total", "montant_brut_sans_ind_sans_alloc", "montant_brut_total"]:
    statut = "✓" if col_requise in df.columns else "✗ ABSENT"
    logger.info(f"      {col_requise:<35} : {statut}")

cols_manquantes = [
    c for c in ["Statut", "Statut_Total", "montant_brut_sans_ind_sans_alloc", "montant_brut_total"]
    if c not in df.columns
]
if cols_manquantes:
    logger.error(f"Colonnes requises absentes pour Superset : {cols_manquantes}")
    sys.exit(1)

# --- Étape 3 : chargement PostgreSQL ---
logger.info("[3/4] Connexion à PostgreSQL...")
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
    logger.info("      ✓ Connexion établie")
except Exception as e:
    logger.error(f"      ✗ Impossible de se connecter à PostgreSQL : {e}")
    sys.exit(1)

logger.info(f"[4/4] Chargement dans {PG_SCHEMA}.{PG_STAGING_TABLE}, puis swap transactionnel...")
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
        pg_type = TYPE_MAP.get(dtype, "TEXT")
        col_defs.append(
            sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(pg_type))
        )

    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(PG_SCHEMA)))
    cur.execute(
        sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
            sql.Identifier(PG_SCHEMA),
            sql.Identifier(PG_STAGING_TABLE),
        )
    )
    cur.execute(
        sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(PG_SCHEMA),
            sql.Identifier(PG_STAGING_TABLE),
            sql.SQL(", ").join(col_defs),
        )
    )

    # COPY via buffer CSV en mémoire
    csv_buf = io.StringIO()
    df.write_csv(csv_buf, null_value="")
    csv_buf.seek(0)
    colonnes = sql.SQL(", ").join(sql.Identifier(name) for name in df.columns)
    cur.copy_expert(
        sql.SQL("COPY {}.{} ({}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')")
        .format(
            sql.Identifier(PG_SCHEMA),
            sql.Identifier(PG_STAGING_TABLE),
            colonnes,
        )
        .as_string(conn),
        csv_buf,
    )

    cur.execute(
        sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
            sql.Identifier(PG_SCHEMA),
            sql.Identifier(PG_STAGING_TABLE),
        )
    )
    nb_staging = cur.fetchone()[0]
    if nb_staging != len(df):
        raise RuntimeError(
            f"Contrôle staging échoué : {nb_staging:,} lignes PostgreSQL vs {len(df):,} lignes Parquet"
        )

    # CASCADE : les vues Superset dépendent de la table ; l'étape 10 les recrée
    cur.execute(
        sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
            sql.Identifier(PG_SCHEMA),
            sql.Identifier(PG_TABLE),
        )
    )
    cur.execute(
        sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
            sql.Identifier(PG_SCHEMA),
            sql.Identifier(PG_STAGING_TABLE),
            sql.Identifier(PG_TABLE),
        )
    )

    conn.commit()
    logger.info("      ✓ Chargement et swap réussis")

except Exception as e:
    conn.rollback()
    logger.error(f"      ✗ Erreur lors du chargement : {e}")
    cur.close()
    conn.close()
    sys.exit(1)

# --- Vérification ---
cur.execute(
    sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
        sql.Identifier(PG_SCHEMA),
        sql.Identifier(PG_TABLE),
    )
)
nb_lignes = cur.fetchone()[0]

cur.execute(
    sql.SQL(
        "SELECT MIN(annee || '-' || LPAD(mois::TEXT,2,'0')), "
        "       MAX(annee || '-' || LPAD(mois::TEXT,2,'0')) "
        "FROM {}.{}"
    ).format(
        sql.Identifier(PG_SCHEMA),
        sql.Identifier(PG_TABLE),
    )
)
periode = cur.fetchone()

cur.close()
conn.close()

logger.info("=" * 70)
logger.info("✓ CHARGEMENT TERMINÉ")
logger.info("=" * 70)
logger.info(f"  Lignes chargées : {nb_lignes:,}")
if periode:
    logger.info(f"  Période         : {periode[0]} → {periode[1]}")
logger.info("Prochaine etape : python src/pipeline/10_validate_superset_views.py")
