# ============================================================
# PANEL ADMIN — ÉTAPE 7b : STATS ANSTAT SANS INDEMNITÉS
# ============================================================
# Calcule les statistiques barèmes officiels ANSTAT (min, max,
# médiane, moyenne) par cluster (SECTEUR × GRADE) à partir
# du fichier de référence anstat_2025.txt.xlsx, en ne retenant
# que les composantes hors indemnités (Salaire de Base,
# Résidence, Logement, Transport, Allocations 2 enfants).
#
# Source  : s3://staging/panel_admin/references/anstat_2025.txt.xlsx
#
# Sorties :
#   s3://staging/panel_admin/references/
#       anstat_2025_avec_brut_sans_indem.csv   (ANSTAT sans indemnités)
#   s3://staging/panel_admin/exports_gold/
#       anstat_sans_indem_SECTEUR_GRADE.csv    (stats min/max/méd/moy)
#
# Prérequis : le fichier ANSTAT doit avoir été uploadé
#             par l'étape 00_upload_staging.py
#
# Dépendances :
#   pip install polars fastexcel boto3 python-dotenv
# ============================================================

import os
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import polars as pl

# Importation des fonctions métier depuis le module refactorisé
from anstat_2025_sans_indem import (
    charger_anstat_depuis_s3,
    calculer_montant_brut_sans_indem,
    calculer_stats_par_groupes,
    COLONNES_BRUT,
    COLONNES_GROUPAGE,
    CLE_ANSTAT,
)

load_dotenv(".env")

# --- CONFIGURATION ---

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "datalab-team")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio-datalabteam123")

BUCKET_STAGING = os.getenv("BUCKET_STAGING", "staging")
PREFIX_GOLD    = "panel_admin/exports_gold"
PREFIX_REF     = "panel_admin/references"


# --- CLIENT S3 (MinIO) ---
s3 = boto3.client(
    "s3",
    endpoint_url          = MINIO_ENDPOINT,
    aws_access_key_id     = MINIO_ACCESS_KEY,
    aws_secret_access_key = MINIO_SECRET_KEY,
    config                = Config(
        signature_version = "s3v4",
        retries           = {"max_attempts": 5, "mode": "adaptive"},
        connect_timeout   = 30,
        read_timeout      = 120,
    ),
    region_name = "us-east-1",
    verify      = False,
)


# ============================================================
# UTILITAIRES
# ============================================================

def exporter_csv_staging(df: pl.DataFrame, prefix: str, nom_fichier: str) -> None:
    """Exporte un DataFrame en CSV dans le bucket staging."""
    cle_s3 = f"{prefix}/{nom_fichier}"
    csv_bytes = df.write_csv().encode("utf-8")
    s3.put_object(
        Bucket=BUCKET_STAGING,
        Key=cle_s3,
        Body=csv_bytes,
    )
    print(f"  ✓ s3://{BUCKET_STAGING}/{cle_s3}  ({len(df):,} lignes)")


# ============================================================
# PIPELINE
# ============================================================

print()
print("=" * 70)
print("STATS ANSTAT SANS INDEMNITÉS — par cluster (SECTEUR × GRADE)")
print("=" * 70)
print()

# ── 1. Chargement du fichier ANSTAT ────────────────────────

print("1. Chargement du fichier ANSTAT depuis staging...")
df_anstat = charger_anstat_depuis_s3(s3, BUCKET_STAGING, CLE_ANSTAT)

# ── 2. Calcul du montant brut sans indemnités ─────────────

print("\n2. Calcul du montant brut sans indemnités...")
df_anstat = calculer_montant_brut_sans_indem(df_anstat, COLONNES_BRUT)
print(f"  ✓ Colonne ajoutée — {len(df_anstat):,} lignes")

# ── 3. Statistiques par cluster (SECTEUR × GRADE) ─────────

print("\n3. Calcul des statistiques par cluster (SECTEUR × GRADE)...")
df_stats_cluster = calculer_stats_par_groupes(df_anstat, COLONNES_GROUPAGE)
print(f"  ✓ {len(df_stats_cluster)} clusters trouvés")
print(f"\n  Colonnes : {df_stats_cluster.columns}")
print(f"\n  Aperçu :")
print(df_stats_cluster)

# ── 4. Export CSV vers Staging ──────────────────────────────

print()
print("=" * 70)
print("EXPORT CSV vers staging")
print("=" * 70)
print()

# a) ANSTAT enrichi (avec colonne "Montant brut sans indemnités") → references
FICHIER_ANSTAT_SANS_INDEM = "anstat_2025_avec_brut_sans_indem.csv"
exporter_csv_staging(df_anstat, PREFIX_REF, FICHIER_ANSTAT_SANS_INDEM)

# b) Statistiques min/max/méd/moy par cluster → exports_gold
FICHIER_STATS = "anstat_sans_indem_SECTEUR_GRADE.csv"
exporter_csv_staging(df_stats_cluster, PREFIX_GOLD, FICHIER_STATS)


# ── 5. Résumé ───────────────────────────────────────────────

print()
print("=" * 70)
print("RÉSUMÉ")
print("=" * 70)
print()
print(f"Fichier source       : s3://{BUCKET_STAGING}{CLE_ANSTAT}")
print(f"Lignes ANSTAT        : {len(df_anstat):,}")
print(f"Composantes brut     : {', '.join(COLONNES_BRUT)}")
print(f"Cluster (groupage)   : {' × '.join(COLONNES_GROUPAGE)}")
print(f"Nombre de clusters   : {len(df_stats_cluster)}")
print()
print(f"  Statistiques calculées : Minimum, Maximum, Médiane, Moyenne, Nombre")
print()
print(f"  Export 1 (ANSTAT sans indèmnités) : s3://{BUCKET_STAGING}/{PREFIX_REF}/{FICHIER_ANSTAT_SANS_INDEM}")
print(f"  Export 2 (Stats cluster)  : s3://{BUCKET_STAGING}/{PREFIX_GOLD}/{FICHIER_STATS}")
print()
print("=" * 70)
print("✓ STATS ANSTAT SANS INDEMNITÉS TERMINÉ")
print("=" * 70)
print()
print("Prochaine étape : exécuter 08_creation_fichier_excel_avec_sommaire.py")
