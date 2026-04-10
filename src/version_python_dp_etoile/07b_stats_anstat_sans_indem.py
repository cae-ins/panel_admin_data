# ============================================================
# PANEL ADMIN — ÉTAPE 7b : STATS ANSTAT SANS INDEMNITÉS
# ============================================================
# Calcule les statistiques barèmes officiels ANSTAT (min, max,
# médiane, moyenne) par SECTEUR, CATÉGORIE et GRADE à partir
# du fichier de référence anstat_2025.txt.xlsx, en ne retenant
# que les composantes hors indemnités (Salaire de Base,
# Résidence, Logement, Transport, Allocations 2 enfants).
#
# Source  : s3://staging/panel_admin/references/anstat_2025.txt.xlsx
#
# Sorties :
#   s3://staging/panel_admin/exports_gold/
#       anstat_sans_indem_SECTEUR.csv
#       anstat_sans_indem_CATEGORIE.csv
#       anstat_sans_indem_GRADE.csv
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

def exporter_csv_staging(df: pl.DataFrame, nom_fichier: str) -> None:
    """Exporte un DataFrame en CSV dans le bucket staging."""
    csv_bytes = df.write_csv().encode("utf-8")
    s3.put_object(
        Bucket=BUCKET_STAGING,
        Key=f"{PREFIX_EXPORTS}/{nom_fichier}",
        Body=csv_bytes,
    )
    print(f"  ✓ {nom_fichier}  ({len(df):,} lignes)")


# ============================================================
# PIPELINE
# ============================================================

print()
print("=" * 70)
print("STATS ANSTAT SANS INDEMNITÉS")
print("=" * 70)
print()

# ── 1. Chargement du fichier ANSTAT ────────────────────────

print("1. Chargement du fichier ANSTAT depuis staging...")
df_anstat = charger_anstat_depuis_s3(s3, BUCKET_STAGING, CLE_ANSTAT)

# ── 2. Calcul du montant brut sans indemnités ─────────────

print("\n2. Calcul du montant brut sans indemnités...")
df_anstat = calculer_montant_brut_sans_indem(df_anstat, COLONNES_BRUT)
print(f"  ✓ Colonne ajoutée — {len(df_anstat):,} lignes")

# ── 3. Statistiques par groupe ─────────────────────────────

print("\n3. Calcul des statistiques par SECTEUR, CATEGORIE, GRADE...")
resultats = calculer_stats_par_groupes(df_anstat, COLONNES_GROUPAGE)

for nom_groupe, df_stats in resultats.items():
    print(f"\n  {nom_groupe} : {len(df_stats)} groupes")

# ── 4. Export CSV vers Staging ──────────────────────────────

print()
print("=" * 70)
print("EXPORT CSV vers staging")
print("=" * 70)
print()

for nom_groupe, df_stats in resultats.items():
    exporter_csv_staging(df_stats, f"anstat_sans_indem_{nom_groupe}.csv")

# ── 5. Résumé ───────────────────────────────────────────────

print()
print("=" * 70)
print("RÉSUMÉ")
print("=" * 70)
print()
print(f"Fichier source       : s3://{BUCKET_STAGING}{CLE_ANSTAT}")
print(f"Lignes ANSTAT        : {len(df_anstat):,}")
print(f"Composantes brut     : {', '.join(COLONNES_BRUT)}")
print(f"Groupes calculés     : {', '.join(COLONNES_GROUPAGE)}")
print()
for nom_groupe, df_stats in resultats.items():
    print(f"  {nom_groupe:15} : {len(df_stats):>4} entrées")
print()
print(f"CSV Staging   : s3://{BUCKET_STAGING}/{PREFIX_EXPORTS}/")
print()
print("=" * 70)
print("✓ STATS ANSTAT SANS INDEMNITÉS TERMINÉ")
print("=" * 70)
print()
print("Prochaine étape : exécuter 08_creation_fichier_excel_avec_sommaire.py")
