# ============================================================
# PANEL ADMIN — ÉTAPE 0 : UPLOAD DES SOURCES VERS MINIO STAGING
# ============================================================
# À exécuter UNE SEULE FOIS pour déposer les fichiers sources
# sur MinIO. Ensuite, toute l'équipe travaille depuis staging.
#
# Dépose :
#   - Les ~120 fichiers Excel mensuels (2015-2025)
#     vers : staging/panel_admin/fichiers_mensuels/
#   - Le fichier de référence ANSTAT_CODE
#     vers : staging/panel_admin/references/
#
# Dépendances :
#   pip install boto3 python-dotenv
# ============================================================

import os
import glob
from pathlib import Path
from dotenv import load_dotenv
import boto3
from botocore.client import Config

load_dotenv("template_project/.env")

# --- CONFIGURATION ---

# LORSQU'ON TRAVAILLE DEPUIS SA MACHINE LOCAL
MINIO_ENDPOINT   = "http://192.168.1.230:30137"
MINIO_ACCESS_KEY = "datalab-team"
MINIO_SECRET_KEY = "minio-datalabteam123"

# LORSQU'ON TRAVAILLE SUR JHUB
# MINIO_ENDPOINT   = "http://minio.mon-namespace.svc.cluster.local:80"
# MINIO_ACCESS_KEY = "datalab-team"
# MINIO_SECRET_KEY = "minio-datalabteam123"

# ============================================================
# <<< À MODIFIER : chemins locaux vers vos fichiers sources >>>
DOSSIER_EXCEL  = "consolidation_solde_2015_2025/01_data_sources/fichiers_solde_mensuels"
FICHIER_ANSTAT = "consolidation_solde_2015_2025/01_data_sources/fichiers_anstat_codes/FICHIER_ANSTAT_CODE_2025.xlsx"
# <<<----------->>>
# ============================================================

BUCKET          = "staging"
PREFIX_MENSUELS = "panel_admin/fichiers_mensuels"
PREFIX_REFS     = "panel_admin/references"

# --- CLIENT S3 (MinIO) ---
s3 = boto3.client(
    "s3",
    endpoint_url          = MINIO_ENDPOINT,
    aws_access_key_id     = MINIO_ACCESS_KEY,
    aws_secret_access_key = MINIO_SECRET_KEY,
    config                = Config(signature_version="s3v4"),
    region_name           = "us-east-1",
    verify                = False,
)

# --- UPLOAD DES FICHIERS MENSUELS ---

print("=" * 60)
print("UPLOAD : Fichiers mensuels Excel")
print("=" * 60)
print()

fichiers_excel = sorted(
    glob.glob(os.path.join(DOSSIER_EXCEL, "**", "*.xlsx"), recursive=True)
)
print(f"Fichiers trouvés : {len(fichiers_excel)}\n")

compteur_ok  = 0
compteur_err = 0

for fichier in fichiers_excel:
    nom_fichier = os.path.basename(fichier)
    objet_dest  = f"{PREFIX_MENSUELS}/{nom_fichier}"

    print(f"  Upload : {nom_fichier} ... ", end="", flush=True)
    try:
        s3.upload_file(fichier, BUCKET, objet_dest)
        print("✓")
        compteur_ok += 1
    except Exception as e:
        print(f"✗ ERREUR : {e}")
        compteur_err += 1

print(f"\n✓ {compteur_ok} fichiers uploadés, {compteur_err} erreurs\n")

# --- UPLOAD DU FICHIER DE RÉFÉRENCE ANSTAT ---

print("=" * 60)
print("UPLOAD : Fichier de référence ANSTAT")
print("=" * 60)
print()

fichier_anstat = os.path.expanduser(FICHIER_ANSTAT)

if os.path.exists(fichier_anstat):
    nom_anstat = os.path.basename(fichier_anstat)
    objet_ref  = f"{PREFIX_REFS}/{nom_anstat}"
    try:
        s3.upload_file(fichier_anstat, BUCKET, objet_ref)
        print(f"✓ {nom_anstat} → s3://{BUCKET}/{objet_ref}")
    except Exception as e:
        print(f"✗ ERREUR : {e}")
else:
    print(f"✗ Fichier non trouvé : {fichier_anstat}")

print()
print("=" * 60)
print("✓ UPLOAD TERMINÉ")
print("=" * 60)
print(f"\nVos fichiers sont disponibles sur :")
print(f"  s3://{BUCKET}/{PREFIX_MENSUELS}/")
print(f"  s3://{BUCKET}/{PREFIX_REFS}/")
print("\nProchaine étape : exécuter 01_pre_analyse.py")
