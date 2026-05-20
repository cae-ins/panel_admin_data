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
from common.s3_client import create_s3_client
from common.logging_config import setup_pipeline_logging

# --- CONFIGURATION ---

# ============================================================
# <<< À MODIFIER : chemins locaux vers vos fichiers sources >>>
DOSSIER_EXCEL          = "consolidation_solde_2015_2025/01_data_sources/fichiers_solde_mensuels"
FICHIER_ANSTAT         = "consolidation_solde_2015_2025/01_data_sources/fichiers_anstat_codes/FICHIER_ANSTAT_CODE_2025.xlsx"
FICHIER_CORRESPONDANCE = "consolidation_solde_2015_2025/01_data_sources/fichiers_anstat_codes/CORRESPONDANCE_GRILLE_EMPLOI.xlsx"
# <<<----------->>>
# ============================================================

BUCKET          = os.getenv("BUCKET_STAGING", "staging")
PREFIX_MENSUELS = "panel_admin/fichiers_mensuels"
PREFIX_REFS     = "panel_admin/references"

# --- CLIENT S3 (MinIO) ---
s3 = create_s3_client()
logger, _ = setup_pipeline_logging("00_upload_staging")

# --- CRÉATION DES BUCKETS SI NÉCESSAIRES ---
for bucket in ["staging", "bronze", "silver", "gold"]:
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)
        logger.info(f"Bucket créé : {bucket}")

# --- UPLOAD DES FICHIERS MENSUELS ---
logger.info("=" * 60)
logger.info("UPLOAD : Fichiers mensuels Excel")
logger.info("=" * 60)

fichiers_excel = sorted(
    glob.glob(os.path.join(DOSSIER_EXCEL, "**", "*.xlsx"), recursive=True)
)

logger.info(f"Fichiers trouvés : {len(fichiers_excel)}")

compteur_ok  = 0
compteur_err = 0

for fichier in fichiers_excel:
    nom_fichier = os.path.basename(fichier)
    objet_dest  = f"{PREFIX_MENSUELS}/{nom_fichier}"
    try:
        s3.upload_file(fichier, BUCKET, objet_dest)
        logger.info(f"Upload ✓ : {nom_fichier}")
        compteur_ok += 1
    except Exception as e:
        logger.error(f"Upload ✗ : {nom_fichier} — {e}")
        compteur_err += 1

logger.info(f"✓ {compteur_ok} fichiers uploadés, {compteur_err} erreurs")

# --- UPLOAD DU FICHIER DE RÉFÉRENCE ANSTAT ---
logger.info("=" * 60)
logger.info("UPLOAD : Fichier de référence ANSTAT")
logger.info("=" * 60)

fichier_anstat = os.path.expanduser(FICHIER_ANSTAT)
if os.path.exists(fichier_anstat):
    nom_anstat = os.path.basename(fichier_anstat)
    objet_ref  = f"{PREFIX_REFS}/{nom_anstat}"
    try:
        s3.upload_file(fichier_anstat, BUCKET, objet_ref)
        logger.info(f"✓ {nom_anstat} → s3://{BUCKET}/{objet_ref}")
    except Exception as e:
        logger.error(f"✗ Erreur upload ANSTAT : {e}")
else:
    logger.error(f"✗ Fichier non trouvé : {fichier_anstat}")

# --- UPLOAD DE LA TABLE DE CORRESPONDANCE EMPLOI → BARÈME ---
logger.info("=" * 60)
logger.info("UPLOAD : Table de correspondance EMPLOI → BARÈME")
logger.info("=" * 60)

fichier_corr = os.path.expanduser(FICHIER_CORRESPONDANCE)
if os.path.exists(fichier_corr):
    nom_corr  = os.path.basename(fichier_corr)
    objet_corr = f"{PREFIX_REFS}/{nom_corr}"
    try:
        s3.upload_file(fichier_corr, BUCKET, objet_corr)
        logger.info(f"✓ {nom_corr} → s3://{BUCKET}/{objet_corr}")
    except Exception as e:
        logger.error(f"✗ Erreur upload correspondance : {e}")
else:
    logger.error(f"✗ Fichier non trouvé : {fichier_corr}")
    logger.info(f"  → Placez CORRESPONDANCE_GRILLE_EMPLOI.xlsx dans : {os.path.dirname(fichier_corr)}")

logger.info("=" * 60)
logger.info("✓ UPLOAD TERMINÉ")
logger.info("=" * 60)
logger.info(f"Fichiers disponibles dans s3://{BUCKET}/{PREFIX_MENSUELS}/ et s3://{BUCKET}/{PREFIX_REFS}/")
logger.info("Prochaine étape : exécuter 01_pre_analyse.py")
