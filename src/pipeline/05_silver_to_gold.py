# ============================================================
# PANEL ADMIN — ÉTAPE 5 : SILVER → GOLD PARQUET
# ============================================================
# Produit les tables Gold prêtes pour les tableaux de bord
# et les exports finaux à partir de la table Silver.
#
# Tables produites :
#   s3://gold/panel_admin/masse_salariale.parquet
#     → Masse salariale brute et nette par période × organisme
#
#   s3://gold/panel_admin/effectifs.parquet
#     → Effectifs et agents uniques par période
#
# Exports CSV (pour consultation externe) :
#   s3://staging/panel_admin/exports_gold/
#
# Source  : s3://silver/panel_admin/YYYY.parquet (un par année)
#
# Architecture : Polars uniquement, lecture année par année.
#                Les agrégations Gold sont légères (quelques centaines
#                de lignes par année) — concat final sans risque mémoire.
#
# Dépendances :
#   pip install polars boto3 python-dotenv
# ============================================================

import gc
import io
import os
import re
from common.s3_client import create_s3_client
from common.logging_config import setup_pipeline_logging
from common.polars_s3 import lire_parquet_s3, ecrire_parquet_s3
import polars as pl

# --- CONFIGURATION ---

BUCKET_SILVER  = os.getenv("BUCKET_SILVER",  "silver")
BUCKET_GOLD    = os.getenv("BUCKET_GOLD",    "gold")
BUCKET_STAGING = os.getenv("BUCKET_STAGING", "staging")
PREFIX_SILVER  = "panel_admin"
KEY_GOLD_MS    = "panel_admin/masse_salariale.parquet"
KEY_GOLD_EFF   = "panel_admin/effectifs.parquet"
PREFIX_EXPORTS = "panel_admin/exports_gold"


# --- CLIENT S3 (MinIO) ---
s3 = create_s3_client()
logger, _ = setup_pipeline_logging("05_silver_to_gold")


def exporter_csv_staging(df: pl.DataFrame, nom_fichier: str) -> None:
    csv_bytes = df.write_csv().encode("utf-8")
    s3.put_object(
        Bucket=BUCKET_STAGING,
        Key=f"{PREFIX_EXPORTS}/{nom_fichier}",
        Body=csv_bytes,
    )
    logger.info(f"✓ {nom_fichier}  ({len(df):,} lignes)")


# ============================================================
# LISTER LES FICHIERS SILVER
# ============================================================

logger.info("=" * 70)
logger.info("SILVER → GOLD : Panel Administratif")
logger.info("=" * 70)

pages_silver = s3.get_paginator("list_objects_v2").paginate(
    Bucket=BUCKET_SILVER, Prefix=PREFIX_SILVER
)
cles_silver = sorted([
    obj["Key"]
    for page in pages_silver
    for obj in page.get("Contents", [])
    if obj["Key"].endswith(".parquet")
])

logger.info(f"Fichiers Silver trouvés : {len(cles_silver)}")

# ============================================================
# BOUCLE PAR ANNÉE : agrégations Gold
# ============================================================
# Les résultats agrégés (quelques dizaines de lignes par année)
# sont accumulés dans des listes — pas de risque mémoire.

frames_ms  = []   # masse salariale par année
frames_eff = []   # effectifs par année

for cle in cles_silver:
    annee = re.search(r"20\d{2}", os.path.basename(cle))
    annee = annee.group() if annee else os.path.splitext(os.path.basename(cle))[0]

    df = lire_parquet_s3(BUCKET_SILVER, cle)
    logger.info(f"[{annee}] ✓ {len(df):,} lignes")

    # --- GOLD 1 : Masse salariale ---
    # Les nulls résiduels (NON_IMPUTABLE) sont ignorés nativement
    # par Polars dans sum/mean — pas besoin de filtre explicite.
    if "montant_brut" in df.columns and "montant_net" in df.columns:
        ms = (
            df
            .with_columns([
                pl.col("mois_annee").str.slice(2, 4).alias("annee"),
                pl.col("mois_annee").str.slice(0, 2).alias("mois"),
                pl.col("CODE_ORGANISME").fill_null("NON_CODE").alias("code_organisme"),
                pl.col("organisme").fill_null("NON_IDENTIFIE").alias("organisme"),
            ])
            .group_by([
                "mois_annee", "annee", "mois",
                "code_organisme", "organisme",
                "situation_normalisee",
            ])
            .agg([
                pl.len().alias("nb_lignes"),
                pl.col("matricule").n_unique().alias("nb_agents_uniques"),
                pl.col("montant_brut").sum().round(0).alias("masse_brute"),
                pl.col("montant_net").sum().round(0).alias("masse_nette"),
                pl.col("montant_brut").mean().round(0).alias("salaire_brut_moyen"),
                pl.col("montant_net").mean().round(0).alias("salaire_net_moyen"),
                pl.col("montant_brut").quantile(0.5, interpolation="nearest").round(0).alias("salaire_brut_mediane"),
                pl.col("montant_net").quantile(0.5, interpolation="nearest").round(0).alias("salaire_net_mediane"),
            ])
            .sort(["mois_annee", "organisme"])
        )
        frames_ms.append(ms)
        logger.info(f"  Masse salariale : {len(ms):,} lignes agrégées")

    # --- GOLD 2 : Effectifs ---
    eff = (
        df
        .with_columns([
            pl.col("mois_annee").str.slice(2, 4).alias("annee"),
            pl.col("mois_annee").str.slice(0, 2).alias("mois"),
            pl.col("CODE_ORGANISME").fill_null("NON_CODE").alias("code_organisme"),
        ])
        .group_by([
            "mois_annee", "annee", "mois",
            "situation_normalisee", "code_organisme",
        ])
        .agg([
            pl.len().alias("nb_lignes"),
            pl.col("matricule").n_unique().alias("nb_agents_uniques"),
            (pl.col("sexe") == "1").sum().alias("nb_hommes"),
            (pl.col("sexe") == "2").sum().alias("nb_femmes"),
        ])
        .sort(["mois_annee", "situation_normalisee"])
    )
    frames_eff.append(eff)
    logger.info(f"  Effectifs       : {len(eff):,} lignes agrégées")

    del df
    gc.collect()

# ============================================================
# CONCAT FINAL ET ÉCRITURE GOLD
# ============================================================

logger.info("=" * 70)
logger.info("ÉCRITURE GOLD")
logger.info("=" * 70)

# Masse salariale
df_masse_salariale = (
    pl.concat(frames_ms, how="diagonal")
    .sort(["mois_annee", "organisme"])
)
ecrire_parquet_s3(df_masse_salariale, BUCKET_GOLD, KEY_GOLD_MS)

# Effectifs
df_effectifs = (
    pl.concat(frames_eff, how="diagonal")
    .sort(["mois_annee", "situation_normalisee"])
)
ecrire_parquet_s3(df_effectifs, BUCKET_GOLD, KEY_GOLD_EFF)

# ============================================================
# EXPORT CSV VERS STAGING
# ============================================================

logger.info("=" * 70)
logger.info("EXPORT CSV vers staging")
logger.info("=" * 70)

exporter_csv_staging(df_masse_salariale, "masse_salariale_par_periode_organisme.csv")
exporter_csv_staging(df_effectifs,       "effectifs_par_periode.csv")

# ============================================================
# RÉSUMÉ
# ============================================================

logger.info("=" * 70)
logger.info("RÉSUMÉ GOLD")
logger.info("=" * 70)
logger.info(f"Années traitées          : {len(cles_silver)}")
logger.info(f"Masse salariale          : {len(df_masse_salariale):,} lignes")
logger.info(f"Effectifs                : {len(df_effectifs):,} lignes")
logger.info(f"Exports CSV : s3://{BUCKET_STAGING}/{PREFIX_EXPORTS}/")
logger.info("=" * 70)
logger.info("✓ PIPELINE PANEL ADMIN COMPLET")
logger.info("=" * 70)
logger.info(f"Tables Gold disponibles :")
logger.info(f"  s3://{BUCKET_GOLD}/{KEY_GOLD_MS}")
logger.info(f"  s3://{BUCKET_GOLD}/{KEY_GOLD_EFF}")
logger.info("Prochaine étape : exécuter 06_compiler_panel.py")
