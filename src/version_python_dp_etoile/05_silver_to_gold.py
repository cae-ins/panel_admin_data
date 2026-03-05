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
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import polars as pl

load_dotenv(".env")

# --- CONFIGURATION ---

# LORSQU'ON TRAVAILLE DEPUIS SA MACHINE LOCAL
MINIO_ENDPOINT   = "http://192.168.1.230:30137"
MINIO_ACCESS_KEY = "datalab-team"
MINIO_SECRET_KEY = "minio-datalabteam123"

# LORSQU'ON TRAVAILLE SUR JHUB
# MINIO_ENDPOINT   = "http://minio.mon-namespace.svc.cluster.local:80"
# MINIO_ACCESS_KEY = "datalab-team"
# MINIO_SECRET_KEY = "minio-datalabteam123"

BUCKET_SILVER  = "silver"
BUCKET_GOLD    = "gold"
BUCKET_STAGING = "staging"
PREFIX_SILVER  = "panel_admin"
KEY_GOLD_MS    = "panel_admin/masse_salariale.parquet"
KEY_GOLD_EFF   = "panel_admin/effectifs.parquet"
PREFIX_EXPORTS = "panel_admin/exports_gold"

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


def lire_parquet_s3(bucket: str, key: str) -> pl.DataFrame:
    buf = io.BytesIO()
    s3.download_fileobj(bucket, key, buf)
    buf.seek(0)
    return pl.read_parquet(buf)


def ecrire_parquet_s3(df: pl.DataFrame, bucket: str, key: str) -> None:
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)
    taille = len(buf.getvalue()) / 1024**2
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    print(f"  ✓ s3://{bucket}/{key}  ({len(df):,} lignes  {taille:.1f} MB)")


def exporter_csv_staging(df: pl.DataFrame, nom_fichier: str) -> None:
    csv_bytes = df.write_csv().encode("utf-8")
    s3.put_object(
        Bucket=BUCKET_STAGING,
        Key=f"{PREFIX_EXPORTS}/{nom_fichier}",
        Body=csv_bytes,
    )
    print(f"  ✓ {nom_fichier}  ({len(df):,} lignes)")


# ============================================================
# LISTER LES FICHIERS SILVER
# ============================================================

print("=" * 70)
print("SILVER → GOLD : Panel Administratif")
print("=" * 70)
print()

pages_silver = s3.get_paginator("list_objects_v2").paginate(
    Bucket=BUCKET_SILVER, Prefix=PREFIX_SILVER
)
cles_silver = sorted([
    obj["Key"]
    for page in pages_silver
    for obj in page.get("Contents", [])
    if obj["Key"].endswith(".parquet")
])

print(f"Fichiers Silver trouvés : {len(cles_silver)}\n")

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

    print(f"[{annee}] Lecture...", end=" ", flush=True)
    df = lire_parquet_s3(BUCKET_SILVER, cle)
    print(f"✓  {len(df):,} lignes", flush=True)

    # --- GOLD 1 : Masse salariale ---
    if "montant_brut" in df.columns and "montant_net" in df.columns:
        ms = (
            df
            .filter(
                pl.col("montant_brut").is_not_null()
                & pl.col("montant_net").is_not_null()
            )
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
        print(f"  Masse salariale : {len(ms):,} lignes agrégées", flush=True)

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
    print(f"  Effectifs       : {len(eff):,} lignes agrégées", flush=True)

    del df
    gc.collect()

# ============================================================
# CONCAT FINAL ET ÉCRITURE GOLD
# ============================================================

print()
print("=" * 70)
print("ÉCRITURE GOLD")
print("=" * 70)
print()

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

print()
print("=" * 70)
print("EXPORT CSV vers staging")
print("=" * 70)
print()

exporter_csv_staging(df_masse_salariale, "masse_salariale_par_periode_organisme.csv")
exporter_csv_staging(df_effectifs,       "effectifs_par_periode.csv")

# ============================================================
# RÉSUMÉ
# ============================================================

print()
print("=" * 70)
print("RÉSUMÉ GOLD")
print("=" * 70)
print()
print(f"Années traitées          : {len(cles_silver)}")
print(f"Masse salariale          : {len(df_masse_salariale):,} lignes")
print(f"Effectifs                : {len(df_effectifs):,} lignes")
print(f"\nExports CSV : s3://{BUCKET_STAGING}/{PREFIX_EXPORTS}/")
print()
print("=" * 70)
print("✓ PIPELINE PANEL ADMIN COMPLET")
print("=" * 70)
print("\nTables Gold disponibles :")
print(f"  s3://{BUCKET_GOLD}/{KEY_GOLD_MS}")
print(f"  s3://{BUCKET_GOLD}/{KEY_GOLD_EFF}")
print("\nProchaine étape : exécuter 06_compiler_panel.py")