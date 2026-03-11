# ============================================================
# PANEL ADMIN — ÉTAPE 3c : IMPUTATION DES SALAIRES (SILVER)
# ============================================================
# Lit tous les fichiers Silver (avec GRADE de 03b), impute les
# salaires manquants par cascade :
#
#   1. LOCF par cle_unique  — dernier salaire connu de l'individu
#                             dans le même organisme, trié chronologiquement
#   2. Médiane L1 : CODE_EMPLOI × GRADE × CODE_ORGANISME × mois_annee
#   3. Médiane L2 : CODE_EMPLOI × GRADE × mois_annee
#   4. Médiane L3 : GRADE × CODE_ORGANISME × mois_annee
#   5. Médiane L4 : GRADE × mois_annee
#
# Prérequis :
#   - 03_bronze_to_silver.py : filtre activité déjà appliqué
#     (seuls les individus en activité sont dans le Silver)
#   - 03b_imputer_grades_silver.py : colonne GRADE présente
#
# Les médianes sont calculées sur les valeurs OBSERVÉES uniquement.
# Le LOCF s'appuie sur l'historique complet (toutes années).
#
# Colonnes ajoutées :
#   montant_brut_SOURCE — OBSERVE | LOCF | IMPUTE_L1..L4 | NON_IMPUTABLE
#   montant_net_SOURCE  — idem
#
# Source  : s3://silver/panel_admin/YYYY.parquet
# Sortie  : mise à jour en place des mêmes fichiers
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
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "datalab-team")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio-datalabteam123")

BUCKET_SILVER = os.getenv("BUCKET_SILVER", "silver")
PREFIX_SILVER = "panel_admin"

# Nombre minimum d'observations par cellule pour une médiane fiable
MIN_OBS_IMPUTATION = int(os.getenv("MIN_OBS_IMPUTATION", "5"))

# Cascade de clusters médiane (du plus fin au plus large)
NIVEAUX_IMPUTATION = [
    ("L1", ["CODE_EMPLOI", "GRADE", "CODE_ORGANISME", "mois_annee"]),
    ("L2", ["CODE_EMPLOI", "GRADE", "mois_annee"]),
    ("L3", ["GRADE", "CODE_ORGANISME", "mois_annee"]),
    ("L4", ["GRADE", "mois_annee"]),
]

# --- CLIENT S3 ---
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
# FONCTIONS UTILITAIRES
# ============================================================

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
    print(f"  ✓ s3://{bucket}/{key}  ({len(df):,} lignes × {len(df.columns)} cols  {taille:.1f} MB)")


# ============================================================
# ÉTAPE 1 : LISTER LES FICHIERS SILVER
# ============================================================

print("=" * 70)
print("IMPUTATION DES SALAIRES — Silver")
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
# ÉTAPE 2 : CHARGEMENT COMPLET
# ============================================================
# Le LOCF nécessite l'historique complet trié chronologiquement :
# un individu absent en janvier 2020 mais présent en décembre 2019
# doit récupérer le salaire de décembre 2019.

print("1. Chargement complet du Silver...")

frames_all = []
for cle in cles_silver:
    annee = re.search(r"20\d{2}", os.path.basename(cle))
    annee = annee.group() if annee else os.path.basename(cle)
    print(f"   [{annee}] Lecture...", end=" ", flush=True)
    df = lire_parquet_s3(BUCKET_SILVER, cle)
    frames_all.append(df)
    print(f"✓ {len(df):,} lignes")
    del df

panel = pl.concat(frames_all, how="diagonal")
del frames_all
gc.collect()
print(f"   ✓ Total : {len(panel):,} lignes × {len(panel.columns)} colonnes\n")

n_total = len(panel)

# ============================================================
# ÉTAPE 3 : STATS INITIALES + MARQUAGE DES NULLS ORIGINAUX
# ============================================================

print("2. Taux de valeurs manquantes initiaux...")

for col in ["montant_brut", "montant_net"]:
    if col in panel.columns:
        n_null = int(panel[col].is_null().sum())
        print(f"   {col:<15} : {n_null:,} nulls  ({100*n_null/n_total:.1f}%)")
print()

# Mémoriser les nulls originaux AVANT toute imputation
panel = panel.with_columns([
    pl.col("montant_brut").is_null().alias("_brut_null")
    if "montant_brut" in panel.columns
    else pl.lit(False).alias("_brut_null"),

    pl.col("montant_net").is_null().alias("_net_null")
    if "montant_net" in panel.columns
    else pl.lit(False).alias("_net_null"),
])

# ============================================================
# ÉTAPE 4 : CALCUL DES MÉDIANES (valeurs observées uniquement)
# ============================================================

print("3. Calcul des médianes par cluster (valeurs observées uniquement)...")

mediane_tables: dict[str, dict] = {"montant_brut": {}, "montant_net": {}}

for col, flag in [("montant_brut", "_brut_null"), ("montant_net", "_net_null")]:
    if col not in panel.columns:
        continue
    df_obs = panel.filter(~pl.col(flag))
    for niveau, cluster_raw in NIVEAUX_IMPUTATION:
        cluster = [c for c in cluster_raw if c in panel.columns]
        if not cluster:
            continue
        med = (
            df_obs
            .group_by(cluster)
            .agg([
                pl.col(col).median().alias("_med"),
                pl.len().alias("_n"),
            ])
            .filter(pl.col("_n") >= MIN_OBS_IMPUTATION)
            .drop("_n")
        )
        mediane_tables[col][niveau] = med
        print(f"   {col} {niveau} : {len(med):,} cellules")
    del df_obs

gc.collect()
print()

# ============================================================
# ÉTAPE 5 : LOCF PAR cle_unique
# ============================================================
# Clé LOCF : cle_unique (matricule || CODE_ORGANISME) si disponible,
# sinon matricule seul.
# Tri chronologique : YYYYMM construit depuis mois_annee (MMYYYY).

print("4. LOCF par cle_unique (tri chronologique)...")

locf_key = "cle_unique" if "cle_unique" in panel.columns else "matricule"

if locf_key not in panel.columns:
    print("   ⚠️  Ni cle_unique ni matricule disponibles — LOCF ignoré")
else:
    panel = panel.with_columns(
        (pl.col("mois_annee").str.slice(2, 4) + pl.col("mois_annee").str.slice(0, 2))
        .alias("_periode_sort")
    )
    panel = panel.sort([locf_key, "_periode_sort"], nulls_last=True)

    for col, flag in [("montant_brut", "_brut_null"), ("montant_net", "_net_null")]:
        if col not in panel.columns:
            continue
        panel = panel.with_columns(
            pl.col(col).forward_fill().over(locf_key).alias(col)
        )
        n_locf = int(panel.filter(pl.col(flag) & pl.col(col).is_not_null()).select(pl.len()).item())
        print(f"   {col:<15} : {n_locf:,} valeurs imputées par LOCF")

    panel = panel.drop("_periode_sort")

print()

# ============================================================
# ÉTAPE 6 : MARQUER LES VALEURS LOCF AVANT LA CASCADE MÉDIANE
# ============================================================
# On mémorise ce qui a été rempli par LOCF pour distinguer
# LOCF de IMPUTE_Ln dans la colonne SOURCE finale.

panel = panel.with_columns([
    (pl.col("_brut_null") & pl.col("montant_brut").is_not_null()).alias("_brut_locf")
    if "montant_brut" in panel.columns
    else pl.lit(False).alias("_brut_locf"),

    (pl.col("_net_null") & pl.col("montant_net").is_not_null()).alias("_net_locf")
    if "montant_net" in panel.columns
    else pl.lit(False).alias("_net_locf"),
])

# ============================================================
# ÉTAPE 7 : MÉDIANE CASCADE POUR LES NULLS RÉSIDUELS
# ============================================================

print("5. Médiane cascade pour les nulls résiduels...")

for col, flag in [("montant_brut", "_brut_null"), ("montant_net", "_net_null")]:
    if col not in panel.columns:
        continue
    for niveau, cluster_raw in NIVEAUX_IMPUTATION:
        cluster = [c for c in cluster_raw if c in panel.columns]
        if not cluster or niveau not in mediane_tables[col]:
            continue
        med = mediane_tables[col][niveau]
        panel = panel.join(med, on=cluster, how="left")
        n_avant = int(panel.filter(pl.col(col).is_null()).select(pl.len()).item())
        panel = panel.with_columns(
            pl.when(pl.col(col).is_null() & pl.col("_med").is_not_null())
            .then(pl.col("_med"))
            .otherwise(pl.col(col))
            .alias(col)
        ).drop("_med")
        n_apres = int(panel.filter(pl.col(col).is_null()).select(pl.len()).item())
        n_imp = n_avant - n_apres
        if n_imp > 0:
            print(f"   {col} {niveau} : {n_imp:,} valeurs imputées")

print()

# ============================================================
# ÉTAPE 8 : COLONNES SOURCE
# ============================================================
# OBSERVE        : valeur originale non nulle
# LOCF           : imputé par le dernier salaire individuel
# IMPUTE_L1..L4  : imputé par médiane de cluster (niveau n)
# NON_IMPUTABLE  : encore null après toute la cascade

print("6. Calcul des colonnes SOURCE...")

for col, flag, flag_locf in [
    ("montant_brut", "_brut_null", "_brut_locf"),
    ("montant_net",  "_net_null",  "_net_locf"),
]:
    if col not in panel.columns:
        continue
    panel = panel.with_columns(
        pl.when(~pl.col(flag))
        .then(pl.lit("OBSERVE"))
        .when(pl.col(flag_locf))
        .then(pl.lit("LOCF"))
        .when(pl.col(col).is_not_null())
        .then(pl.lit("IMPUTE"))
        .otherwise(pl.lit("NON_IMPUTABLE"))
        .alias(f"{col}_SOURCE")
    )

panel = panel.drop(["_brut_null", "_net_null", "_brut_locf", "_net_locf"])

# ============================================================
# ÉTAPE 9 : BILAN GLOBAL
# ============================================================

print()
print("=" * 70)
print("BILAN IMPUTATION SALAIRES")
print("=" * 70)
print()

for col in ["montant_brut", "montant_net"]:
    source_col = f"{col}_SOURCE"
    if source_col not in panel.columns:
        continue
    print(f"  {col} :")
    for src in ["OBSERVE", "LOCF", "IMPUTE", "NON_IMPUTABLE"]:
        n = int((panel[source_col] == src).sum())
        if n > 0:
            print(f"    {src:<16} : {n:,}  ({100*n/n_total:.1f}%)")
print()

# ============================================================
# ÉTAPE 10 : RÉÉCRITURE SILVER ANNÉE PAR ANNÉE
# ============================================================

print("7. Réécriture Silver par année...")
print()

panel = panel.with_columns(
    pl.col("mois_annee").str.slice(2, 4).alias("_annee_str")
)

sort_cols = [c for c in ["matricule", "mois_annee"] if c in panel.columns]

for cle in cles_silver:
    annee = re.search(r"20\d{2}", os.path.basename(cle))
    annee = annee.group() if annee else os.path.basename(cle)

    df_annee = (
        panel
        .filter(pl.col("_annee_str") == annee)
        .drop("_annee_str")
        .sort(sort_cols, nulls_last=True)
    )

    ecrire_parquet_s3(df_annee, BUCKET_SILVER, cle)
    del df_annee
    gc.collect()

# ============================================================
# RÉSUMÉ FINAL
# ============================================================

print()
print("=" * 70)
print("✓ IMPUTATION SALAIRES SILVER TERMINÉE")
print("=" * 70)
print()
print("Colonnes ajoutées dans tous les fichiers Silver :")
print("  montant_brut_SOURCE — OBSERVE | LOCF | IMPUTE | NON_IMPUTABLE")
print("  montant_net_SOURCE  — OBSERVE | LOCF | IMPUTE | NON_IMPUTABLE")
print()
print("Prochaine étape : exécuter 04_validation_silver.py")
