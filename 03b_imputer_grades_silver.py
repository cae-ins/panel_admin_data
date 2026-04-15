# ============================================================
# PANEL ADMIN — ÉTAPE 3b : IMPUTATION DES GRADES (SILVER)
# ============================================================
# Lit tous les fichiers Silver, impute les grades manquants
# ou invalides, puis réécrit les fichiers Silver avec les
# colonnes GRADE et GRADE_SOURCE.
#
# Pourquoi sur Silver (et non Gold) ?
#   Le Silver est la couche apurée : toutes les corrections
#   de données doivent y figurer avant toute agrégation Gold.
#
# Méthode : cascade emploi sur 3 niveaux de priorité
#   P1 : grade modal de l'emploi pour ce mois précis
#   P2 : grade modal de l'emploi pour cette année
#   P3 : grade modal de l'emploi sur tout l'historique
#
# Les lookups sont construits sur l'intégralité du Silver
# (toutes années) pour que P3 soit fiable sur les emplois rares.
#
# Colonnes ajoutées au Silver :
#   GRADE        (Utf8) — ex. "A3", "B2", "NF"
#   GRADE_SOURCE (Utf8) — "OBSERVE" | "IMPUTE" | "NF_NON_IMPUTABLE"
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

GRADES_VALIDES = (
    [f"A{i}" for i in range(1, 8)] +
    [f"B{i}" for i in range(1, 7)] +
    [f"C{i}" for i in range(1, 6)] +
    [f"D{i}" for i in range(1, 4)]
)
GRADES_INVALIDES = ["B7", "C7", "D7", "A", "B", "C", "D"]

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


def extraire_grade(statut: str | None) -> str:
    """Extrait le grade (ex: A3, B2) depuis statut_fonctionnaire."""
    if not statut:
        return "NF"
    m = re.search(r"Cat[eé]gorie\s+([ABCD][0-9]*)", str(statut), re.IGNORECASE)
    if m:
        return m.group(1).upper().strip()
    return "NF"


# ============================================================
# ÉTAPE 1 : LISTER LES FICHIERS SILVER
# ============================================================

print("=" * 70)
print("IMPUTATION DES GRADES — Silver")
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
# ÉTAPE 2 : CHARGEMENT COMPLET POUR CONSTRUIRE LES LOOKUPS
# ============================================================
# P3 (emploi global) nécessite l'intégralité de l'historique.
# On charge tout le Silver en mémoire pour construire les
# lookups, puis on libère avant la boucle d'écriture.

print("1. Chargement complet du Silver pour construction des lookups...")

frames_all = []
for cle in cles_silver:
    annee = re.search(r"20\d{2}", os.path.basename(cle))
    annee = annee.group() if annee else os.path.basename(cle)
    print(f"   [{annee}] Lecture...", end=" ", flush=True)
    df = lire_parquet_s3(BUCKET_SILVER, cle)
    # On ne garde que les colonnes nécessaires pour les lookups
    cols_lookup = [c for c in ["statut_fonctionnaire", "emploi", "mois_annee"]
                   if c in df.columns]
    frames_all.append(df.select(cols_lookup))
    print(f"✓ {len(df):,} lignes")
    del df

panel_all = pl.concat(frames_all, how="diagonal")
del frames_all
gc.collect()
print(f"   ✓ Total : {len(panel_all):,} lignes\n")

# ============================================================
# ÉTAPE 3 : EXTRACTION DU GRADE BRUT ET STATS INITIALES
# ============================================================

print("2. Extraction du grade depuis statut_fonctionnaire...")

if "statut_fonctionnaire" not in panel_all.columns:
    print("   ⚠️  Colonne statut_fonctionnaire absente — aucune imputation possible")
    lk_p1 = lk_p2 = lk_p3 = None
else:
    panel_all = panel_all.with_columns(
        pl.col("statut_fonctionnaire")
        .map_elements(extraire_grade, return_dtype=pl.Utf8)
        .alias("GRADE")
    )

    n_total     = len(panel_all)
    n_valides   = int(panel_all["GRADE"].is_in(GRADES_VALIDES).sum())
    n_invalides = int(panel_all["GRADE"].is_in(GRADES_INVALIDES).sum())
    n_nf        = int((panel_all["GRADE"] == "NF").sum())

    print(f"   Grades valides     : {n_valides:,}  ({100*n_valides/n_total:.1f}%)")
    print(f"   Grades invalides   : {n_invalides:,}  ({100*n_invalides/n_total:.1f}%)")
    print(f"   Non fonctionnaires : {n_nf:,}  ({100*n_nf/n_total:.1f}%)\n")

    # ============================================================
    # ÉTAPE 4 : CONSTRUCTION DES LOOKUPS (toutes années)
    # ============================================================

    has_emploi = "emploi" in panel_all.columns
    lk_p1 = lk_p2 = lk_p3 = None

    if n_invalides > 0 and has_emploi:
        print("3. Construction des lookups grade modal (toutes années)...")

        panel_all = panel_all.with_columns([
            pl.col("mois_annee").str.slice(2, 4).cast(pl.Int32, strict=False).alias("_ANNEE"),
            pl.col("mois_annee").str.slice(0, 2).cast(pl.Int32, strict=False).alias("_MOIS_NUM"),
        ])

        base_valides = panel_all.filter(pl.col("GRADE").is_in(GRADES_VALIDES))

        lk_p1 = (
            base_valides
            .group_by(["emploi", "_ANNEE", "_MOIS_NUM"])
            .agg(pl.col("GRADE").mode().first().alias("GRADE_P1"))
        )
        print(f"   ✓ P1 (emploi × année × mois) : {len(lk_p1):,} combinaisons")

        lk_p2 = (
            base_valides
            .group_by(["emploi", "_ANNEE"])
            .agg(pl.col("GRADE").mode().first().alias("GRADE_P2"))
        )
        print(f"   ✓ P2 (emploi × année)         : {len(lk_p2):,} combinaisons")

        lk_p3 = (
            base_valides
            .group_by(["emploi"])
            .agg(pl.col("GRADE").mode().first().alias("GRADE_P3"))
        )
        print(f"   ✓ P3 (emploi — global)        : {len(lk_p3):,} emplois couverts")

        del base_valides

    elif n_invalides > 0 and not has_emploi:
        print("3. ⚠️  Colonne emploi absente — imputation impossible")
    else:
        print("3. ✓ Aucun grade invalide détecté — lookups non nécessaires")

del panel_all
gc.collect()
print()

# ============================================================
# ÉTAPE 5 : APPLICATION ANNÉE PAR ANNÉE + RÉÉCRITURE SILVER
# ============================================================

print("4. Application de l'imputation et réécriture Silver...")
print()

stats_globales = {"OBSERVE": 0, "IMPUTE": 0, "NF_NON_IMPUTABLE": 0}

for cle in cles_silver:
    annee = re.search(r"20\d{2}", os.path.basename(cle))
    annee = annee.group() if annee else os.path.basename(cle)

    print(f"  [{annee}] Lecture...", end=" ", flush=True)
    df = lire_parquet_s3(BUCKET_SILVER, cle)
    print(f"✓ {len(df):,} lignes")

    # Supprimer d'éventuelles anciennes colonnes GRADE / GRADE_SOURCE
    df = df.select([c for c in df.columns if c not in ("GRADE", "GRADE_SOURCE")])

    # Extraction grade brut
    if "statut_fonctionnaire" in df.columns:
        df = df.with_columns(
            pl.col("statut_fonctionnaire")
            .map_elements(extraire_grade, return_dtype=pl.Utf8)
            .alias("GRADE")
        )
    else:
        df = df.with_columns(pl.lit("NF").alias("GRADE"))

    # Marquer les lignes qui nécessitent une imputation
    df = df.with_columns(
        pl.col("GRADE").is_in(GRADES_INVALIDES).alias("_grade_invalide")
    )

    # Application de la cascade si lookups disponibles
    if lk_p1 is not None and "emploi" in df.columns:
        df = df.with_columns([
            pl.col("mois_annee").str.slice(2, 4).cast(pl.Int32, strict=False).alias("_ANNEE"),
            pl.col("mois_annee").str.slice(0, 2).cast(pl.Int32, strict=False).alias("_MOIS_NUM"),
        ])

        df = df.join(lk_p1, on=["emploi", "_ANNEE", "_MOIS_NUM"], how="left")
        df = df.join(lk_p2, on=["emploi", "_ANNEE"],              how="left")
        df = df.join(lk_p3, on=["emploi"],                        how="left")

        # Remplacement des grades invalides par cascade P1 → P2 → P3
        df = df.with_columns(
            pl.when(pl.col("_grade_invalide"))
            .then(
                pl.when(pl.col("GRADE_P1").is_not_null()).then(pl.col("GRADE_P1"))
                .when(pl.col("GRADE_P2").is_not_null()).then(pl.col("GRADE_P2"))
                .when(pl.col("GRADE_P3").is_not_null()).then(pl.col("GRADE_P3"))
                .otherwise(pl.col("GRADE"))
            )
            .otherwise(pl.col("GRADE"))
            .alias("GRADE")
        ).drop(["GRADE_P1", "GRADE_P2", "GRADE_P3", "_ANNEE", "_MOIS_NUM"])

    # GRADE_SOURCE : basé sur l'état initial (_grade_invalide) et le résultat final
    df = df.with_columns(
        pl.when(~pl.col("_grade_invalide") & pl.col("GRADE").is_in(GRADES_VALIDES))
        .then(pl.lit("OBSERVE"))
        .when(pl.col("_grade_invalide") & pl.col("GRADE").is_in(GRADES_VALIDES))
        .then(pl.lit("IMPUTE"))
        .otherwise(pl.lit("NF_NON_IMPUTABLE"))
        .alias("GRADE_SOURCE")
    ).drop("_grade_invalide")

    # Réordonnancement : GRADE et GRADE_SOURCE après statut_fonctionnaire
    cols = df.columns
    if "statut_fonctionnaire" in cols:
        idx  = cols.index("statut_fonctionnaire")
        cols = [c for c in cols if c not in ("GRADE", "GRADE_SOURCE")]
        cols = cols[:idx + 1] + ["GRADE", "GRADE_SOURCE"] + cols[idx + 1:]
        df   = df.select(cols)

    # Bilan de l'année
    n_obs = int((df["GRADE_SOURCE"] == "OBSERVE").sum())
    n_imp = int((df["GRADE_SOURCE"] == "IMPUTE").sum())
    n_nfi = int((df["GRADE_SOURCE"] == "NF_NON_IMPUTABLE").sum())
    stats_globales["OBSERVE"]          += n_obs
    stats_globales["IMPUTE"]           += n_imp
    stats_globales["NF_NON_IMPUTABLE"] += n_nfi
    print(f"    OBSERVE={n_obs:,}  IMPUTE={n_imp:,}  NF_NON_IMPUTABLE={n_nfi:,}")

    ecrire_parquet_s3(df, BUCKET_SILVER, cle)
    del df
    gc.collect()

# ============================================================
# RÉSUMÉ FINAL
# ============================================================

n_tot = sum(stats_globales.values())
print()
print("=" * 70)
print("RÉSUMÉ IMPUTATION GRADES — Silver")
print("=" * 70)
print()
for src, n in stats_globales.items():
    pct = 100 * n / n_tot if n_tot > 0 else 0
    print(f"  {src:<20} : {n:,}  ({pct:.1f}%)")
print()
print("=" * 70)
print("✓ IMPUTATION GRADES SILVER TERMINÉE")
print("=" * 70)
print()
print("Colonnes ajoutées dans tous les fichiers Silver :")
print("  GRADE        — grade final (valide ou imputé)")
print("  GRADE_SOURCE — OBSERVE | IMPUTE | NF_NON_IMPUTABLE")
print()
print("Prochaine étape : exécuter 03c_imputer_salaires_silver.py")
