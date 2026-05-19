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


def construire_lookup_forward_fill(
    df: pl.DataFrame,
    cols_cles: list[str],
    annee_col: str,
    grade_col: str,
    grades_valides: set[str],
) -> pl.DataFrame:
    """
    Construit un lookup de grades valides par individu × année.
    Pour chaque individu, on garde le grade valide et l'année la plus
    récente où il a été observé.

    cols_cles : colonnes identifiant l'individu (ex: cle_unique, emploi, etc.)
    """
    df_valides = df.filter(
        pl.col(grade_col).is_in(grades_valides)
    ).with_columns(
        pl.col(annee_col).cast(pl.Int32, strict=False).alias("_annee_int")
    )

    lookup = (
        df_valides
        .group_by(cols_cles)
        .agg(
            pl.col("_annee_int").max().alias("_annee_future"),
            pl.col(grade_col).first().alias("_grade_future"),
        )
    )

    lookup_grade = (
        df_valides
        .join(lookup, on=cols_cles + ["_annee_int"], how="inner")
        .select(cols_cles + ["_annee_int", grade_col])
        .rename({
            grade_col: "_GRADE_FF",
            "_annee_int": "_annee_source",
        })
    )

    return lookup_grade


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
# L'imputation forward-fill nécessite l'intégralité de l'historique
# pour trouver le grade futur d'un individu.
# On charge tout le Silver en mémoire, puis on libère avant la
# boucle d'écriture.

print("1. Chargement complet du Silver pour construction des lookups...")

frames_all = []
for cle in cles_silver:
    annee = re.search(r"20\d{2}", os.path.basename(cle))
    annee = annee.group() if annee else os.path.basename(cle)
    print(f"   [{annee}] Lecture...", end=" ", flush=True)
    df = lire_parquet_s3(BUCKET_SILVER, cle)
    cols_needed = ["statut_fonctionnaire", "cle_unique", "emploi", "code_organisme", "mois_annee"]
    cols_present = [c for c in cols_needed if c in df.columns]
    frames_all.append(df.select(cols_present))
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
    lk_forward = None
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
    # ÉTAPE 4 : CONSTRUCTION DU LOOKUP FORWARD-FILL
    # ============================================================
    # Pour les grades invalides (ex: B7, C7, D7, ou NF sur années anciennes),
    # on regarde si l'individu a un grade valide dans les années futures.
    # La clé d'identification est : cle_unique + emploi
    # On utilise l'année la plus récente où le grade est valide.

    has_cle_unique = "cle_unique" in panel_all.columns
    has_emploi     = "emploi" in panel_all.columns
    lk_forward = None

    if n_invalides > 0 and has_cle_unique and has_emploi:
        print("3. Construction du lookup forward-fill (grade futur par individu)...")

        panel_all = panel_all.with_columns([
            pl.col("mois_annee").str.slice(2, 4).cast(pl.Int32, strict=False).alias("_annee"),
        ])

        base_valides = panel_all.filter(pl.col("GRADE").is_in(GRADES_VALIDES))

        max_annee = int(base_valides["_annee"].max())
        print(f"   Année max avec grades valides : {max_annee}")

        lk_forward = (
            base_valides
            .group_by(["cle_unique", "emploi"])
            .agg(
                pl.col("GRADE")
                .sort_by("_annee", descending=True)
                .first()
                .alias("_GRADE_FUTUR"),
                pl.col("_annee")
                .max()
                .alias("_annee_source"),
            )
        )
        print(f"   ✓ Lookup forward-fill : {len(lk_forward):,} individus\n")

        del base_valides

    elif n_invalides > 0 and (not has_cle_unique or not has_emploi):
        print("3. ⚠️  Colonnes cle_unique/emploi absentes — forward-fill impossible")
    else:
        print("3. ✓ Aucun grade invalide détecté — lookup non nécessaire")

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

    df = df.select([c for c in df.columns if c not in ("GRADE", "GRADE_SOURCE")])

    if "statut_fonctionnaire" in df.columns:
        df = df.with_columns(
            pl.col("statut_fonctionnaire")
            .map_elements(extraire_grade, return_dtype=pl.Utf8)
            .alias("GRADE")
        )
    else:
        df = df.with_columns(pl.lit("NF").alias("GRADE"))

    df = df.with_columns(
        pl.col("GRADE").is_in(GRADES_INVALIDES).alias("_grade_invalide")
    )

    if lk_forward is not None and "cle_unique" in df.columns and "emploi" in df.columns:
        df = df.join(
            lk_forward,
            on=["cle_unique", "emploi"],
            how="left",
        )

        df = df.with_columns(
            pl.when(pl.col("_grade_invalide") & pl.col("_GRADE_FUTUR").is_not_null())
            .then(pl.col("_GRADE_FUTUR"))
            .when(pl.col("_grade_invalide"))
            .then(pl.col("GRADE"))
            .otherwise(pl.col("GRADE"))
            .alias("GRADE")
        )

        n_ff = int(df.filter(
            pl.col("_grade_invalide") & pl.col("_GRADE_FUTUR").is_not_null()
        ).select(pl.len()).item())
        if n_ff > 0:
            print(f"    Forward-fill appliqué : {n_ff:,} grades")

        df = df.drop(["_GRADE_FUTUR", "_annee_source"])

    df = df.with_columns(
        pl.when(~pl.col("_grade_invalide") & pl.col("GRADE").is_in(GRADES_VALIDES))
        .then(pl.lit("OBSERVE"))
        .when(pl.col("_grade_invalide") & pl.col("GRADE").is_in(GRADES_VALIDES))
        .then(pl.lit("IMPUTE"))
        .otherwise(pl.lit("NF_NON_IMPUTABLE"))
        .alias("GRADE_SOURCE")
    ).drop("_grade_invalide")

    cols = df.columns
    if "statut_fonctionnaire" in cols:
        idx  = cols.index("statut_fonctionnaire")
        cols = [c for c in cols if c not in ("GRADE", "GRADE_SOURCE")]
        cols = cols[:idx + 1] + ["GRADE", "GRADE_SOURCE"] + cols[idx + 1:]
        df   = df.select(cols)

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
