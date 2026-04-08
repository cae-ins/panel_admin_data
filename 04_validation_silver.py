# ============================================================
# PANEL ADMIN — ÉTAPE 4 : VALIDATION DE LA TABLE SILVER
# ============================================================
# Contrôles qualité sur les tables Silver Parquet annuelles.
#
# Contrôles :
#   1. Complétude des colonnes clés
#   2. Colonnes avec variations NA suspectes entre années
#   3. Doublons matricule × mois_annee
#   4. Cohérence des montants (net > brut)
#   5. Distribution des situations administratives
#
# Source  : s3://silver/panel_admin/YYYY.parquet (un par année)
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

BUCKET_SILVER  = os.getenv("BUCKET_SILVER",  "silver")
BUCKET_STAGING = os.getenv("BUCKET_STAGING", "staging")
PREFIX_SILVER  = "panel_admin"
PREFIX_VALID   = "panel_admin/validation"

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


def sauvegarder_rapport(df: pl.DataFrame, nom_fichier: str) -> None:
    csv_bytes = df.write_csv().encode("utf-8")
    s3.put_object(
        Bucket=BUCKET_STAGING,
        Key=f"{PREFIX_VALID}/{nom_fichier}",
        Body=csv_bytes,
    )
    print(f"  ✓ Rapport sauvegardé : {nom_fichier}")


# ============================================================
# LISTER LES FICHIERS SILVER
# ============================================================

print("=" * 70)
print("POST-VALIDATION : CONTRÔLE QUALITÉ TABLE SILVER")
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

print(f"Fichiers Silver trouvés : {len(cles_silver)}")
for cle in cles_silver:
    print(f"  · {os.path.basename(cle)}")
print()

# ============================================================
# COLONNES À SURVEILLER
# ============================================================

COLONNES_CLES      = ["matricule", "nom", "montant_brut", "montant_net",
                       "CODE_ORGANISME", "CODE_GRADE", "CODE_EMPLOI",
                       "situation_normalisee", "mois_annee"]
COLONNES_NA_SURVEY = ["matricule", "nom", "montant_brut", "montant_net",
                       "organisme", "grade", "emploi",
                       "lieu_affectation", "service"]

# Accumulateurs inter-années
completude_totale: dict  = {}
na_par_annee_rows: list  = []
doublons_all:      list  = []
incoherences_tot         = 0
nb_total_global          = 0
situations_all:    dict  = {}
matricules_vus:    set   = set()
periodes_vues:     list  = []

# ============================================================
# BOUCLE PAR ANNÉE
# ============================================================

for cle in cles_silver:
    annee = re.search(r"20\d{2}", os.path.basename(cle))
    annee = annee.group() if annee else os.path.splitext(os.path.basename(cle))[0]

    print(f"[{annee}] Lecture...", end=" ", flush=True)
    df       = lire_parquet_s3(BUCKET_SILVER, cle)
    nb       = len(df)
    cols_all = df.columns
    nb_total_global += nb
    print(f"✓  {nb:,} lignes × {len(cols_all)} cols")

    # --- Contrôle 1 : complétude ---
    for col in COLONNES_CLES:
        if col not in cols_all:
            continue
        nn = int(df[col].is_not_null().sum())
        if col not in completude_totale:
            completude_totale[col] = [0, 0]
        completude_totale[col][0] += nn
        completude_totale[col][1] += nb

    # --- Contrôle 2 : NA par année ---
    row = {"annee": annee, "n": nb}
    for col in COLONNES_NA_SURVEY:
        if col in cols_all:
            row[f"{col}_pct_na"] = round(100.0 * df[col].is_null().sum() / nb, 1)
        else:
            row[f"{col}_pct_na"] = None
    na_par_annee_rows.append(row)

    # --- Contrôle 3 : doublons matricule × mois_annee ---
    if "matricule" in cols_all and "mois_annee" in cols_all:
        doublons_annee = (
            df.filter(pl.col("matricule").is_not_null())
            .group_by(["matricule", "mois_annee"])
            .agg(pl.len().alias("n"))
            .filter(pl.col("n") > 1)
        )
        if len(doublons_annee) > 0:
            doublons_all.append(doublons_annee)

    # --- Contrôle 4 : incohérences montants ---
    if "montant_net" in cols_all and "montant_brut" in cols_all:
        incoherences_tot += int(
            df.filter(
                pl.col("montant_net").is_not_null()
                & pl.col("montant_brut").is_not_null()
                & (pl.col("montant_net") > pl.col("montant_brut"))
            ).select(pl.len()).item()
        )

    # --- Contrôle 5 : distribution situations ---
    if "situation_normalisee" in cols_all:
        dist = df.group_by("situation_normalisee").agg(pl.len().alias("nb"))
        for row_s in dist.iter_rows(named=True):
            cat = row_s["situation_normalisee"]
            situations_all[cat] = situations_all.get(cat, 0) + row_s["nb"]

    # Stats résumé
    if "matricule" in cols_all:
        matricules_vus.update(df["matricule"].drop_nulls().to_list())
    if "mois_annee" in cols_all:
        periodes_vues.extend(df["mois_annee"].drop_nulls().unique().to_list())

    del df
    gc.collect()

# ============================================================
# AFFICHAGE DES RÉSULTATS
# ============================================================

print()

# --- Contrôle 1 ---
print("=" * 70)
print("CONTRÔLE 1 : COMPLÉTUDE DES COLONNES CLÉS (toutes années)")
print("=" * 70)
print()
for col, (nn, tot) in sorted(completude_totale.items()):
    pct  = round(100.0 * nn / tot, 1) if tot > 0 else 0.0
    flag = "⚠️ " if pct < 95 else "✓ "
    print(f"  {flag}{col:<30} : {pct:.1f}% complet  ({nn:,}/{tot:,})")
print()

# --- Contrôle 2 ---
print("=" * 70)
print("CONTRÔLE 2 : COLONNES SUSPECTES (NA variable selon l'année)")
print("=" * 70)
print()
na_par_annee       = pl.DataFrame(na_par_annee_rows).sort("annee")
colonnes_suspectes = []
for col in COLONNES_NA_SURVEY:
    col_na = f"{col}_pct_na"
    if col_na in na_par_annee.columns:
        pcts = na_par_annee[col_na].drop_nulls()
        if len(pcts) > 0 and pcts.max() > 95 and pcts.min() < 5:
            colonnes_suspectes.append(col)
            print(f"⚠️  {col} : NA varie de {pcts.min():.1f}% à {pcts.max():.1f}% selon l'année")
if not colonnes_suspectes:
    print("✓ Aucune colonne suspecte détectée")
else:
    sauvegarder_rapport(na_par_annee, "na_par_annee_colonnes_surveillees.csv")
print()

# --- Contrôle 3 ---
print("=" * 70)
print("CONTRÔLE 3 : DOUBLONS MATRICULE × MOIS_ANNEE")
print("=" * 70)
print()
if doublons_all:
    doublons_df = pl.concat(doublons_all).sort("n", descending=True)
    print(f"⚠️  {len(doublons_df):,} combinaisons matricule × mois_annee en doublon")
    print("  Top 5 :")
    for row in doublons_df.head(5).iter_rows(named=True):
        print(f"    {row['matricule']} | {row['mois_annee']} | {row['n']} occurrences")
    sauvegarder_rapport(doublons_df, "doublons_matricule_periode.csv")
else:
    print("✓ Aucun doublon matricule × mois_annee")
print()

# --- Contrôle 4 ---
print("=" * 70)
print("CONTRÔLE 4 : COHÉRENCE MONTANTS (net > brut)")
print("=" * 70)
print()
if incoherences_tot > 0:
    pct_inc = round(100.0 * incoherences_tot / nb_total_global, 2)
    print(f"⚠️  Net > Brut : {incoherences_tot:,} lignes ({pct_inc:.2f}%)")
else:
    print("✓ Tous les montants sont cohérents (net ≤ brut)")
print()

# --- Contrôle 5 ---
print("=" * 70)
print("CONTRÔLE 5 : DISTRIBUTION DES SITUATIONS")
print("=" * 70)
print()
for cat, nb_cat in sorted(situations_all.items(), key=lambda x: -x[1]):
    pct = round(100.0 * nb_cat / nb_total_global, 1)
    print(f"  {cat:<25} : {nb_cat:,} ({pct:.1f}%)")
print()

# ============================================================
# RÉSUMÉ FINAL
# ============================================================

print("=" * 70)
print("RÉSUMÉ VALIDATION")
print("=" * 70)
print()
print(f"Années validées  : {len(cles_silver)}")
print(f"Lignes totales   : {nb_total_global:,}")
print(f"Agents uniques   : {len(matricules_vus):,}")
if periodes_vues:
    print(f"Périodes         : {min(periodes_vues)} à {max(periodes_vues)} "
          f"({len(set(periodes_vues))} périodes)")
print()
print("=" * 70)
print("✓ VALIDATION TERMINÉE")
print("=" * 70)
print("Prochaine étape : exécuter 05_silver_to_gold.py")
