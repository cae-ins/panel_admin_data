# ============================================================
# PANEL ADMIN — ÉTAPE 3c : IMPUTATION DES SALAIRES (SILVER)
# ============================================================
# Lit tous les fichiers Silver (avec GRADE de 03b), impute les
# salaires manquants par cascade :
#
#   1. LOCF par cle_unique  — dernier salaire connu de l'individu
#                             (forward fill) + remontée au premier
#                             mois connu (backward fill), dans le
#                             même organisme, trié chronologiquement
#   2. Médiane L1 : CODE_EMPLOI × GRADE × CODE_ORGANISME × mois_annee
#   3. Médiane L2 : CODE_EMPLOI × GRADE × mois_annee
#   4. Médiane L3 : GRADE × CODE_ORGANISME × mois_annee
#   5. Médiane L4 : GRADE × mois_annee
#
# Prérequis :
#   - 03_bronze_to_silver.py : filtre activité déjà appliqué
#   - 03b_imputer_grades_silver.py : colonne GRADE présente
#
# Architecture mémoire :
#   Phase 1 — Médianes : streaming fichier par fichier (colonnes minimales)
#   Phase 2 — LOCF     : index léger (colonnes minimales) + lookup par join
#   Phase 3 — Écriture : imputation + réécriture fichier par fichier
#   → Pic mémoire réduit de ~80% vs chargement global
#
# Colonnes ajoutées :
#   montant_brut_SOURCE — OBSERVE | LOCF_FWD | LOCF_BWD |
#                         IMPUTE_L1..L4 | NON_IMPUTABLE
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

# ============================================================
# CONFIGURATION
# ============================================================

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "datalab-team")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio-datalabteam123")

BUCKET_SILVER = os.getenv("BUCKET_SILVER", "silver")
PREFIX_SILVER = "panel_admin"

# Seuil minimum d'observations par cellule pour une médiane fiable
# Paramétrable par niveau (L4 = filet de sécurité → seuil plus souple)
MIN_OBS_PAR_NIVEAU = {
    "L1": int(os.getenv("MIN_OBS_L1", "5")),
    "L2": int(os.getenv("MIN_OBS_L2", "5")),
    "L3": int(os.getenv("MIN_OBS_L3", "3")),
    "L4": int(os.getenv("MIN_OBS_L4", "3")),
}

# Cascade de clusters médiane (du plus fin au plus large)
NIVEAUX_IMPUTATION = [
    ("L1", ["CODE_EMPLOI", "GRADE", "CODE_ORGANISME", "mois_annee"]),
    ("L2", ["CODE_EMPLOI", "GRADE", "mois_annee"]),
    ("L3", ["GRADE", "CODE_ORGANISME", "mois_annee"]),
    ("L4", ["GRADE", "mois_annee"]),
]

COLS_SALAIRE = ["montant_brut", "montant_net"]

# ============================================================
# CLIENT S3
# ============================================================

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


def annee_depuis_cle(cle: str) -> str:
    m = re.search(r"20\d{2}", os.path.basename(cle))
    return m.group() if m else os.path.basename(cle)


def periode_sort(col_mois_annee: pl.Expr) -> pl.Expr:
    """MMYYYY → YYYYMM pour tri chronologique correct."""
    return col_mois_annee.str.slice(2, 4) + col_mois_annee.str.slice(0, 2)


# ============================================================
# ÉTAPE 0 : LISTER LES FICHIERS SILVER
# ============================================================

print("=" * 70)
print("IMPUTATION DES SALAIRES — Silver")
print("=" * 70)
print()

pages = s3.get_paginator("list_objects_v2").paginate(
    Bucket=BUCKET_SILVER, Prefix=PREFIX_SILVER
)
cles_silver = sorted([
    obj["Key"]
    for page in pages
    for obj in page.get("Contents", [])
    if obj["Key"].endswith(".parquet")
])

if not cles_silver:
    raise RuntimeError(f"Aucun fichier .parquet trouvé sous s3://{BUCKET_SILVER}/{PREFIX_SILVER}")

print(f"Fichiers Silver trouvés : {len(cles_silver)}")
for cle in cles_silver:
    print(f"   {cle}")
print()

# Détecter les colonnes disponibles depuis le premier fichier
_df_probe = lire_parquet_s3(BUCKET_SILVER, cles_silver[0])
COLS_DISPO = set(_df_probe.columns)
locf_key   = "cle_unique" if "cle_unique" in COLS_DISPO else "matricule"
if locf_key not in COLS_DISPO:
    raise RuntimeError("Ni 'cle_unique' ni 'matricule' trouvés dans le Silver.")
del _df_probe
gc.collect()

print(f"Clé LOCF utilisée : {locf_key}\n")

# ============================================================
# PHASE 1 : CALCUL DES MÉDIANES (streaming fichier par fichier)
# ============================================================
# On ne charge que les colonnes nécessaires (cluster + salaire).
# Les DataFrames partiels sont légers : aucune colonne inutile.

print("─" * 70)
print("PHASE 1 — Calcul des médianes (streaming par fichier)")
print("─" * 70)
print()

# Accumulateurs : dict[col][niveau] → liste de DataFrames partiels
acc: dict[str, dict[str, list[pl.DataFrame]]] = {
    col: {niv: [] for niv, _ in NIVEAUX_IMPUTATION}
    for col in COLS_SALAIRE
}

for cle in cles_silver:
    annee = annee_depuis_cle(cle)
    print(f"   [{annee}] lecture partielle...", end=" ", flush=True)

    df = lire_parquet_s3(BUCKET_SILVER, cle)

    for col in COLS_SALAIRE:
        if col not in df.columns:
            continue
        df_obs = df.filter(pl.col(col).is_not_null())
        for niveau, cluster_raw in NIVEAUX_IMPUTATION:
            cluster = [c for c in cluster_raw if c in df.columns]
            if not cluster:
                continue
            acc[col][niveau].append(df_obs.select(cluster + [col]))
        del df_obs

    del df
    gc.collect()
    print("✓")

print()
print("   Consolidation des médianes...")

mediane_tables: dict[str, dict[str, pl.DataFrame]] = {
    col: {} for col in COLS_SALAIRE
}

for col in COLS_SALAIRE:
    for niveau, cluster_raw in NIVEAUX_IMPUTATION:
        frames = acc[col][niveau]
        if not frames:
            continue

        # cluster effectif = colonnes présentes dans au moins un fichier
        cluster = [c for c in cluster_raw if c in frames[0].columns]
        if not cluster:
            continue

        combined = pl.concat(frames, how="diagonal")
        seuil    = MIN_OBS_PAR_NIVEAU[niveau]

        med = (
            combined
            .group_by(cluster)
            .agg([
                pl.col(col).median().alias("_med"),
                pl.len().alias("_n"),
            ])
            .filter(pl.col("_n") >= seuil)
            .drop("_n")
        )
        mediane_tables[col][niveau] = med
        print(f"   {col:<15} {niveau} : {len(med):,} cellules (seuil={seuil})")

        del combined, frames
        acc[col][niveau] = []
        gc.collect()

del acc
gc.collect()
print()

# ============================================================
# PHASE 2 : CONSTRUCTION DE L'INDEX LOCF
# ============================================================
# Chargement des colonnes minimales (locf_key + mois_annee + salaires).
# Forward fill puis backward fill, tous les deux par groupe (locf_key).
#
# Forward fill : comble les mois après le premier salaire connu.
# Backward fill : remonte aux mois AVANT le premier salaire connu
#                 (nouvel entrant sans historique antérieur).

print("─" * 70)
print("PHASE 2 — Construction de l'index LOCF")
print("─" * 70)
print()

COLS_LOCF_LOAD = [locf_key, "mois_annee"] + COLS_SALAIRE

frames_locf = []
for cle in cles_silver:
    annee = annee_depuis_cle(cle)
    print(f"   [{annee}] colonnes LOCF...", end=" ", flush=True)
    df = lire_parquet_s3(BUCKET_SILVER, cle)
    cols_dispo = [c for c in COLS_LOCF_LOAD if c in df.columns]
    frames_locf.append(df.select(cols_dispo))
    del df
    gc.collect()
    print("✓")

index_locf = pl.concat(frames_locf, how="diagonal")
del frames_locf
gc.collect()
print(f"\n   Index brut : {len(index_locf):,} lignes")

# Tri chronologique
index_locf = (
    index_locf
    .with_columns(periode_sort(pl.col("mois_annee")).alias("_periode_sort"))
    .sort([locf_key, "_periode_sort"], nulls_last=True)
)

# LOCF en deux passes séparées pour chaque colonne salaire
for col in COLS_SALAIRE:
    if col not in index_locf.columns:
        continue

    # Passe 1 : forward fill par groupe
    index_locf = index_locf.with_columns(
        pl.col(col)
          .forward_fill()
          .over(locf_key)
          .alias(f"_locf_fwd_{col}")
    )

    # Passe 2 : backward fill par groupe (sur le résultat du forward)
    index_locf = index_locf.with_columns(
        pl.col(f"_locf_fwd_{col}")
          .backward_fill()
          .over(locf_key)
          .alias(f"_locf_bwd_{col}")
    )

# Table de lookup allégée : uniquement les colonnes nécessaires au join
locf_cols_keep = (
    [locf_key, "mois_annee"]
    + [f"_locf_fwd_{col}" for col in COLS_SALAIRE if f"_locf_fwd_{col}" in index_locf.columns]
    + [f"_locf_bwd_{col}" for col in COLS_SALAIRE if f"_locf_bwd_{col}" in index_locf.columns]
)
lookup_locf = index_locf.select(locf_cols_keep)
del index_locf
gc.collect()
print(f"   Index LOCF final : {len(lookup_locf):,} lignes × {len(lookup_locf.columns)} cols\n")

# ============================================================
# PHASE 3 : IMPUTATION + RÉÉCRITURE FICHIER PAR FICHIER
# ============================================================

print("─" * 70)
print("PHASE 3 — Imputation et réécriture par année")
print("─" * 70)

bilan_global: dict[str, dict[str, int]] = {col: {} for col in COLS_SALAIRE}
sort_cols_base = ["matricule", "mois_annee"]

for cle in cles_silver:
    annee = annee_depuis_cle(cle)
    print(f"\n   ┌─ [{annee}] ─────────────────────────────")

    df = lire_parquet_s3(BUCKET_SILVER, cle)
    n_annee = len(df)

    # ── Mémoriser les nulls originaux ──────────────────────
    for col in COLS_SALAIRE:
        df = df.with_columns(
            pl.col(col).is_null().alias(f"_null_{col}")
            if col in df.columns
            else pl.lit(False).alias(f"_null_{col}")
        )

    # ── Appliquer le LOCF via join ─────────────────────────
    join_cols = [c for c in [locf_key, "mois_annee"] if c in df.columns]

    # Filtrer le lookup sur l'année courante pour réduire la taille du join
    lookup_annee = lookup_locf.filter(
        pl.col("mois_annee").str.slice(2, 4) == annee
    )
    df = df.join(lookup_annee, on=join_cols, how="left")
    del lookup_annee

    # Marquer et appliquer FWD puis BWD
    locf_fwd_flags: dict[str, str] = {}
    locf_bwd_flags: dict[str, str] = {}

    for col in COLS_SALAIRE:
        if col not in df.columns:
            continue

        fwd_col   = f"_locf_fwd_{col}"
        bwd_col   = f"_locf_bwd_{col}"
        flag_null = f"_null_{col}"
        flag_fwd  = f"_flag_fwd_{col}"
        flag_bwd  = f"_flag_bwd_{col}"

        # Flag FWD : null original ET fwd disponible
        df = df.with_columns(
            (pl.col(flag_null) & pl.col(fwd_col).is_not_null()).alias(flag_fwd)
            if fwd_col in df.columns
            else pl.lit(False).alias(flag_fwd)
        )

        # Appliquer FWD
        if fwd_col in df.columns:
            df = df.with_columns(
                pl.when(pl.col(flag_null) & pl.col(fwd_col).is_not_null())
                  .then(pl.col(fwd_col))
                  .otherwise(pl.col(col))
                  .alias(col)
            )

        # Flag BWD : null original, PAS de fwd, bwd disponible
        df = df.with_columns(
            (pl.col(flag_null) & ~pl.col(flag_fwd) & pl.col(bwd_col).is_not_null()).alias(flag_bwd)
            if bwd_col in df.columns
            else pl.lit(False).alias(flag_bwd)
        )

        # Appliquer BWD (uniquement si toujours null après FWD)
        if bwd_col in df.columns:
            df = df.with_columns(
                pl.when(pl.col(col).is_null() & pl.col(bwd_col).is_not_null())
                  .then(pl.col(bwd_col))
                  .otherwise(pl.col(col))
                  .alias(col)
            )

        locf_fwd_flags[col] = flag_fwd
        locf_bwd_flags[col] = flag_bwd

        n_fwd = int(df[flag_fwd].sum()) if flag_fwd in df.columns else 0
        n_bwd = int(df[flag_bwd].sum()) if flag_bwd in df.columns else 0
        print(f"   │  {col:<15} LOCF_FWD : {n_fwd:,}   LOCF_BWD : {n_bwd:,}")

    # Drop colonnes LOCF temporaires
    df = df.drop([c for c in df.columns if c.startswith("_locf_")])

    # ── Médiane cascade ────────────────────────────────────
    niveau_col_map: dict[str, str] = {}

    for col in COLS_SALAIRE:
        if col not in df.columns:
            continue

        niveau_col = f"_niveau_{col}"
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(niveau_col))
        niveau_col_map[col] = niveau_col

        for niveau, cluster_raw in NIVEAUX_IMPUTATION:
            cluster = [c for c in cluster_raw if c in df.columns]
            if not cluster or niveau not in mediane_tables.get(col, {}):
                continue

            med = mediane_tables[col][niveau]

            # Sécurité : supprimer _med résiduel d'un join précédent
            if "_med" in df.columns:
                df = df.drop("_med")

            df = df.join(med, on=cluster, how="left")

            mask_imp = pl.col(col).is_null() & pl.col("_med").is_not_null()

            # Matérialiser le mask en colonne booléenne PENDANT que _med existe
            # (mask_imp est une expression lazy — l'évaluer après drop("_med") plante)
            df = df.with_columns([
                pl.when(mask_imp)
                  .then(pl.col("_med"))
                  .otherwise(pl.col(col))
                  .alias(col),

                # Enregistrer le niveau uniquement si pas encore imputé
                pl.when(mask_imp & pl.col(niveau_col).is_null())
                  .then(pl.lit(f"IMPUTE_{niveau}"))
                  .otherwise(pl.col(niveau_col))
                  .alias(niveau_col),

                # Comptage matérialisé avant drop
                mask_imp.alias("_mask_imp_bool"),
            ])

            n_imp = int(df["_mask_imp_bool"].sum())
            df = df.drop(["_med", "_mask_imp_bool"])

            if n_imp > 0:
                print(f"   │  {col:<15} {niveau}       : {n_imp:,}")

    # ── Colonnes SOURCE ────────────────────────────────────
    for col in COLS_SALAIRE:
        if col not in df.columns:
            continue

        flag_null  = f"_null_{col}"
        flag_fwd   = locf_fwd_flags.get(col, "_absent_")
        flag_bwd   = locf_bwd_flags.get(col, "_absent_")
        niveau_col = niveau_col_map.get(col, "_absent_")

        df = df.with_columns(
            pl.when(~pl.col(flag_null))
              .then(pl.lit("OBSERVE"))
              .when(
                  pl.col(flag_fwd)
                  if flag_fwd in df.columns
                  else pl.lit(False)
              )
              .then(pl.lit("LOCF_FWD"))
              .when(
                  pl.col(flag_bwd)
                  if flag_bwd in df.columns
                  else pl.lit(False)
              )
              .then(pl.lit("LOCF_BWD"))
              .when(
                  pl.col(niveau_col).is_not_null()
                  if niveau_col in df.columns
                  else pl.lit(False)
              )
              .then(
                  pl.col(niveau_col)
                  if niveau_col in df.columns
                  else pl.lit("IMPUTE")
              )
              .otherwise(pl.lit("NON_IMPUTABLE"))
              .alias(f"{col}_SOURCE")
        )

    # ── Nettoyage colonnes internes ────────────────────────
    cols_drop = [
        c for c in df.columns
        if c.startswith(("_null_", "_flag_fwd_", "_flag_bwd_", "_niveau_"))
    ]
    df = df.drop(cols_drop)

    # ── Vérification cohérence année ──────────────────────
    annees_dans_df = df.select(
        pl.col("mois_annee").str.slice(2, 4).alias("a")
    )["a"].unique().to_list()
    if annees_dans_df != [annee]:
        raise RuntimeError(
            f"Incohérence année : fichier={annee}, données={annees_dans_df}"
        )

    # ── Bilan annuel ───────────────────────────────────────
    for col in COLS_SALAIRE:
        src_col = f"{col}_SOURCE"
        if src_col not in df.columns:
            continue
        counts = (
            df[src_col]
            .value_counts()
            .sort("count", descending=True)
        )
        print(f"   │  {col} SOURCE :")
        for row in counts.iter_rows(named=True):
            src = row[src_col]
            n   = row["count"]
            print(f"   │    {src:<18} : {n:,}  ({100*n/n_annee:.1f}%)")
            bilan_global[col][src] = bilan_global[col].get(src, 0) + n

    # ── Écriture ──────────────────────────────────────────
    sort_cols = [c for c in sort_cols_base if c in df.columns]
    df = df.sort(sort_cols, nulls_last=True)
    print(f"   │")
    ecrire_parquet_s3(df, BUCKET_SILVER, cle)
    print(f"   └─────────────────────────────────────────")

    del df
    gc.collect()

del lookup_locf
gc.collect()

# ============================================================
# RÉSUMÉ FINAL
# ============================================================

print()
print("=" * 70)
print("BILAN GLOBAL IMPUTATION SALAIRES")
print("=" * 70)
print()

ORDRE_SOURCE = ["OBSERVE", "LOCF_FWD", "LOCF_BWD",
                "IMPUTE_L1", "IMPUTE_L2", "IMPUTE_L3", "IMPUTE_L4",
                "NON_IMPUTABLE"]

for col in COLS_SALAIRE:
    if not bilan_global[col]:
        continue
    n_tot = sum(bilan_global[col].values())
    print(f"  {col} (total : {n_tot:,}) :")
    for src in ORDRE_SOURCE:
        n = bilan_global[col].get(src, 0)
        if n > 0:
            print(f"    {src:<18} : {n:,}  ({100*n/n_tot:.1f}%)")
print()

print("=" * 70)
print("✓ IMPUTATION SALAIRES SILVER TERMINÉE")
print("=" * 70)
print()
print("Colonnes ajoutées dans tous les fichiers Silver :")
print("  montant_brut_SOURCE — OBSERVE | LOCF_FWD | LOCF_BWD |")
print("                        IMPUTE_L1..L4 | NON_IMPUTABLE")
print("  montant_net_SOURCE  — idem")
print()
print("Prochaine étape : exécuter 04_validation_silver.py")