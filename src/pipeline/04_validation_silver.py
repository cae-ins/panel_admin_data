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
from common.s3_client import create_s3_client
from common.logging_config import setup_pipeline_logging
from common.polars_s3 import lire_parquet_s3
import polars as pl

# --- CONFIGURATION ---

BUCKET_SILVER  = os.getenv("BUCKET_SILVER",  "silver")
BUCKET_STAGING = os.getenv("BUCKET_STAGING", "staging")
PREFIX_SILVER  = "panel_admin"
PREFIX_VALID   = "panel_admin/validation"

# --- CLIENT S3 (MinIO) ---
s3 = create_s3_client()
logger, _ = setup_pipeline_logging("04_validation_silver")




def sauvegarder_rapport(df: pl.DataFrame, nom_fichier: str) -> None:
    csv_bytes = df.write_csv().encode("utf-8")
    s3.put_object(
        Bucket=BUCKET_STAGING,
        Key=f"{PREFIX_VALID}/{nom_fichier}",
        Body=csv_bytes,
    )
    logger.info(f"Rapport sauvegardé : {nom_fichier}")


# ============================================================
# LISTER LES FICHIERS SILVER
# ============================================================

logger.info("=" * 70)
logger.info("POST-VALIDATION : CONTRÔLE QUALITÉ TABLE SILVER")
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
for cle in cles_silver:
    logger.info(f"  · {os.path.basename(cle)}")

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

    df       = lire_parquet_s3(BUCKET_SILVER, cle)
    nb       = len(df)
    cols_all = df.columns
    nb_total_global += nb
    logger.info(f"[{annee}] ✓ {nb:,} lignes × {len(cols_all)} cols")

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

# --- Contrôle 1 ---
logger.info("=" * 70)
logger.info("CONTRÔLE 1 : COMPLÉTUDE DES COLONNES CLÉS (toutes années)")
logger.info("=" * 70)
for col, (nn, tot) in sorted(completude_totale.items()):
    pct  = round(100.0 * nn / tot, 1) if tot > 0 else 0.0
    flag = "⚠️ " if pct < 95 else "✓ "
    if pct < 95:
        logger.warning(f"{col:<30} : {pct:.1f}% complet ({nn:,}/{tot:,})")
    else:
        logger.info(f"✓ {col:<30} : {pct:.1f}% complet ({nn:,}/{tot:,})")

# --- Contrôle 2 ---
logger.info("=" * 70)
logger.info("CONTRÔLE 2 : COLONNES SUSPECTES (NA variable selon l'année)")
logger.info("=" * 70)
na_par_annee       = pl.DataFrame(na_par_annee_rows).sort("annee")
colonnes_suspectes = []
for col in COLONNES_NA_SURVEY:
    col_na = f"{col}_pct_na"
    if col_na in na_par_annee.columns:
        pcts = na_par_annee[col_na].drop_nulls()
        if len(pcts) > 0 and pcts.max() > 95 and pcts.min() < 5:
            colonnes_suspectes.append(col)
            logger.warning(f"{col} : NA varie de {pcts.min():.1f}% à {pcts.max():.1f}% selon l'année")
if not colonnes_suspectes:
    logger.info("✓ Aucune colonne suspecte détectée")
else:
    sauvegarder_rapport(na_par_annee, "na_par_annee_colonnes_surveillees.csv")

# --- Contrôle 3 ---
logger.info("=" * 70)
logger.info("CONTRÔLE 3 : DOUBLONS MATRICULE × MOIS_ANNEE")
logger.info("=" * 70)
if doublons_all:
    doublons_df = pl.concat(doublons_all).sort("n", descending=True)
    logger.warning(f"{len(doublons_df):,} combinaisons matricule × mois_annee en doublon")
    logger.info("Top 5 :")
    for row in doublons_df.head(5).iter_rows(named=True):
        logger.info(f"  {row['matricule']} | {row['mois_annee']} | {row['n']} occurrences")
    sauvegarder_rapport(doublons_df, "doublons_matricule_periode.csv")
else:
    logger.info("✓ Aucun doublon matricule × mois_annee")

# --- Contrôle 4 ---
logger.info("=" * 70)
logger.info("CONTRÔLE 4 : COHÉRENCE MONTANTS (net > brut)")
logger.info("=" * 70)
if incoherences_tot > 0:
    pct_inc = round(100.0 * incoherences_tot / nb_total_global, 2)
    logger.warning(f"Net > Brut : {incoherences_tot:,} lignes ({pct_inc:.2f}%)")
else:
    logger.info("✓ Tous les montants sont cohérents (net ≤ brut)")

# --- Contrôle 5 ---
logger.info("=" * 70)
logger.info("CONTRÔLE 5 : DISTRIBUTION DES SITUATIONS")
logger.info("=" * 70)
for cat, nb_cat in sorted(situations_all.items(), key=lambda x: -x[1]):
    pct = round(100.0 * nb_cat / nb_total_global, 1)
    logger.info(f"  {cat:<25} : {nb_cat:,} ({pct:.1f}%)")

# ============================================================
# RÉSUMÉ FINAL
# ============================================================

logger.info("=" * 70)
logger.info("RÉSUMÉ VALIDATION")
logger.info("=" * 70)
logger.info(f"Années validées  : {len(cles_silver)}")
logger.info(f"Lignes totales   : {nb_total_global:,}")
logger.info(f"Agents uniques   : {len(matricules_vus):,}")
if periodes_vues:
    logger.info(f"Périodes         : {min(periodes_vues)} à {max(periodes_vues)} "
                f"({len(set(periodes_vues))} périodes)")
logger.info("=" * 70)
logger.info("✓ VALIDATION TERMINÉE")
logger.info("=" * 70)
logger.info("Prochaine étape : exécuter 05_silver_to_gold.py")
