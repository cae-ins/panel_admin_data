# ============================================================
# PANEL ADMIN — ÉTAPE 7a : IMPUTATION FORWARD/BACKWARD
#              DES AGENTS HORS-BARÈME
# ============================================================
# Pour les agents ayant entre 1 et 5 mois hors-barème, remplace
# le montant_brut_bareme par la valeur conforme la plus proche
# dans le temps (même matricule × GRADE × emploi × CODE_ORGANISME).
#
# Prérequis : 07_calcul_indicateur.py doit avoir produit
#   s3://gold/panel_admin/base_avec_statut.parquet
#
# Méthode :
#   1. Éligibilité : agents avec 1-5 mois hors-barème par matricule
#   2. Forward fill (valeur conforme passée la plus récente)
#   3. Backward fill (valeur conforme future la plus proche)
#   4. Sélection de la valeur la plus proche en distance mensuelle
#      (priorité au passé en cas d'égalité)
#   5. Vérification : la valeur imputée doit être dans [min_bareme, max_bareme]
#   6. Mise à jour du Statut des agents imputés → "Conforme au barème"
#
# Sorties :
#   s3://gold/panel_admin/base_avec_statut_corrigee.parquet
#
# Dépendances :
#   pip install polars boto3 python-dotenv
# ============================================================

import io
import os
import urllib3
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import polars as pl

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv(".env")

# ============================================================
# CONFIGURATION
# ============================================================

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "datalab-team")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio-datalabteam123")

BUCKET_GOLD = os.getenv("BUCKET_GOLD", "gold")
PREFIX_PANEL = "panel_admin"

KEY_BASE_AVEC_STATUT     = f"{PREFIX_PANEL}/base_avec_statut.parquet"
KEY_BASE_CORRIGEE        = f"{PREFIX_PANEL}/base_avec_statut_corrigee.parquet"

# Critère d'éligibilité : nombre max de mois hors-barème par matricule
MAX_MOIS_HORS_BAREME = 5

# Colonnes de groupe pour chercher la valeur conforme de remplacement
GROUP_COLS = ["matricule", "GRADE", "emploi", "CODE_ORGANISME"]

# Statuts considérés comme non conformes
STATUTS_HORS_BAREME = [
    "Inférieur au minimum du barème",
    "Supérieur au maximum du barème",
]

# --- CLIENT S3 ---
s3 = boto3.client(
    "s3",
    endpoint_url          = MINIO_ENDPOINT,
    aws_access_key_id     = MINIO_ACCESS_KEY,
    aws_secret_access_key = MINIO_SECRET_KEY,
    config                = Config(signature_version="s3v4"),
    region_name           = "us-east-1",
    verify                = False,
)

print("=" * 70)
print("IMPUTATION HORS-BARÈME — Forward/Backward fill (1-5 mois)")
print("=" * 70)
print()

# ============================================================
# 1. CHARGEMENT base_avec_statut.parquet
# ============================================================

print("1. Chargement base_avec_statut.parquet...")

buf = io.BytesIO()
s3.download_fileobj(BUCKET_GOLD, KEY_BASE_AVEC_STATUT, buf)
buf.seek(0)
base = pl.read_parquet(buf)

print(f"   Base chargée : {base.shape[0]:,} lignes × {base.shape[1]} colonnes")
print(f"   Agents uniques : {base.select('matricule').n_unique():,}")

# Distribution Statut initiale
total = len(base)
print(f"\n   Distribution Statut (entrée) :")
for row in base["Statut"].value_counts().sort("Statut").iter_rows(named=True):
    pct = 100 * row["count"] / total if total > 0 else 0.0
    print(f"     {row['Statut']:<40} : {row['count']:>10,}  ({pct:.1f}%)")
print()

# ============================================================
# 2. VÉRIFICATION COLONNES NÉCESSAIRES
# ============================================================

cols_requises = ["matricule", "GRADE", "emploi", "CODE_ORGANISME",
                 "mois_annee", "Statut", "montant_brut_bareme",
                 "min_bareme", "max_bareme"]

cols_manquantes = [c for c in cols_requises if c not in base.columns]
if cols_manquantes:
    raise RuntimeError(
        f"Colonnes manquantes dans base_avec_statut.parquet : {cols_manquantes}\n"
        "Vérifiez que l'étape 07_calcul_indicateur.py a été exécutée correctement."
    )

# Filtrer les GROUP_COLS disponibles (emploi peut être absent)
group_cols_dispo = [c for c in GROUP_COLS if c in base.columns]
print(f"2. Colonnes de groupe utilisées : {group_cols_dispo}")
print()

# ============================================================
# 3. CLÉ DE TRI CHRONOLOGIQUE
# ============================================================

# mois_annee format MMYYYY → clé de tri YYYYMM
base = base.with_columns(
    (pl.col("mois_annee").str.slice(2, 4) + pl.col("mois_annee").str.slice(0, 2))
    .alias("_periode_sort")
)

base = base.with_columns(
    (pl.col("_periode_sort").str.slice(0, 4).cast(pl.Int32) * 12 +
     pl.col("_periode_sort").str.slice(4, 2).cast(pl.Int32))
    .alias("_month_idx")
)

# ============================================================
# 4. ÉLIGIBILITÉ — agents avec 1 à MAX_MOIS_HORS_BAREME mois hors-barème
# ============================================================

print("3. Calcul éligibilité (1-{} mois hors-barème par matricule)...".format(
    MAX_MOIS_HORS_BAREME))

nb_hors_matricule = (
    base.group_by("matricule")
    .agg(
        pl.col("Statut").is_in(STATUTS_HORS_BAREME).sum().alias("nb_hors_groupe")
    )
)

base = base.join(nb_hors_matricule, on="matricule", how="left")

base = base.with_columns(
    (
        (pl.col("nb_hors_groupe") >= 1) &
        (pl.col("nb_hors_groupe") <= MAX_MOIS_HORS_BAREME)
    ).alias("eligible_groupe")
)

nb_total_incoh  = base.filter(pl.col("Statut").is_in(STATUTS_HORS_BAREME)).select("matricule").n_unique()
nb_eligible     = base.filter(
    pl.col("eligible_groupe") & pl.col("Statut").is_in(STATUTS_HORS_BAREME)
).select("matricule").n_unique()
nb_non_eligible = nb_total_incoh - nb_eligible

nb_lignes_total = base.filter(pl.col("Statut").is_in(STATUTS_HORS_BAREME)).shape[0]
nb_lignes_elig  = base.filter(
    pl.col("eligible_groupe") & pl.col("Statut").is_in(STATUTS_HORS_BAREME)
).shape[0]

pct_agents = 100 * nb_eligible / nb_total_incoh if nb_total_incoh > 0 else 0.0
pct_lignes = 100 * nb_lignes_elig / nb_lignes_total if nb_lignes_total > 0 else 0.0

print(f"   Agents hors-barème          : {nb_total_incoh:,}")
print(f"   → éligibles  (1-{MAX_MOIS_HORS_BAREME} mois)  : {nb_eligible:,}  ({pct_agents:.1f}%)")
print(f"   → non éligibles (>{MAX_MOIS_HORS_BAREME} mois): {nb_non_eligible:,}  "
      f"({100-pct_agents:.1f}%)")
print(f"   Lignes hors-barème éligibles : {nb_lignes_elig:,}  ({pct_lignes:.1f}%)")
print()

# ============================================================
# 5. FORWARD + BACKWARD FILL — valeur conforme la plus proche
# ============================================================

print("4. Imputation forward/backward...")

# Valeur conforme uniquement (sinon None)
base = base.with_columns([
    pl.when(pl.col("Statut") == "Conforme au barème")
      .then(pl.col("montant_brut_bareme"))
      .otherwise(None)
      .alias("_val_conforme"),
    pl.when(pl.col("Statut") == "Conforme au barème")
      .then(pl.col("_month_idx"))
      .otherwise(None)
      .alias("_midx_conforme"),
])

# Tri chronologique dans le groupe
base = base.sort(group_cols_dispo + ["_periode_sort"])

# Forward fill (passé)
base = base.with_columns([
    pl.col("_val_conforme").fill_null(strategy="forward").over(group_cols_dispo).alias("_val_fwd"),
    pl.col("_midx_conforme").fill_null(strategy="forward").over(group_cols_dispo).alias("_midx_fwd"),
])

# Backward fill (futur)
base = base.with_columns([
    pl.col("_val_conforme").fill_null(strategy="backward").over(group_cols_dispo).alias("_val_bwd"),
    pl.col("_midx_conforme").fill_null(strategy="backward").over(group_cols_dispo).alias("_midx_bwd"),
])

# Distances en mois
base = base.with_columns([
    (pl.col("_month_idx") - pl.col("_midx_fwd")).abs().alias("_dist_fwd"),
    (pl.col("_midx_bwd") - pl.col("_month_idx")).abs().alias("_dist_bwd"),
])

# Valeur la plus proche (priorité au passé en cas d'égalité)
base = base.with_columns(
    pl.when(
        pl.col("_val_fwd").is_not_null() & (
            pl.col("_val_bwd").is_null() |
            (pl.col("_dist_fwd") <= pl.col("_dist_bwd"))
        )
    )
    .then(pl.col("_val_fwd"))
    .when(pl.col("_val_bwd").is_not_null())
    .then(pl.col("_val_bwd"))
    .otherwise(None)
    .alias("_valeur_proche")
)

# ============================================================
# 6. CONDITION D'IMPUTATION + SOURCE
# ============================================================

_condition_impute = (
    pl.col("eligible_groupe") &
    pl.col("Statut").is_in(STATUTS_HORS_BAREME) &
    pl.col("_valeur_proche").is_not_null() &
    (pl.col("_valeur_proche") != pl.col("montant_brut_bareme"))
)

base = base.with_columns(
    pl.when(_condition_impute)
      .then(pl.lit("IMPUTE"))
      .otherwise(pl.lit("OBSERVE"))
      .alias("montant_brut_bareme_source")
)

# Écraser montant_brut_bareme pour les mois éligibles imputés
base_corrigee = base.with_columns(
    pl.when(_condition_impute)
      .then(pl.col("_valeur_proche"))
      .otherwise(pl.col("montant_brut_bareme"))
      .alias("montant_brut_bareme")
)

# ============================================================
# 7. VÉRIFICATION — valeurs imputées dans [min_bareme, max_bareme]
# ============================================================

print("5. Vérification : valeurs imputées dans [min_bareme, max_bareme]...")

imputes = base_corrigee.filter(pl.col("montant_brut_bareme_source") == "IMPUTE")
n_imputes = len(imputes)

if n_imputes > 0 and "min_bareme" in base_corrigee.columns and "max_bareme" in base_corrigee.columns:
    hors_borne = imputes.filter(
        (pl.col("montant_brut_bareme") < pl.col("min_bareme")) |
        (pl.col("montant_brut_bareme") > pl.col("max_bareme"))
    )
    n_hors_borne = len(hors_borne)

    if n_hors_borne == 0:
        print(f"   ✅ Toutes les valeurs imputées ({n_imputes:,}) sont dans [min, max] du barème")
    else:
        print(f"   ⚠️  {n_hors_borne:,} valeurs imputées hors [min_bareme, max_bareme]")
        print(f"       Ces imputations seront annulées (retour à OBSERVE)")
        # Annuler les imputations hors-borne (remettre la valeur originale)
        base_corrigee = base_corrigee.with_columns(
            pl.when(
                (pl.col("montant_brut_bareme_source") == "IMPUTE") &
                ((pl.col("montant_brut_bareme") < pl.col("min_bareme")) |
                 (pl.col("montant_brut_bareme") > pl.col("max_bareme")))
            )
            .then(pl.col("_val_conforme"))  # revert: aucune valeur conforme originale = null
            .otherwise(pl.col("montant_brut_bareme"))
            .alias("montant_brut_bareme")
        )
        base_corrigee = base_corrigee.with_columns(
            pl.when(
                (pl.col("montant_brut_bareme_source") == "IMPUTE") &
                ((pl.col("montant_brut_bareme") < pl.col("min_bareme")) |
                 (pl.col("montant_brut_bareme") > pl.col("max_bareme")))
            )
            .then(pl.lit("OBSERVE"))
            .otherwise(pl.col("montant_brut_bareme_source"))
            .alias("montant_brut_bareme_source")
        )
        print(f"       {n_hors_borne:,} imputations annulées — retour à OBSERVE")
else:
    print(f"   ✓ {n_imputes:,} lignes imputées — vérification bornes ignorée "
          f"(colonnes min/max absentes)")

# ============================================================
# 8. MISE À JOUR DU STATUT post-imputation
# ============================================================

print("6. Mise à jour Statut post-imputation...")

if "min_bareme" in base_corrigee.columns and "max_bareme" in base_corrigee.columns:
    base_corrigee = base_corrigee.with_columns(
        pl.when(
            # Agent imputable, valeur imputée avec succès → recalcul Statut
            pl.col("eligible_groupe") &
            pl.col("Statut").is_in(STATUTS_HORS_BAREME) &
            (pl.col("montant_brut_bareme_source") == "IMPUTE") &
            pl.col("min_bareme").is_not_null() &
            pl.col("max_bareme").is_not_null()
        )
        .then(
            pl.when(pl.col("montant_brut_bareme") < pl.col("min_bareme"))
              .then(pl.lit("Inférieur au minimum du barème"))
            .when(pl.col("montant_brut_bareme") > pl.col("max_bareme"))
              .then(pl.lit("Supérieur au maximum du barème"))
            .otherwise(pl.lit("Conforme au barème"))
        )
        .otherwise(pl.col("Statut"))
        .alias("Statut")
    )

# ============================================================
# 9. NETTOYAGE COLONNES TEMPORAIRES
# ============================================================

cols_temp = [
    "_val_conforme", "_midx_conforme", "_month_idx",
    "_val_fwd", "_midx_fwd", "_val_bwd", "_midx_bwd",
    "_dist_fwd", "_dist_bwd", "_valeur_proche", "_periode_sort",
    "nb_hors_groupe", "eligible_groupe",
]
base_corrigee = base_corrigee.drop([c for c in cols_temp if c in base_corrigee.columns])

print(f"   Colonnes temporaires supprimées")
print(f"   Base corrigée : {len(base_corrigee):,} lignes × {len(base_corrigee.columns)} colonnes")
print()

# ============================================================
# 10. STATISTIQUES POST-IMPUTATION
# ============================================================

print("7. Statistiques post-imputation :")

# Distribution source
distrib_source = (
    base_corrigee.group_by("montant_brut_bareme_source")
    .agg(
        pl.len().alias("nb_lignes"),
        pl.col("matricule").n_unique().alias("nb_agents"),
    )
    .with_columns([
        (pl.col("nb_lignes") / pl.col("nb_lignes").sum() * 100).round(2).alias("pct_lignes"),
        (pl.col("nb_agents") / pl.col("nb_agents").sum() * 100).round(2).alias("pct_agents"),
    ])
    .sort("nb_lignes", descending=True)
)

print(f"\n   Distribution montant_brut_bareme_source :")
for row in distrib_source.iter_rows(named=True):
    print(f"     {row['montant_brut_bareme_source']:<10} : "
          f"{row['nb_lignes']:>10,} lignes  "
          f"({row['pct_lignes']:.1f}%)  —  "
          f"{row['nb_agents']:>8,} agents "
          f"({row['pct_agents']:.1f}%)")

# Distribution Statut après imputation
total = len(base_corrigee)
print(f"\n   Distribution Statut (après imputation) :")
for row in base_corrigee["Statut"].value_counts().sort("Statut").iter_rows(named=True):
    pct = 100 * row["count"] / total if total > 0 else 0.0
    print(f"     {row['Statut']:<40} : {row['count']:>10,}  ({pct:.1f}%)")

# Résumé agents imputés
n_lignes_impute  = len(base_corrigee.filter(pl.col("montant_brut_bareme_source") == "IMPUTE"))
n_lignes_hors    = base.filter(pl.col("Statut").is_in(STATUTS_HORS_BAREME)).shape[0]
n_agents_imputes = base_corrigee.filter(
    pl.col("montant_brut_bareme_source") == "IMPUTE"
).select("matricule").n_unique()

pct_imp = 100 * n_lignes_impute / n_lignes_hors if n_lignes_hors > 0 else 0.0
print(f"\n   Bilan imputation :")
print(f"     Lignes hors-barème initiales  : {n_lignes_hors:,}")
print(f"     Lignes imputées (IMPUTE)       : {n_lignes_impute:,}  ({pct_imp:.1f}%)")
print(f"     Agents avec au moins 1 imputation : {n_agents_imputes:,}")

# ============================================================
# 11. SAUVEGARDE base_avec_statut_corrigee.parquet → Gold
# ============================================================

print("\n8. Sauvegarde base_avec_statut_corrigee.parquet...")

buf_pq = io.BytesIO()
base_corrigee.write_parquet(buf_pq, compression="snappy")
buf_pq.seek(0)
s3.put_object(
    Bucket=BUCKET_GOLD,
    Key=KEY_BASE_CORRIGEE,
    Body=buf_pq.getvalue(),
)

print(f"   ✅ s3://{BUCKET_GOLD}/{KEY_BASE_CORRIGEE}")
print(f"   {len(base_corrigee):,} lignes × {len(base_corrigee.columns)} colonnes")

print()
print("=" * 70)
print("✓ IMPUTATION HORS-BARÈME TERMINÉE")
print("=" * 70)
print()
print("Prochaine étape : exécuter 07b_calcul_indicateurs.py")
