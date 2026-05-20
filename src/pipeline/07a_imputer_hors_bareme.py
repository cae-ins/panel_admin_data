# ============================================================
# PANEL ADMIN — ÉTAPE 7a : IMPUTATION FORWARD/BACKWARD
#              DES AGENTS HORS-BARÈME (M1 + M2)
# ============================================================
# Pour les agents ayant entre 1 et 5 mois hors-barème, remplace
# la valeur hors-barème par la valeur conforme la plus proche dans
# le temps (même matricule × GRADE × emploi × CODE_ORGANISME).
#
# Méthodo 1 — Statut
#   Variable : montant_brut_sans_ind_sans_alloc
#   Bornes   : [min_bareme, max_bareme]
#   Source   : montant_brut_sans_ind_sans_alloc_source (OBSERVE / IMPUTE)
#
# Méthodo 2 — Statut_Total
#   Variable : montant_brut_total
#   Bornes   : [min_total, max_total]
#   Source   : montant_brut_total_source (OBSERVE / IMPUTE)
#
# Prérequis : 07_calcul_indicateur.py doit avoir produit
#   s3://gold/panel_admin/base_avec_statut.parquet
#
# Méthode (identique M1 et M2) :
#   1. Éligibilité : agents avec 1-5 mois hors-barème par matricule
#   2. Forward fill (valeur conforme passée la plus récente)
#   3. Backward fill (valeur conforme future la plus proche)
#   4. Sélection de la valeur la plus proche en distance mensuelle
#      (priorité au passé en cas d'égalité)
#   5. Vérification : valeur imputée dans [min, max] du barème
#   6. Mise à jour du Statut → "Conforme au barème"
#
# Sortie :
#   s3://gold/panel_admin/base_avec_statut_corrigee.parquet
#
# Dépendances :
#   pip install polars boto3 python-dotenv
# ============================================================

import io
import os
from common.s3_client import create_s3_client
from common.logging_config import setup_pipeline_logging
import polars as pl

# ============================================================
# CONFIGURATION
# ============================================================

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
s3 = create_s3_client()
logger, _ = setup_pipeline_logging("07a_imputer_hors_bareme")

logger.info("=" * 70)
logger.info("IMPUTATION HORS-BARÈME — Forward/Backward fill (1-5 mois)")
logger.info("=" * 70)

# ============================================================
# 1. CHARGEMENT base_avec_statut.parquet
# ============================================================

logger.info("1. Chargement base_avec_statut.parquet...")

buf = io.BytesIO()
s3.download_fileobj(BUCKET_GOLD, KEY_BASE_AVEC_STATUT, buf)
buf.seek(0)
base = pl.read_parquet(buf)

logger.info(f"   Base chargée : {base.shape[0]:,} lignes × {base.shape[1]} colonnes")
logger.info(f"   Agents uniques : {base.select('matricule').n_unique():,}")

# Distribution Statut initiale
total = len(base)
logger.info(f"\n   Distribution Statut (entrée) :")
for row in base["Statut"].value_counts().sort("Statut").iter_rows(named=True):
    pct = 100 * row["count"] / total if total > 0 else 0.0
    logger.info(f"     {row['Statut']:<40} : {row['count']:>10,}  ({pct:.1f}%)")

# ============================================================
# 2. VÉRIFICATION COLONNES NÉCESSAIRES
# ============================================================

cols_requises = [
    # Identifiants individu / temps
    "matricule", "GRADE", "mois_annee",
    # M1
    "Statut", "montant_brut_sans_ind_sans_alloc", "min_bareme", "max_bareme",
    # M2
    "Statut_Total", "montant_brut_total", "min_total", "max_total",
]
# Note : emploi et CODE_ORGANISME sont optionnels dans les colonnes de groupe
# (GROUP_COLS = ["matricule","GRADE","emploi","CODE_ORGANISME"]).
# group_cols_dispo filtre dynamiquement les colonnes présentes — pas bloquant si absentes.

cols_manquantes = [c for c in cols_requises if c not in base.columns]
if cols_manquantes:
    raise RuntimeError(
        f"Colonnes manquantes dans base_avec_statut.parquet : {cols_manquantes}\n"
        "Vérifiez que l'étape 07_calcul_indicateur.py a été exécutée correctement."
    )

# Filtrer les GROUP_COLS disponibles (emploi peut être absent)
group_cols_dispo = [c for c in GROUP_COLS if c in base.columns]
logger.info(f"2. Colonnes de groupe utilisées : {group_cols_dispo}")

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

logger.info("3. Calcul éligibilité (1-{} mois hors-barème par matricule)...".format(
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

logger.info(f"   Agents hors-barème          : {nb_total_incoh:,}")
logger.info(f"   → éligibles  (1-{MAX_MOIS_HORS_BAREME} mois)  : {nb_eligible:,}  ({pct_agents:.1f}%)")
logger.info(f"   → non éligibles (>{MAX_MOIS_HORS_BAREME} mois): {nb_non_eligible:,}  "
            f"({100-pct_agents:.1f}%)")
logger.info(f"   Lignes hors-barème éligibles : {nb_lignes_elig:,}  ({pct_lignes:.1f}%)")

# ============================================================
# 5. FORWARD + BACKWARD FILL — valeur conforme la plus proche
# ============================================================

logger.info("4. M1 — Imputation forward/backward (montant_brut_sans_ind_sans_alloc)...")
logger.info(f"   Méthode : pour chaque mois hors-barème, on cherche la valeur conforme la plus proche")
logger.info(f"   dans le temps (même {group_cols_dispo}) — priorité au passé en cas d'égalité de distance")

# Valeur conforme uniquement (sinon None)
base = base.with_columns([
    pl.when(pl.col("Statut") == "Conforme au barème")
      .then(pl.col("montant_brut_sans_ind_sans_alloc"))
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
    (pl.col("_valeur_proche") != pl.col("montant_brut_sans_ind_sans_alloc"))
)

base = base.with_columns(
    pl.when(_condition_impute)
      .then(pl.lit("IMPUTE"))
      .otherwise(pl.lit("OBSERVE"))
      .alias("montant_brut_sans_ind_sans_alloc_source")
)

# Sauvegarde de la valeur originale avant écrasement
# (utilisée pour restaurer la valeur observée si l'imputation est annulée)
base = base.with_columns(
    pl.col("montant_brut_sans_ind_sans_alloc").alias("_original_m1")
)

# Écraser montant_brut_sans_ind_sans_alloc pour les mois éligibles imputés
base_corrigee = base.with_columns(
    pl.when(_condition_impute)
      .then(pl.col("_valeur_proche"))
      .otherwise(pl.col("montant_brut_sans_ind_sans_alloc"))
      .alias("montant_brut_sans_ind_sans_alloc")
)

# ============================================================
# 7. VÉRIFICATION — valeurs imputées dans [min_bareme, max_bareme]
# ============================================================

logger.info("5. M1 — Vérification : les valeurs imputées respectent-elles [min_bareme, max_bareme] ?")

imputes = base_corrigee.filter(pl.col("montant_brut_sans_ind_sans_alloc_source") == "IMPUTE")
n_imputes = len(imputes)

if n_imputes > 0 and "min_bareme" in base_corrigee.columns and "max_bareme" in base_corrigee.columns:
    hors_borne = imputes.filter(
        (pl.col("montant_brut_sans_ind_sans_alloc") < pl.col("min_bareme")) |
        (pl.col("montant_brut_sans_ind_sans_alloc") > pl.col("max_bareme"))
    )
    n_hors_borne = len(hors_borne)

    if n_hors_borne == 0:
        logger.info(f"   ✅ Toutes les {n_imputes:,} imputations M1 sont dans [min_bareme, max_bareme] — OK")
    else:
        pct_hors = 100 * n_hors_borne / n_imputes if n_imputes > 0 else 0
        logger.warning(
            f"   ⚠️  {n_hors_borne:,} / {n_imputes:,} imputations M1 ({pct_hors:.1f}%) "
            f"dépassent les bornes du barème → annulées, retour à la valeur observée (OBSERVE)"
        )
        # Annuler les imputations hors-borne : restaurer la valeur originale observée
        # _original_m1 a été sauvegardé juste avant l'écrasement → valeur hors-barème d'origine
        base_corrigee = base_corrigee.with_columns(
            pl.when(
                (pl.col("montant_brut_sans_ind_sans_alloc_source") == "IMPUTE") &
                ((pl.col("montant_brut_sans_ind_sans_alloc") < pl.col("min_bareme")) |
                 (pl.col("montant_brut_sans_ind_sans_alloc") > pl.col("max_bareme")))
            )
            .then(pl.col("_original_m1"))   # ← valeur observée d'origine, non NULL
            .otherwise(pl.col("montant_brut_sans_ind_sans_alloc"))
            .alias("montant_brut_sans_ind_sans_alloc")
        )
        # Remettre source à OBSERVE : la valeur restaurée est hors-barème → test bornes valide
        base_corrigee = base_corrigee.with_columns(
            pl.when(
                (pl.col("montant_brut_sans_ind_sans_alloc_source") == "IMPUTE") &
                ((pl.col("montant_brut_sans_ind_sans_alloc") < pl.col("min_bareme")) |
                 (pl.col("montant_brut_sans_ind_sans_alloc") > pl.col("max_bareme")))
            )
            .then(pl.lit("OBSERVE"))
            .otherwise(pl.col("montant_brut_sans_ind_sans_alloc_source"))
            .alias("montant_brut_sans_ind_sans_alloc_source")
        )
        n_retenues_m1 = n_imputes - n_hors_borne
        pct_ret_m1    = 100 * n_retenues_m1 / n_imputes if n_imputes > 0 else 0
        logger.info(f"\n   Bilan M1 :")
        logger.info(f"     Imputations réalisées              : {n_imputes:>10,}")
        logger.info(
            f"     Annulées (hors [min_bareme, max])  : {n_hors_borne:>10,}  ({pct_hors:.1f}%)"
            f"  → valeur originale restaurée, Statut inchangé (hors-barème)"
        )
        logger.info(
            f"     Retenues dans la base              : {n_retenues_m1:>10,}  ({pct_ret_m1:.1f}%)"
            f"  → valeur remplacée, Statut → Conforme"
        )
else:
    logger.info(f"   ✓ {n_imputes:,} lignes imputées — vérification bornes ignorée "
                f"(colonnes min/max absentes)")

# ============================================================
# 8. MISE À JOUR DU STATUT post-imputation
# ============================================================

logger.info("6. M1 — Recalcul du Statut pour les lignes imputées avec succès...")

if "min_bareme" in base_corrigee.columns and "max_bareme" in base_corrigee.columns:
    base_corrigee = base_corrigee.with_columns(
        pl.when(
            # Agent imputable, valeur imputée avec succès → recalcul Statut
            pl.col("eligible_groupe") &
            pl.col("Statut").is_in(STATUTS_HORS_BAREME) &
            (pl.col("montant_brut_sans_ind_sans_alloc_source") == "IMPUTE") &
            pl.col("min_bareme").is_not_null() &
            pl.col("max_bareme").is_not_null()
        )
        .then(
            pl.when(pl.col("montant_brut_sans_ind_sans_alloc") < pl.col("min_bareme"))
              .then(pl.lit("Inférieur au minimum du barème"))
            .when(pl.col("montant_brut_sans_ind_sans_alloc") > pl.col("max_bareme"))
              .then(pl.lit("Supérieur au maximum du barème"))
            .otherwise(pl.lit("Conforme au barème"))
        )
        .otherwise(pl.col("Statut"))
        .alias("Statut")
    )

# Distribution Statut M1 après imputation
total_m1 = len(base_corrigee)
logger.info(f"\n   Distribution Statut M1 après imputation :")
for row in base_corrigee["Statut"].value_counts().sort("Statut").iter_rows(named=True):
    pct = 100 * row["count"] / total_m1 if total_m1 > 0 else 0.0
    logger.info(f"     {row['Statut']:<40} : {row['count']:>10,}  ({pct:.1f}%)")

# ============================================================
# M2. IMPUTATION STATUT_TOTAL (montant_brut_total)
# ============================================================

logger.info("─" * 70)
logger.info("M2 — Imputation Statut_Total (montant_brut_total vs [min_total, max_total])")
logger.info("─" * 70)

# Éligibilité M2 : agents avec 1-MAX_MOIS_HORS_BAREME mois hors-barème (Statut_Total)
nb_hors_m2_df = (
    base_corrigee.group_by("matricule")
    .agg(pl.col("Statut_Total").is_in(STATUTS_HORS_BAREME).sum().alias("nb_hors_m2"))
)
base_corrigee = base_corrigee.join(nb_hors_m2_df, on="matricule", how="left")
base_corrigee = base_corrigee.with_columns(
    ((pl.col("nb_hors_m2") >= 1) & (pl.col("nb_hors_m2") <= MAX_MOIS_HORS_BAREME))
    .alias("eligible_m2")
)

nb_hors_m2_tot = base_corrigee.filter(
    pl.col("Statut_Total").is_in(STATUTS_HORS_BAREME)
).select("matricule").n_unique()
nb_elig_m2 = base_corrigee.filter(
    pl.col("eligible_m2") & pl.col("Statut_Total").is_in(STATUTS_HORS_BAREME)
).select("matricule").n_unique()
pct_m2 = 100 * nb_elig_m2 / nb_hors_m2_tot if nb_hors_m2_tot > 0 else 0.0
# Distribution Statut_Total M2 à l'entrée
total_m2_in = len(base_corrigee)
logger.info(f"   Distribution Statut_Total (entrée) :")
for row in base_corrigee["Statut_Total"].value_counts().sort("Statut_Total").iter_rows(named=True):
    pct = 100 * row["count"] / total_m2_in if total_m2_in > 0 else 0.0
    logger.info(f"     {row['Statut_Total']:<40} : {row['count']:>10,}  ({pct:.1f}%)")

pct_non_m2 = 100 - pct_m2
logger.info(f"   Agents avec Statut_Total hors-barème            : {nb_hors_m2_tot:,}")
logger.info(f"   → Éligibles à l'imputation (1–{MAX_MOIS_HORS_BAREME} mois hors-barème) : {nb_elig_m2:,}  ({pct_m2:.1f}%)")
logger.info(f"   → Non éligibles (>{MAX_MOIS_HORS_BAREME} mois — trop instables)        : {nb_hors_m2_tot - nb_elig_m2:,}  ({pct_non_m2:.1f}%)  → conservés hors-barème")

# Valeur conforme M2
base_corrigee = base_corrigee.with_columns([
    pl.when(pl.col("Statut_Total") == "Conforme au barème")
      .then(pl.col("montant_brut_total"))
      .otherwise(None)
      .alias("_val_conf_m2"),
    pl.when(pl.col("Statut_Total") == "Conforme au barème")
      .then(pl.col("_month_idx"))
      .otherwise(None)
      .alias("_midx_conf_m2"),
])

base_corrigee = base_corrigee.sort(group_cols_dispo + ["_periode_sort"])

base_corrigee = base_corrigee.with_columns([
    pl.col("_val_conf_m2").fill_null(strategy="forward").over(group_cols_dispo).alias("_val_fwd_m2"),
    pl.col("_midx_conf_m2").fill_null(strategy="forward").over(group_cols_dispo).alias("_midx_fwd_m2"),
])
base_corrigee = base_corrigee.with_columns([
    pl.col("_val_conf_m2").fill_null(strategy="backward").over(group_cols_dispo).alias("_val_bwd_m2"),
    pl.col("_midx_conf_m2").fill_null(strategy="backward").over(group_cols_dispo).alias("_midx_bwd_m2"),
])
base_corrigee = base_corrigee.with_columns([
    (pl.col("_month_idx") - pl.col("_midx_fwd_m2")).abs().alias("_dist_fwd_m2"),
    (pl.col("_midx_bwd_m2") - pl.col("_month_idx")).abs().alias("_dist_bwd_m2"),
])
base_corrigee = base_corrigee.with_columns(
    pl.when(
        pl.col("_val_fwd_m2").is_not_null() & (
            pl.col("_val_bwd_m2").is_null() |
            (pl.col("_dist_fwd_m2") <= pl.col("_dist_bwd_m2"))
        )
    )
    .then(pl.col("_val_fwd_m2"))
    .when(pl.col("_val_bwd_m2").is_not_null())
    .then(pl.col("_val_bwd_m2"))
    .otherwise(None)
    .alias("_val_proche_m2")
)

# Condition d'imputation M2
_cond_m2 = (
    pl.col("eligible_m2") &
    pl.col("Statut_Total").is_in(STATUTS_HORS_BAREME) &
    pl.col("_val_proche_m2").is_not_null() &
    (pl.col("_val_proche_m2") != pl.col("montant_brut_total"))
)

# Sauvegarde de la valeur originale avant écrasement
base_corrigee = base_corrigee.with_columns(
    pl.col("montant_brut_total").alias("_original_m2")
)

base_corrigee = base_corrigee.with_columns(
    pl.when(_cond_m2).then(pl.lit("IMPUTE")).otherwise(pl.lit("OBSERVE"))
    .alias("montant_brut_total_source")
)
base_corrigee = base_corrigee.with_columns(
    pl.when(_cond_m2).then(pl.col("_val_proche_m2")).otherwise(pl.col("montant_brut_total"))
    .alias("montant_brut_total")
)

# Vérification : valeurs imputées dans [min_total, max_total]
imputes_m2 = base_corrigee.filter(pl.col("montant_brut_total_source") == "IMPUTE")
n_imp_m2 = len(imputes_m2)
n_hb_m2  = 0   # initialisé ici pour être accessible dans le bilan
if n_imp_m2 > 0:
    hors_borne_m2 = imputes_m2.filter(
        (pl.col("montant_brut_total") < pl.col("min_total")) |
        (pl.col("montant_brut_total") > pl.col("max_total"))
    )
    n_hb_m2 = len(hors_borne_m2)
    if n_hb_m2 == 0:
        logger.info(f"   ✅ Toutes les {n_imp_m2:,} imputations M2 respectent [min_total, max_total] — OK")
    else:
        # Annuler les imputations hors-borne : restaurer la valeur originale observée
        base_corrigee = base_corrigee.with_columns(
            pl.when(
                (pl.col("montant_brut_total_source") == "IMPUTE") &
                ((pl.col("montant_brut_total") < pl.col("min_total")) |
                 (pl.col("montant_brut_total") > pl.col("max_total")))
            )
            .then(pl.col("_original_m2"))   # ← valeur observée d'origine, non NULL
            .otherwise(pl.col("montant_brut_total"))
            .alias("montant_brut_total")
        )
        # Remettre source à OBSERVE : la valeur restaurée est hors-barème → test bornes valide
        base_corrigee = base_corrigee.with_columns(
            pl.when(
                (pl.col("montant_brut_total_source") == "IMPUTE") &
                ((pl.col("montant_brut_total") < pl.col("min_total")) |
                 (pl.col("montant_brut_total") > pl.col("max_total")))
            )
            .then(pl.lit("OBSERVE"))
            .otherwise(pl.col("montant_brut_total_source"))
            .alias("montant_brut_total_source")
        )
        pct_hb_m2 = 100 * n_hb_m2 / n_imp_m2 if n_imp_m2 > 0 else 0
        logger.warning(
            f"   ⚠️  {n_hb_m2:,} / {n_imp_m2:,} imputations M2 ({pct_hb_m2:.1f}%) "
            f"dépassent [min_total, max_total] → annulées, valeur originale restaurée (OBSERVE)"
        )

# Mise à jour Statut_Total post-imputation
base_corrigee = base_corrigee.with_columns(
    pl.when(
        pl.col("eligible_m2") &
        pl.col("Statut_Total").is_in(STATUTS_HORS_BAREME) &
        (pl.col("montant_brut_total_source") == "IMPUTE") &
        pl.col("min_total").is_not_null() &
        pl.col("max_total").is_not_null()
    )
    .then(
        pl.when(pl.col("montant_brut_total") < pl.col("min_total"))
          .then(pl.lit("Inférieur au minimum du barème"))
        .when(pl.col("montant_brut_total") > pl.col("max_total"))
          .then(pl.lit("Supérieur au maximum du barème"))
        .otherwise(pl.lit("Conforme au barème"))
    )
    .otherwise(pl.col("Statut_Total"))
    .alias("Statut_Total")
)

# Stats M2
n_imp_m2_final = len(base_corrigee.filter(pl.col("montant_brut_total_source") == "IMPUTE"))
total = len(base_corrigee)
pct_imp_ret = 100 * n_imp_m2_final / n_imp_m2 if n_imp_m2 > 0 else 0

logger.info(f"\n   Bilan M2 :")
logger.info(f"     Imputations réalisées            : {n_imp_m2:>10,}")
logger.info(
    f"     Annulées (hors [min_total, max]) : {n_hb_m2:>10,}  ({100*n_hb_m2/n_imp_m2:.1f}%)"
    f"  → valeur originale restaurée, Statut_Total inchangé (hors-barème)"
)
logger.info(
    f"     Retenues dans la base            : {n_imp_m2_final:>10,}  ({pct_imp_ret:.1f}%)"
    f"  → valeur remplacée, Statut_Total → Conforme"
)

logger.info(f"\n   Distribution Statut_Total après imputation M2 :")
for row in base_corrigee["Statut_Total"].value_counts().sort("Statut_Total").iter_rows(named=True):
    pct = 100 * row["count"] / total if total > 0 else 0.0
    logger.info(f"     {row['Statut_Total']:<40} : {row['count']:>10,}  ({pct:.1f}%)")

# ============================================================
# 9. NETTOYAGE COLONNES TEMPORAIRES
# ============================================================

cols_temp = [
    "_val_conforme", "_midx_conforme", "_month_idx",
    "_val_fwd", "_midx_fwd", "_val_bwd", "_midx_bwd",
    "_dist_fwd", "_dist_bwd", "_valeur_proche", "_periode_sort",
    "nb_hors_groupe", "eligible_groupe",
    "_original_m1",   # sauvegarde valeur observée M1 avant imputation
    # M2
    "_val_conf_m2", "_midx_conf_m2", "_val_fwd_m2", "_midx_fwd_m2",
    "_val_bwd_m2", "_midx_bwd_m2", "_dist_fwd_m2", "_dist_bwd_m2",
    "_val_proche_m2", "nb_hors_m2", "eligible_m2",
    "_original_m2",   # sauvegarde valeur observée M2 avant imputation
]
base_corrigee = base_corrigee.drop([c for c in cols_temp if c in base_corrigee.columns])

logger.info(f"   ✓ Colonnes de travail (forward/backward fill) supprimées — base allégée")
logger.info(f"   Base corrigée finale : {len(base_corrigee):,} lignes × {len(base_corrigee.columns)} colonnes")

# ============================================================
# 10. STATISTIQUES POST-IMPUTATION
# ============================================================

logger.info("7. Statistiques post-imputation :")

# Distribution source
distrib_source = (
    base_corrigee.group_by("montant_brut_sans_ind_sans_alloc_source")
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

logger.info(f"\n   Distribution montant_brut_sans_ind_sans_alloc_source (M1) :")
for row in distrib_source.iter_rows(named=True):
    logger.info(
        f"     {row['montant_brut_sans_ind_sans_alloc_source']:<10} : "
        f"{row['nb_lignes']:>10,} lignes  "
        f"({row['pct_lignes']:.1f}%)  —  "
        f"{row['nb_agents']:>8,} agents "
        f"({row['pct_agents']:.1f}%)"
    )

if "montant_brut_total_source" in base_corrigee.columns:
    distrib_source_m2 = (
        base_corrigee.group_by("montant_brut_total_source")
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
    logger.info(f"\n   Distribution montant_brut_total_source (M2) :")
    for row in distrib_source_m2.iter_rows(named=True):
        logger.info(
            f"     {row['montant_brut_total_source']:<10} : "
            f"{row['nb_lignes']:>10,} lignes  "
            f"({row['pct_lignes']:.1f}%)  —  "
            f"{row['nb_agents']:>8,} agents "
            f"({row['pct_agents']:.1f}%)"
        )

# Distribution Statut M1 après imputation
total = len(base_corrigee)
logger.info(f"\n   Distribution Statut M1 (après imputation) :")
for row in base_corrigee["Statut"].value_counts().sort("Statut").iter_rows(named=True):
    pct = 100 * row["count"] / total if total > 0 else 0.0
    logger.info(f"     {row['Statut']:<40} : {row['count']:>10,}  ({pct:.1f}%)")

# Distribution Statut_Total M2 après imputation
if "Statut_Total" in base_corrigee.columns:
    logger.info(f"\n   Distribution Statut_Total M2 (après imputation) :")
    for row in base_corrigee["Statut_Total"].value_counts().sort("Statut_Total").iter_rows(named=True):
        pct = 100 * row["count"] / total if total > 0 else 0.0
        logger.info(f"     {row['Statut_Total']:<40} : {row['count']:>10,}  ({pct:.1f}%)")


# ============================================================
# 11. SAUVEGARDE base_avec_statut_corrigee.parquet → Gold
# ============================================================

logger.info("8. Sauvegarde base_avec_statut_corrigee.parquet...")

buf_pq = io.BytesIO()
base_corrigee.write_parquet(buf_pq, compression="snappy")
buf_pq.seek(0)
s3.put_object(
    Bucket=BUCKET_GOLD,
    Key=KEY_BASE_CORRIGEE,
    Body=buf_pq.getvalue(),
)

logger.info(f"   ✅ s3://{BUCKET_GOLD}/{KEY_BASE_CORRIGEE}")
logger.info(f"   {len(base_corrigee):,} lignes × {len(base_corrigee.columns)} colonnes")

logger.info("=" * 70)
logger.info("✓ IMPUTATION HORS-BARÈME TERMINÉE")
logger.info("=" * 70)
logger.info("Prochaine étape : exécuter 07b_calcul_indicateurs.py")
