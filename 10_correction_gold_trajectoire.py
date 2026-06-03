# ============================================================
# PANEL ADMIN — SCRIPT ISOLÉ
# CORRECTION GOLD : TRAJECTOIRES PROFESSIONNELLES
# ============================================================
#
# OBJECTIF
# --------
#   Ce script produit un panel Gold nettoyé spécifiquement pour
#   le suivi longitudinal des trajectoires professionnelles.
#
#   Pour un même individu (matricule), certaines informations
#   *invariantes* peuvent changer d'une période à l'autre à
#   cause d'erreurs de saisie : sexe, date de naissance, date
#   de prise de service. Ce script détecte et corrige ces
#   incohérences afin de garantir une trajectoire fiable.
#
# CONTRÔLES ET CORRECTIONS APPLIQUÉS
# -----------------------------------
#   1. SEXE
#      — Détection : un matricule a ≥ 2 valeurs distinctes
#      — Correction : vote majoritaire (valeur la plus fréquente)
#      — Si égalité parfaite : première valeur chronologique
#
#   2. DATE DE NAISSANCE
#      — Parsing multi-format (YYYY-MM-DD, DD/MM/YYYY, etc.)
#      — Contrôle de plausibilité : âge ∈ [16, 80] à la période
#      — Correction : date modale (la plus fréquente) par matricule
#      — Dates aberrantes (avant 1930 ou après 2010) exclues du vote
#
#   3. DATE DE PRISE DE SERVICE
#      — Parsing multi-format
#      — Contrôle : prise_service < mois_annee (pas dans le futur)
#      — Contrôle : prise_service > date_naissance + 16 ans
#      — Correction : date modale par matricule
#
# SORTIE
# ------
#   Dossier local : gold-clean-trajectoire-pro/
#     ├── panel_trajectoire_clean.parquet   Panel nettoyé complet
#     ├── rapport_corrections.csv           Détail par matricule corrigé
#     ├── rapport_anomalies_calendaires.csv Anomalies de dates détectées
#     └── rapport_synthese.txt              Synthèse lisible des corrections
#
#   + Upload S3 : s3://gold/panel_admin/trajectoire-pro/
#
# SOURCE
# ------
#   s3://gold/panel_admin/panel_complet.parquet (produit par 06_compiler_panel.py)
#
# DÉPENDANCES
# -----------
#   pip install polars boto3 python-dotenv
#
# USAGE
# -----
#   python 10_correction_gold_trajectoire.py
#
# ARCHITECTURE
# ------------
#   Script 100 % isolé — ne dépend d'aucun autre module du pipeline.
#   Polars uniquement, pas de Pandas.
# ============================================================

import gc
import io
import os
import re
import datetime
from pathlib import Path
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import polars as pl
import urllib3

# Désactiver les warnings SSL pour MinIO local
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv(".env")

# ============================================================
# CONFIGURATION
# ============================================================

# MinIO / S3
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "datalab-team")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio-datalabteam123")

BUCKET_GOLD = os.getenv("BUCKET_GOLD", "gold")

# Source : panel complet produit par l'étape 06
KEY_PANEL_SOURCE = "panel_admin/panel_complet.parquet"

# Destination S3
PREFIX_DESTINATION = "panel_admin/trajectoire-pro"

# Dossier local de sortie
DOSSIER_SORTIE = Path("gold-clean-trajectoire-pro")

# Colonnes invariantes à vérifier/corriger
COLS_INVARIANTES = ["sexe", "date_naissance", "prise_service"]

# Bornes de plausibilité
AGE_MIN_OBS          = 16     # Âge minimum à la période d'observation
AGE_MAX_OBS          = 80     # Âge maximum à la période d'observation
AGE_MIN_PRISE_SERVICE = 16   # Âge minimum à la prise de service
AGE_MAX_PRISE_SERVICE = 65   # Âge maximum à la prise de service
DATE_NAISS_MIN       = datetime.date(1930, 1, 1)   # Borne basse plausible
DATE_NAISS_MAX       = datetime.date(2010, 1, 1)   # Borne haute plausible
PRISE_SERVICE_MIN    = datetime.date(1960, 1, 1)   # Borne basse plausible

DAYS_PER_YEAR = 365.25


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
    """Télécharge et lit un fichier Parquet depuis S3/MinIO."""
    buf = io.BytesIO()
    s3.download_fileobj(bucket, key, buf)
    buf.seek(0)
    return pl.read_parquet(buf)


def ecrire_parquet_s3(df: pl.DataFrame, bucket: str, key: str) -> None:
    """Écrit un DataFrame Polars en Parquet vers S3/MinIO."""
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)
    taille = len(buf.getvalue()) / 1024**2
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    print(f"  ✓ s3://{bucket}/{key}  ({len(df):,} lignes  {taille:.1f} MB)")


def parse_date_multi(col_expr: pl.Expr) -> pl.Expr:
    """
    Parse une expression de colonne en pl.Date en essayant
    plusieurs formats courants (best-effort).

    Formats tentés :
      - YYYY-MM-DD  (ISO)
      - DD/MM/YYYY  (français)
      - DD-MM-YYYY
      - YYYY/MM/DD
      - DD.MM.YYYY
      - datetime ISO (avec T ou espace + heure)

    Retourne null si aucun format ne correspond.
    """
    s = col_expr.cast(pl.Utf8, strict=False)
    return pl.coalesce([
        s.str.to_date("%Y-%m-%d", strict=False),
        s.str.to_date("%d/%m/%Y", strict=False),
        s.str.to_date("%d-%m-%Y", strict=False),
        s.str.to_date("%Y/%m/%d", strict=False),
        s.str.to_date("%d.%m.%Y", strict=False),
        s.str.to_datetime(strict=False).cast(pl.Date),
    ])


def parse_mois_annee(col_expr: pl.Expr) -> pl.Expr:
    """
    Convertit mois_annee (String, format MMYYYY ou YYYY-MM)
    en pl.Date correspondant au premier jour du mois.
    """
    s = col_expr.cast(pl.Utf8, strict=False).str.replace_all(r"[_/.\-]", "")
    return pl.coalesce([
        # MMYYYY → 01-YYYY-MM
        ("01-" + s.str.slice(2, 4) + "-" + s.str.slice(0, 2))
            .str.to_date("%d-%Y-%m", strict=False),
        # YYYYMM → 01-YYYY-MM
        ("01-" + s.str.slice(0, 4) + "-" + s.str.slice(4, 2))
            .str.to_date("%d-%Y-%m", strict=False),
    ])


def valeur_modale(df_group: pl.DataFrame, col: str) -> str | None:
    """
    Retourne la valeur modale (la plus fréquente) d'une colonne
    dans un groupe. En cas d'égalité, retourne la première valeur
    chronologique (première apparition dans les données triées).
    """
    comptage = (
        df_group
        .filter(pl.col(col).is_not_null())
        .group_by(col)
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
    )
    if len(comptage) == 0:
        return None
    return comptage[col][0]


# ============================================================
# ÉTAPE 1 : CHARGEMENT DU PANEL COMPLET
# ============================================================

print("=" * 70)
print("CORRECTION GOLD — TRAJECTOIRES PROFESSIONNELLES")
print("=" * 70)
print()
print(f"Source : s3://{BUCKET_GOLD}/{KEY_PANEL_SOURCE}")
print()

print("[1/7] Chargement du panel complet...", end=" ", flush=True)
base = lire_parquet_s3(BUCKET_GOLD, KEY_PANEL_SOURCE)
print(f"✓  {len(base):,} lignes × {len(base.columns)} colonnes")
print()

# Vérification des colonnes nécessaires
cols_manquantes = [c for c in COLS_INVARIANTES + ["matricule", "mois_annee"]
                   if c not in base.columns]
if cols_manquantes:
    raise ValueError(
        f"Colonnes requises absentes du panel : {cols_manquantes}\n"
        f"Colonnes disponibles : {base.columns}"
    )

n_obs = len(base)
n_matricules = base["matricule"].n_unique()
print(f"  Observations totales  : {n_obs:,}")
print(f"  Matricules uniques    : {n_matricules:,}")

if "mois_annee" in base.columns:
    periodes = base["mois_annee"].drop_nulls().unique().sort()
    print(f"  Périodes              : {len(periodes)}  ·  {periodes.min()} → {periodes.max()}")
print()


# ============================================================
# ÉTAPE 2 : PARSING DES DATES
# ============================================================

print("[2/7] Parsing des dates...", flush=True)

base = base.with_columns([
    parse_date_multi(pl.col("date_naissance")).alias("_dn_dt"),
    parse_date_multi(pl.col("prise_service")).alias("_ps_dt"),
    parse_mois_annee(pl.col("mois_annee")).alias("_ma_dt"),
])

# Statistiques de parsing
for src, dst in [("date_naissance", "_dn_dt"),
                 ("prise_service",  "_ps_dt"),
                 ("mois_annee",     "_ma_dt")]:
    n_non_null = n_obs - base[src].null_count()
    n_parsed   = n_obs - base[dst].null_count()
    pct = 100 * n_parsed / n_non_null if n_non_null > 0 else 0
    print(f"  {src:<18} : {n_parsed:>10,} parsées / {n_non_null:,} non-null  ({pct:.1f} %)")

print()


# ============================================================
# ÉTAPE 3 : DIAGNOSTIC DE COHÉRENCE INTRA-INDIVIDU
# ============================================================

print("[3/7] Diagnostic de cohérence intra-individu...", flush=True)
print()

diagnostics = {}

for col in COLS_INVARIANTES:
    coh = (
        base
        .group_by("matricule")
        .agg([
            pl.col(col).drop_nulls().n_unique().alias("n_valeurs_distinctes"),
            pl.col(col).is_null().sum().alias("n_lignes_nulles"),
            pl.len().alias("n_lignes_totales"),
        ])
    )

    n_zero   = int((coh["n_valeurs_distinctes"] == 0).sum())
    n_une    = int((coh["n_valeurs_distinctes"] == 1).sum())
    n_incoh  = int((coh["n_valeurs_distinctes"] > 1).sum())

    diagnostics[col] = {
        "n_matricules_total": n_matricules,
        "n_aucune_obs": n_zero,
        "n_1_valeur": n_une,
        "n_incoherent": n_incoh,
    }

    print(f"  --- {col} ---")
    print(f"    Matricules total            : {n_matricules:>8,}")
    print(f"    Matricules sans obs (null)  : {n_zero:>8,}  ({100*n_zero/n_matricules:.2f} %)")
    print(f"    Matricules avec 1 valeur    : {n_une:>8,}  ({100*n_une/n_matricules:.2f} %)")
    print(f"    Matricules incohérents (>1) : {n_incoh:>8,}  ({100*n_incoh/n_matricules:.2f} %)")

    if n_incoh > 0:
        repart = (
            coh.filter(pl.col("n_valeurs_distinctes") > 1)
               .group_by("n_valeurs_distinctes")
               .agg(pl.len().alias("n_matricules"))
               .sort("n_valeurs_distinctes")
        )
        for row in repart.iter_rows(named=True):
            print(f"      {row['n_valeurs_distinctes']:>3} valeurs distinctes : "
                  f"{row['n_matricules']:>6,} matricules")
    print()

print()


# ============================================================
# ÉTAPE 4 : ANOMALIES CALENDAIRES
# ============================================================

print("[4/7] Détection des anomalies calendaires...", flush=True)

base = base.with_columns([
    ((pl.col("_ma_dt") - pl.col("_dn_dt")).dt.total_days() / DAYS_PER_YEAR)
        .alias("_age_obs"),
    ((pl.col("_ps_dt") - pl.col("_dn_dt")).dt.total_days() / DAYS_PER_YEAR)
        .alias("_age_prise_service"),
])

# Construction du rapport d'anomalies calendaires
anomalies = []

# 4.1 - Âge à la période hors bornes
mask_age = base["_age_obs"].is_not_null()
n_age_neg   = int(((base["_age_obs"] < 0) & mask_age).sum())
n_age_lt_16 = int(((base["_age_obs"] < AGE_MIN_OBS) & mask_age).sum())
n_age_gt_80 = int(((base["_age_obs"] > AGE_MAX_OBS) & mask_age).sum())

print(f"  Âge à la période < 0 (naiss. après obs)    : {n_age_neg:>10,}  ({100*n_age_neg/n_obs:.2f} %)")
print(f"  Âge à la période < {AGE_MIN_OBS}                     : {n_age_lt_16:>10,}  ({100*n_age_lt_16/n_obs:.2f} %)")
print(f"  Âge à la période > {AGE_MAX_OBS}                     : {n_age_gt_80:>10,}  ({100*n_age_gt_80/n_obs:.2f} %)")

# 4.2 - Prise de service après la période
mask_ps = base["_ps_dt"].is_not_null() & base["_ma_dt"].is_not_null()
n_ps_future = int(((base["_ps_dt"] > base["_ma_dt"]) & mask_ps).sum())
print(f"  Prise service > mois_annee (futur)         : {n_ps_future:>10,}  ({100*n_ps_future/n_obs:.2f} %)")

# 4.3 - Âge à la prise de service hors bornes
mask_aps = base["_age_prise_service"].is_not_null()
n_aps_neg   = int(((base["_age_prise_service"] < 0) & mask_aps).sum())
n_aps_lt_16 = int(((base["_age_prise_service"] < AGE_MIN_PRISE_SERVICE) & mask_aps).sum())
n_aps_gt_65 = int(((base["_age_prise_service"] > AGE_MAX_PRISE_SERVICE) & mask_aps).sum())
print(f"  Âge prise service < 0                      : {n_aps_neg:>10,}  ({100*n_aps_neg/n_obs:.2f} %)")
print(f"  Âge prise service < {AGE_MIN_PRISE_SERVICE}                     : {n_aps_lt_16:>10,}  ({100*n_aps_lt_16/n_obs:.2f} %)")
print(f"  Âge prise service > {AGE_MAX_PRISE_SERVICE}                     : {n_aps_gt_65:>10,}  ({100*n_aps_gt_65/n_obs:.2f} %)")

# 4.4 - Prise de service avant date_naissance
mask_dn_ps = base["_dn_dt"].is_not_null() & base["_ps_dt"].is_not_null()
n_naiss_apres_ps = int(((base["_dn_dt"] >= base["_ps_dt"]) & mask_dn_ps).sum())
print(f"  Naissance >= prise_service                  : {n_naiss_apres_ps:>10,}  ({100*n_naiss_apres_ps/n_obs:.2f} %)")

# 4.5 - date_retraite < prise_service (si colonne disponible)
if "date_retraite" in base.columns:
    base = base.with_columns(
        parse_date_multi(pl.col("date_retraite")).alias("_dr_dt")
    )
    mask_dr = base["_dr_dt"].is_not_null() & base["_ps_dt"].is_not_null()
    n_dr_lt_ps = int(((base["_dr_dt"] < base["_ps_dt"]) & mask_dr).sum())
    print(f"  Retraite < prise_service                   : {n_dr_lt_ps:>10,}  ({100*n_dr_lt_ps/n_obs:.2f} %)")

print()

# Exporter les lignes anomales pour le rapport
df_anomalies = (
    base
    .with_columns([
        (pl.col("_age_obs") < AGE_MIN_OBS).fill_null(False).alias("_ano_age_jeune"),
        (pl.col("_age_obs") > AGE_MAX_OBS).fill_null(False).alias("_ano_age_vieux"),
        (pl.col("_age_obs") < 0).fill_null(False).alias("_ano_age_negatif"),
        (pl.col("_ps_dt") > pl.col("_ma_dt")).fill_null(False).alias("_ano_ps_futur"),
        (pl.col("_age_prise_service") < 0).fill_null(False).alias("_ano_ps_avant_naiss"),
        (pl.col("_age_prise_service") < AGE_MIN_PRISE_SERVICE).fill_null(False).alias("_ano_aps_jeune"),
        (pl.col("_age_prise_service") > AGE_MAX_PRISE_SERVICE).fill_null(False).alias("_ano_aps_vieux"),
    ])
    .filter(
        pl.col("_ano_age_jeune") | pl.col("_ano_age_vieux") |
        pl.col("_ano_age_negatif") | pl.col("_ano_ps_futur") |
        pl.col("_ano_ps_avant_naiss") | pl.col("_ano_aps_jeune") |
        pl.col("_ano_aps_vieux")
    )
    .select([
        "matricule", "mois_annee",
        "sexe", "date_naissance", "prise_service",
        "_dn_dt", "_ps_dt", "_ma_dt",
        "_age_obs", "_age_prise_service",
        "_ano_age_jeune", "_ano_age_vieux", "_ano_age_negatif",
        "_ano_ps_futur", "_ano_ps_avant_naiss",
        "_ano_aps_jeune", "_ano_aps_vieux",
    ])
)

print(f"  Lignes avec au moins une anomalie calendaire : {len(df_anomalies):,}")
print()


# ============================================================
# ÉTAPE 5 : CORRECTION PAR VOTE MAJORITAIRE
# ============================================================

print("[5/7] Correction des colonnes invariantes...", flush=True)
print()

rapport_corrections = []

# ----- 5.1 CORRECTION DU SEXE -----

print("  5.1 — Correction du sexe...", flush=True)

# Calculer la valeur modale du sexe par matricule
sexe_modal = (
    base
    .filter(pl.col("sexe").is_not_null())
    .group_by(["matricule", "sexe"])
    .agg(pl.len().alias("n"))
    .sort(["matricule", "n"], descending=[False, True])
    .unique(subset=["matricule"], keep="first")
    .select(["matricule", pl.col("sexe").alias("sexe_corrige")])
)

# Identifier les matricules dont le sexe va changer
base_avec_sexe = base.join(sexe_modal, on="matricule", how="left")

n_sexe_corrige = int(
    (
        base_avec_sexe["sexe"].is_not_null() &
        base_avec_sexe["sexe_corrige"].is_not_null() &
        (base_avec_sexe["sexe"] != base_avec_sexe["sexe_corrige"])
    ).sum()
)

# Calculer le nombre de matricules affectés
matricules_sexe_corrige = (
    base_avec_sexe
    .filter(
        pl.col("sexe").is_not_null() &
        pl.col("sexe_corrige").is_not_null() &
        (pl.col("sexe") != pl.col("sexe_corrige"))
    )
    ["matricule"].n_unique()
)

print(f"    Lignes corrigées      : {n_sexe_corrige:,}")
print(f"    Matricules affectés   : {matricules_sexe_corrige:,}")

# Détail des corrections pour le rapport
if n_sexe_corrige > 0:
    detail_sexe = (
        base_avec_sexe
        .filter(
            pl.col("sexe").is_not_null() &
            pl.col("sexe_corrige").is_not_null() &
            (pl.col("sexe") != pl.col("sexe_corrige"))
        )
        .group_by(["matricule", "sexe", "sexe_corrige"])
        .agg(pl.len().alias("n_lignes"))
        .with_columns(pl.lit("sexe").alias("colonne"))
        .rename({"sexe": "ancienne_valeur", "sexe_corrige": "nouvelle_valeur"})
    )
    rapport_corrections.append(detail_sexe)

# Appliquer la correction
base = (
    base_avec_sexe
    .with_columns(
        pl.when(pl.col("sexe_corrige").is_not_null())
          .then(pl.col("sexe_corrige"))
          .otherwise(pl.col("sexe"))
          .alias("sexe")
    )
    .drop("sexe_corrige")
)

del base_avec_sexe
gc.collect()
print()

# ----- 5.2 CORRECTION DE LA DATE DE NAISSANCE -----

print("  5.2 — Correction de la date de naissance...", flush=True)

# Filtrer les dates de naissance plausibles avant le vote
# (exclure les dates aberrantes du calcul de mode)
dn_plausible = (
    base
    .filter(
        pl.col("_dn_dt").is_not_null() &
        (pl.col("_dn_dt") >= pl.lit(DATE_NAISS_MIN)) &
        (pl.col("_dn_dt") <= pl.lit(DATE_NAISS_MAX))
    )
    .group_by(["matricule", "_dn_dt"])
    .agg(pl.len().alias("n"))
    .sort(["matricule", "n"], descending=[False, True])
    .unique(subset=["matricule"], keep="first")
    .select(["matricule", pl.col("_dn_dt").alias("dn_corrigee")])
)

base = base.join(dn_plausible, on="matricule", how="left")

n_dn_corrige = int(
    (
        base["_dn_dt"].is_not_null() &
        base["dn_corrigee"].is_not_null() &
        (base["_dn_dt"] != base["dn_corrigee"])
    ).sum()
)

matricules_dn_corrige = (
    base
    .filter(
        pl.col("_dn_dt").is_not_null() &
        pl.col("dn_corrigee").is_not_null() &
        (pl.col("_dn_dt") != pl.col("dn_corrigee"))
    )
    ["matricule"].n_unique()
)

print(f"    Lignes corrigées      : {n_dn_corrige:,}")
print(f"    Matricules affectés   : {matricules_dn_corrige:,}")

# Détail pour le rapport
if n_dn_corrige > 0:
    detail_dn = (
        base
        .filter(
            pl.col("_dn_dt").is_not_null() &
            pl.col("dn_corrigee").is_not_null() &
            (pl.col("_dn_dt") != pl.col("dn_corrigee"))
        )
        .group_by(["matricule"])
        .agg([
            pl.col("_dn_dt").first().cast(pl.Utf8).alias("ancienne_valeur"),
            pl.col("dn_corrigee").first().cast(pl.Utf8).alias("nouvelle_valeur"),
            pl.len().alias("n_lignes"),
        ])
        .with_columns(pl.lit("date_naissance").alias("colonne"))
    )
    rapport_corrections.append(detail_dn)

# Écraser la date de naissance brute par la date corrigée (reformatée en String)
# On remplace aussi la colonne _dn_dt parsée
base = base.with_columns(
    pl.when(pl.col("dn_corrigee").is_not_null())
      .then(pl.col("dn_corrigee"))
      .otherwise(pl.col("_dn_dt"))
      .alias("_dn_dt")
).drop("dn_corrigee")

# Mettre à jour date_naissance textuelle à partir de la date parsée corrigée
base = base.with_columns(
    pl.when(pl.col("_dn_dt").is_not_null())
      .then(pl.col("_dn_dt").cast(pl.Utf8))
      .otherwise(pl.col("date_naissance"))
      .alias("date_naissance")
)

gc.collect()
print()


# ----- 5.3 CORRECTION DE LA PRISE DE SERVICE -----

print("  5.3 — Correction de la prise de service...", flush=True)

# Filtrer les dates de prise de service plausibles avant le vote
ps_plausible = (
    base
    .filter(
        pl.col("_ps_dt").is_not_null() &
        (pl.col("_ps_dt") >= pl.lit(PRISE_SERVICE_MIN))
    )
    .group_by(["matricule", "_ps_dt"])
    .agg(pl.len().alias("n"))
    .sort(["matricule", "n"], descending=[False, True])
    .unique(subset=["matricule"], keep="first")
    .select(["matricule", pl.col("_ps_dt").alias("ps_corrigee")])
)

base = base.join(ps_plausible, on="matricule", how="left")

n_ps_corrige = int(
    (
        base["_ps_dt"].is_not_null() &
        base["ps_corrigee"].is_not_null() &
        (base["_ps_dt"] != base["ps_corrigee"])
    ).sum()
)

matricules_ps_corrige = (
    base
    .filter(
        pl.col("_ps_dt").is_not_null() &
        pl.col("ps_corrigee").is_not_null() &
        (pl.col("_ps_dt") != pl.col("ps_corrigee"))
    )
    ["matricule"].n_unique()
)

print(f"    Lignes corrigées      : {n_ps_corrige:,}")
print(f"    Matricules affectés   : {matricules_ps_corrige:,}")

# Détail pour le rapport
if n_ps_corrige > 0:
    detail_ps = (
        base
        .filter(
            pl.col("_ps_dt").is_not_null() &
            pl.col("ps_corrigee").is_not_null() &
            (pl.col("_ps_dt") != pl.col("ps_corrigee"))
        )
        .group_by(["matricule"])
        .agg([
            pl.col("_ps_dt").first().cast(pl.Utf8).alias("ancienne_valeur"),
            pl.col("ps_corrigee").first().cast(pl.Utf8).alias("nouvelle_valeur"),
            pl.len().alias("n_lignes"),
        ])
        .with_columns(pl.lit("prise_service").alias("colonne"))
    )
    rapport_corrections.append(detail_ps)

# Appliquer la correction
base = base.with_columns(
    pl.when(pl.col("ps_corrigee").is_not_null())
      .then(pl.col("ps_corrigee"))
      .otherwise(pl.col("_ps_dt"))
      .alias("_ps_dt")
).drop("ps_corrigee")

# Mettre à jour prise_service textuelle
base = base.with_columns(
    pl.when(pl.col("_ps_dt").is_not_null())
      .then(pl.col("_ps_dt").cast(pl.Utf8))
      .otherwise(pl.col("prise_service"))
      .alias("prise_service")
)

gc.collect()
print()


# ============================================================
# ÉTAPE 6 : CONSTRUCTION DES INDICATEURS DE TRAJECTOIRE
# ============================================================

print("[6/7] Calcul des indicateurs de trajectoire...", flush=True)

# Recalculer les âges après correction
base = base.with_columns([
    ((pl.col("_ma_dt") - pl.col("_dn_dt")).dt.total_days() / DAYS_PER_YEAR)
        .round(1)
        .alias("age_a_la_periode"),
    ((pl.col("_ps_dt") - pl.col("_dn_dt")).dt.total_days() / DAYS_PER_YEAR)
        .round(1)
        .alias("anciennete_debut"),
])

# Calculer l'ancienneté à la période (mois_annee - prise_service)
base = base.with_columns(
    ((pl.col("_ma_dt") - pl.col("_ps_dt")).dt.total_days() / DAYS_PER_YEAR)
        .round(1)
        .alias("anciennete_a_la_periode")
)

# ── Trajectoire par individu ──
# Nombre de périodes observées, premier et dernier mois, postes occupés
trajectoire = (
    base
    .group_by("matricule")
    .agg([
        pl.len().alias("nb_periodes_observees"),
        pl.col("mois_annee").min().alias("premiere_periode"),
        pl.col("mois_annee").max().alias("derniere_periode"),
        pl.col("_ma_dt").min().alias("premiere_date"),
        pl.col("_ma_dt").max().alias("derniere_date"),
        # Stabilité des attributs invariants (après correction)
        pl.col("sexe").drop_nulls().n_unique().alias("n_sexe_distinct"),
        pl.col("date_naissance").drop_nulls().n_unique().alias("n_dn_distinct"),
        pl.col("prise_service").drop_nulls().n_unique().alias("n_ps_distinct"),
        # Mobilité professionnelle
        pl.col("CODE_ORGANISME").drop_nulls().n_unique().alias("n_organismes"),
        pl.col("CODE_EMPLOI").drop_nulls().n_unique().alias("n_emplois"),
        pl.col("CODE_GRADE").drop_nulls().n_unique().alias("n_grades"),
        pl.col("CODE_FONCTION").drop_nulls().n_unique().alias("n_fonctions"),
        pl.col("CODE_SERVICE").drop_nulls().n_unique().alias("n_services"),
    ])
    .with_columns(
        ((pl.col("derniere_date") - pl.col("premiere_date")).dt.total_days() / DAYS_PER_YEAR)
            .round(1)
            .alias("duree_observation_ans")
    )
    .sort("matricule")
)

# Vérification post-correction
n_encore_incoh_sexe = int((trajectoire["n_sexe_distinct"] > 1).sum())
n_encore_incoh_dn   = int((trajectoire["n_dn_distinct"] > 1).sum())
n_encore_incoh_ps   = int((trajectoire["n_ps_distinct"] > 1).sum())

print(f"  Résidus post-correction :")
print(f"    Matricules avec sexe encore incohérent       : {n_encore_incoh_sexe:,}")
print(f"    Matricules avec date_naissance encore incoh.  : {n_encore_incoh_dn:,}")
print(f"    Matricules avec prise_service encore incoh.   : {n_encore_incoh_ps:,}")
print()

print(f"  Statistiques de mobilité :")
print(f"    Agents ayant changé d'organisme  : {int((trajectoire['n_organismes'] > 1).sum()):,}")
print(f"    Agents ayant changé d'emploi     : {int((trajectoire['n_emplois'] > 1).sum()):,}")
print(f"    Agents ayant changé de grade     : {int((trajectoire['n_grades'] > 1).sum()):,}")
print(f"    Agents ayant changé de fonction  : {int((trajectoire['n_fonctions'] > 1).sum()):,}")
print(f"    Agents ayant changé de service   : {int((trajectoire['n_services'] > 1).sum()):,}")
print()


# ============================================================
# ÉTAPE 7 : ÉCRITURE DES RÉSULTATS
# ============================================================

print("[7/7] Écriture des résultats...", flush=True)
print()

# Créer le dossier local de sortie
DOSSIER_SORTIE.mkdir(parents=True, exist_ok=True)
print(f"  Dossier local : {DOSSIER_SORTIE.resolve()}")
print()

# ── 7.1 Nettoyer les colonnes temporaires et écrire le panel ──
cols_tmp = [c for c in base.columns if c.startswith("_")]
panel_clean = base.drop(cols_tmp)

# Ajouter les colonnes de trajectoire au panel
panel_clean = panel_clean.with_columns([
    base["age_a_la_periode"],
    base["anciennete_a_la_periode"],
])

# Sauvegarder localement
chemin_panel = DOSSIER_SORTIE / "panel_trajectoire_clean.parquet"
panel_clean.write_parquet(str(chemin_panel), compression="snappy")
taille_mb = chemin_panel.stat().st_size / 1024**2
print(f"  ✓ {chemin_panel.name}  ({len(panel_clean):,} lignes × {len(panel_clean.columns)} cols  {taille_mb:.1f} MB)")

# Upload S3
ecrire_parquet_s3(panel_clean, BUCKET_GOLD, f"{PREFIX_DESTINATION}/panel_trajectoire_clean.parquet")

del panel_clean
gc.collect()

# ── 7.2 Rapport des corrections ──
if rapport_corrections:
    df_rapport = pl.concat(rapport_corrections, how="diagonal")
    df_rapport = df_rapport.select([
        "colonne", "matricule", "ancienne_valeur", "nouvelle_valeur", "n_lignes"
    ]).sort(["colonne", "matricule"])

    chemin_rapport = DOSSIER_SORTIE / "rapport_corrections.csv"
    df_rapport.write_csv(str(chemin_rapport))
    print(f"  ✓ {chemin_rapport.name}  ({len(df_rapport):,} corrections)")

    # Upload S3
    csv_bytes = df_rapport.write_csv().encode("utf-8")
    s3.put_object(
        Bucket=BUCKET_GOLD,
        Key=f"{PREFIX_DESTINATION}/rapport_corrections.csv",
        Body=csv_bytes,
    )
else:
    print("  (aucune correction — données déjà cohérentes)")

# ── 7.3 Rapport des anomalies calendaires ──
if len(df_anomalies) > 0:
    chemin_anomalies = DOSSIER_SORTIE / "rapport_anomalies_calendaires.csv"
    # Convertir les colonnes date en string pour le CSV
    df_ano_export = df_anomalies.with_columns([
        pl.col("_dn_dt").cast(pl.Utf8).alias("date_naissance_parsee"),
        pl.col("_ps_dt").cast(pl.Utf8).alias("prise_service_parsee"),
        pl.col("_ma_dt").cast(pl.Utf8).alias("mois_annee_date"),
    ]).drop(["_dn_dt", "_ps_dt", "_ma_dt"])

    df_ano_export.write_csv(str(chemin_anomalies))
    print(f"  ✓ {chemin_anomalies.name}  ({len(df_anomalies):,} anomalies)")

    # Upload S3
    csv_bytes_ano = df_ano_export.write_csv().encode("utf-8")
    s3.put_object(
        Bucket=BUCKET_GOLD,
        Key=f"{PREFIX_DESTINATION}/rapport_anomalies_calendaires.csv",
        Body=csv_bytes_ano,
    )

# ── 7.4 Rapport de synthèse textuel ──
chemin_synthese = DOSSIER_SORTIE / "rapport_synthese.txt"
with open(chemin_synthese, "w", encoding="utf-8") as f:
    f.write("=" * 70 + "\n")
    f.write("RAPPORT DE SYNTHÈSE — CORRECTION GOLD TRAJECTOIRES PROFESSIONNELLES\n")
    f.write("=" * 70 + "\n")
    f.write(f"Date d'exécution : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"Source           : s3://{BUCKET_GOLD}/{KEY_PANEL_SOURCE}\n")
    f.write(f"Observations     : {n_obs:,}\n")
    f.write(f"Matricules       : {n_matricules:,}\n")
    f.write("\n")

    f.write("-" * 70 + "\n")
    f.write("DIAGNOSTIC PRE-CORRECTION\n")
    f.write("-" * 70 + "\n\n")
    for col in COLS_INVARIANTES:
        d = diagnostics[col]
        f.write(f"  {col}\n")
        f.write(f"    Matricules sans obs   : {d['n_aucune_obs']:>8,}\n")
        f.write(f"    Matricules cohérents  : {d['n_1_valeur']:>8,}\n")
        f.write(f"    Matricules incohérents: {d['n_incoherent']:>8,}  "
                f"({100*d['n_incoherent']/d['n_matricules_total']:.2f} %)\n")
        f.write("\n")

    f.write("-" * 70 + "\n")
    f.write("CORRECTIONS APPLIQUÉES\n")
    f.write("-" * 70 + "\n\n")
    f.write(f"  Sexe            : {n_sexe_corrige:>8,} lignes  ({matricules_sexe_corrige:,} matricules)\n")
    f.write(f"  Date naissance  : {n_dn_corrige:>8,} lignes  ({matricules_dn_corrige:,} matricules)\n")
    f.write(f"  Prise service   : {n_ps_corrige:>8,} lignes  ({matricules_ps_corrige:,} matricules)\n")
    f.write("\n")

    f.write("-" * 70 + "\n")
    f.write("ANOMALIES CALENDAIRES DÉTECTÉES\n")
    f.write("-" * 70 + "\n\n")
    f.write(f"  Âge à la période < {AGE_MIN_OBS}           : {n_age_lt_16:>10,}\n")
    f.write(f"  Âge à la période > {AGE_MAX_OBS}           : {n_age_gt_80:>10,}\n")
    f.write(f"  Prise service dans le futur   : {n_ps_future:>10,}\n")
    f.write(f"  Âge prise service < {AGE_MIN_PRISE_SERVICE}        : {n_aps_lt_16:>10,}\n")
    f.write(f"  Âge prise service > {AGE_MAX_PRISE_SERVICE}        : {n_aps_gt_65:>10,}\n")
    f.write(f"  Naissance >= prise service     : {n_naiss_apres_ps:>10,}\n")
    f.write(f"  Total anomalies calendaires    : {len(df_anomalies):>10,}\n")
    f.write("\n")

    f.write("-" * 70 + "\n")
    f.write("RÉSIDUS POST-CORRECTION\n")
    f.write("-" * 70 + "\n\n")
    f.write(f"  Sexe encore incohérent         : {n_encore_incoh_sexe:>8,} matricules\n")
    f.write(f"  Date naissance encore incoh.    : {n_encore_incoh_dn:>8,} matricules\n")
    f.write(f"  Prise service encore incoh.     : {n_encore_incoh_ps:>8,} matricules\n")
    f.write("\n")

    f.write("-" * 70 + "\n")
    f.write("MOBILITÉ PROFESSIONNELLE\n")
    f.write("-" * 70 + "\n\n")
    f.write(f"  Changement d'organisme  : {int((trajectoire['n_organismes'] > 1).sum()):>8,} agents\n")
    f.write(f"  Changement d'emploi     : {int((trajectoire['n_emplois'] > 1).sum()):>8,} agents\n")
    f.write(f"  Changement de grade     : {int((trajectoire['n_grades'] > 1).sum()):>8,} agents\n")
    f.write(f"  Changement de fonction  : {int((trajectoire['n_fonctions'] > 1).sum()):>8,} agents\n")
    f.write(f"  Changement de service   : {int((trajectoire['n_services'] > 1).sum()):>8,} agents\n")
    f.write("\n")

    f.write("-" * 70 + "\n")
    f.write("FICHIERS PRODUITS\n")
    f.write("-" * 70 + "\n\n")
    f.write(f"  LOCAL :\n")
    f.write(f"    {DOSSIER_SORTIE}/panel_trajectoire_clean.parquet\n")
    f.write(f"    {DOSSIER_SORTIE}/rapport_corrections.csv\n")
    f.write(f"    {DOSSIER_SORTIE}/rapport_anomalies_calendaires.csv\n")
    f.write(f"    {DOSSIER_SORTIE}/rapport_synthese.txt\n")
    f.write(f"\n")
    f.write(f"  S3 :\n")
    f.write(f"    s3://{BUCKET_GOLD}/{PREFIX_DESTINATION}/panel_trajectoire_clean.parquet\n")
    f.write(f"    s3://{BUCKET_GOLD}/{PREFIX_DESTINATION}/rapport_corrections.csv\n")
    f.write(f"    s3://{BUCKET_GOLD}/{PREFIX_DESTINATION}/rapport_anomalies_calendaires.csv\n")
    f.write("\n")
    f.write("=" * 70 + "\n")
    f.write("✓ CORRECTION TERMINÉE\n")
    f.write("=" * 70 + "\n")

print(f"  ✓ {chemin_synthese.name}")
print()


# ============================================================
# RÉSUMÉ FINAL
# ============================================================

print()
print("=" * 70)
print("RÉSUMÉ FINAL")
print("=" * 70)
print()
print(f"  Observations traitées        : {n_obs:,}")
print(f"  Matricules uniques           : {n_matricules:,}")
print()
print(f"  Corrections appliquées :")
print(f"    Sexe           : {n_sexe_corrige:>8,} lignes  ({matricules_sexe_corrige:,} matricules)")
print(f"    Date naissance : {n_dn_corrige:>8,} lignes  ({matricules_dn_corrige:,} matricules)")
print(f"    Prise service  : {n_ps_corrige:>8,} lignes  ({matricules_ps_corrige:,} matricules)")
print()
print(f"  Anomalies calendaires        : {len(df_anomalies):,} lignes")
print()
print(f"  Fichiers produits dans : {DOSSIER_SORTIE.resolve()}/")
for f_out in sorted(DOSSIER_SORTIE.iterdir()):
    taille = f_out.stat().st_size
    if taille > 1024**2:
        print(f"    {f_out.name:<45} {taille/1024**2:.1f} MB")
    else:
        print(f"    {f_out.name:<45} {taille/1024:.1f} KB")
print()
print(f"  S3 : s3://{BUCKET_GOLD}/{PREFIX_DESTINATION}/")
print()
print("=" * 70)
print("✓ CORRECTION GOLD TRAJECTOIRES PROFESSIONNELLES TERMINÉE")
print("=" * 70)
