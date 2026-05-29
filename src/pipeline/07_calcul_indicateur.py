# ============================================================
# PANEL ADMIN — ÉTAPE 7 : CALCUL DES STATUTS BARÈME
# ============================================================
# Calcule deux statuts de conformité au barème pour chaque agent
# fonctionnaire à partir du Gold panel et du référentiel anstat_2025.
#
# Méthodo 1 — Statut (variable : montant_brut_sans_ind_sans_alloc)
#   Salaire sans indemnités et sans allocation familiale
#   (codes postes 124 + 228 + 271 + 425)
#   Référence anstat_2025 : Salaire de Base + Résidence + Logement + Transport
#   → montant_brut_sans_indemnites_sans_alloc
#
# Méthodo 2 — Statut_Total (variable : montant_brut_total)
#   Salaire brut total Feuille 1, tous codes postes confondus
#   Référence anstat_2025 : colonne "montant brut"
#   → montant_brut_total_ref
#
# Pour chaque méthodo :
#   - Calcul min/max par cluster (secteur × grade) depuis anstat_2025
#   - Jointure sur le Gold panel
#   - Classification : Conforme / Inférieur / Supérieur / Cluster non déterminé
#   - Récupération ±5% : agents légèrement hors-barème reclassés Conforme
#   - Distribution et tranches d'écart affichées avant et après récupération
#
# Prérequis : 06_compiler_panel.py doit avoir été exécuté.
#
# Paramètres configurables :
#   MODE_SALAIRE_BRUT : True  = agrégation par cle_unique (salaire brut)
#                       False = agrégation par matricule (revenu salarial)
#   PERIODE_RECENTE   : True  = années récentes uniquement (2024-2025)
#                       False = période complète (2015 → dernière année)
#   INCLURE_ZEROS     : True  = inclure salaires = 0 ou NA
#                       False = salaire strictement > 0
#
# Sortie  : s3://gold/panel_admin/base_avec_statut.parquet
#           (contient Statut + Statut_Total + min/max bornes des deux méthodos)
#
# Pipeline :
#   07  (ce script) → Statut (M1) + Statut_Total (M2)  → base_avec_statut.parquet
#   07a             → Imputation hors-barème M1 + M2   → base_avec_statut_corrigee.parquet
#   07b             → Indicateurs salaires M1 + M2     → indicateurs_*.xlsx
#
# Dépendances :
#   pip install polars boto3 python-dotenv openpyxl
# ============================================================

import gc
import io
import json
import os
import re
import tempfile
import unicodedata
from pathlib import Path
from common.s3_client import create_s3_client
from common.logging_config import setup_pipeline_logging
from common.polars_s3 import lire_parquet_s3
import polars as pl
import yaml
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter

# ============================================================
# PARAMÈTRES DE CONFIGURATION
# ============================================================

CONFIG_PATH = Path(__file__).parent / "config" / "pipeline_settings.yaml"

DEFAULT_METHODOLOGY = {
    "mode_salaire_brut": True,
    "periode_recente": True,
    "inclure_zeros": True,
    "seuil_wins": 0.01,
    "min_obs_wins": 100,
}


def load_methodology_settings() -> dict:
    if not CONFIG_PATH.exists():
        return DEFAULT_METHODOLOGY.copy()
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}
    settings = DEFAULT_METHODOLOGY.copy()
    settings.update(config.get("methodology", {}) or {})
    return settings


METHODOLOGY = load_methodology_settings()

# True = Salaire brut (cle_unique) | False = Revenu salarial (matricule)
MODE_SALAIRE_BRUT = bool(METHODOLOGY["mode_salaire_brut"])

# True = 2024-2025 | False = 2015 → dernière année disponible
PERIODE_RECENTE = bool(METHODOLOGY["periode_recente"])

# True = inclure zéros | False = salaire strictement > 0
INCLURE_ZEROS = bool(METHODOLOGY["inclure_zeros"])

# Seuil de winsorisation (ex: 0.01 = écrêtage à P1/P99), None pour désactiver.
SEUIL_WINS = METHODOLOGY["seuil_wins"]
if SEUIL_WINS is not None:
    SEUIL_WINS = float(SEUIL_WINS)

# Effectif minimum par cellule mois × grade × CITP avant repli mois × grade.
MIN_OBS_WINS = int(METHODOLOGY["min_obs_wins"])

# ============================================================

BUCKET_GOLD    = os.getenv("BUCKET_GOLD",    "gold")
BUCKET_STAGING = os.getenv("BUCKET_STAGING", "staging")
PREFIX_PANEL   = "panel_admin"
PREFIX_EXPORTS = "panel_admin/exports_gold"
KEY_CORRESPONDANCE = "panel_admin/references/CORRESPONDANCE_GRILLE_EMPLOI.xlsx"

_mode_label    = "salaire_brut"  if MODE_SALAIRE_BRUT else "revenu_salarial"
_periode_label = "2024_2025"     if PERIODE_RECENTE   else "2015_recent"
_zeros_label   = "avec_zeros"    if INCLURE_ZEROS     else "sans_zeros"

# Libellés mois français
MOIS_FR = {
    1: "Janvier",  2: "Février",   3: "Mars",      4: "Avril",
    5: "Mai",      6: "Juin",      7: "Juillet",   8: "Août",
    9: "Septembre",10: "Octobre",  11: "Novembre", 12: "Décembre",
}

GRADES_VALIDES = list(METHODOLOGY["grades_valides"])

# Composantes du montant_brut_sans_ind_sans_alloc (codes F2, sans 421 allocation familiale)
COMPOSANTES_SALAIRE_NOMS = {"124_F2", "228_F2", "271_F2", "425_F2"}

# Codes poste éligibles pour montant_brut_sans_ind_sans_alloc (sans 421 allocation familiale)
CODES_POSTE_ELIGIBLES = {"124", "228", "271", "425"}

# Code organisme à exclure de tous les traitements
CODE_ORGANISME_EXCLUS = "59"

# Fichier de référence barème 2025 (dans staging/panel_admin/references/)
KEY_ANSTAT_2025 = "panel_admin/references/anstat_2025.txt.xlsx"

# Clé de sortie base_avec_statut
KEY_BASE_AVEC_STATUT = f"{PREFIX_PANEL}/base_avec_statut.parquet"
KEY_PARAMETRES_METHODO = f"{PREFIX_EXPORTS}/parametres_methodologiques_07.json"

# --- CLIENT S3 ---
s3 = create_s3_client()
logger, _ = setup_pipeline_logging("07_calcul_indicateur")

logger.info("=" * 70)
logger.info("STATUT BARÈME — Calcul du statut (Conforme/Inférieur/Supérieur)")
logger.info(f"  Période : {'2024-2025' if PERIODE_RECENTE else '2015 → récent'}")
logger.info(
    "Paramètres méthodologiques",
    extra={
        "config_path": str(CONFIG_PATH),
        "mode_salaire_brut": MODE_SALAIRE_BRUT,
        "periode_recente": PERIODE_RECENTE,
        "inclure_zeros": INCLURE_ZEROS,
        "seuil_wins": SEUIL_WINS,
        "min_obs_wins": MIN_OBS_WINS,
    },
)
logger.info("=" * 70)

s3.put_object(
    Bucket=BUCKET_STAGING,
    Key=KEY_PARAMETRES_METHODO,
    Body=json.dumps(
        {
            "script": "07_calcul_indicateur.py",
            "config_path": str(CONFIG_PATH),
            "methodology": {
                "mode_salaire_brut": MODE_SALAIRE_BRUT,
                "periode_recente": PERIODE_RECENTE,
                "inclure_zeros": INCLURE_ZEROS,
                "seuil_wins": SEUIL_WINS,
                "min_obs_wins": MIN_OBS_WINS,
            },
        },
        ensure_ascii=False,
        indent=2,
    ).encode("utf-8"),
)
logger.info(f"Paramètres méthodologiques sauvegardés : s3://{BUCKET_STAGING}/{KEY_PARAMETRES_METHODO}")

# ============================================================
# FONCTIONS UTILITAIRES
# ============================================================

# ============================================================
# CONSTANTES BARÈME
# ============================================================

# Pour ENS SEC & PRIM : grades B/C/D = Primaire, grades A = Secondaire
GRADES_PRIMAIRE = {"B3", "C3", "D1", "D2"}

# Ordre d'affichage dans la feuille MODELE_ANSTAT
ORDRE_MODELE_ANSTAT = [
    ("TOUS SECTEURS CONFONDUS",                       "_GLOBAL_"),
    ("SECTEUR EDUCATION",                             None),
    ("Primaire",                                      "ENS SEC & PRIM_PRIM"),
    ("Secondaire",                                    "ENS SEC & PRIM_SEC"),
    ("Supérieur",                                     "ENS SUP"),
    ("SECTEUR SANTE",                                 None),
    ("Cadres supérieurs",                             "Cadre-Sup-Santé Generaliste & Specialiste"),
    ("Personnels Techniques",                         "Tech-Santé"),
    ("AUTRES SECTEURS",                               None),
    ("Para-militaire",                                 "para militaire"),
    ("Barème Général",                                "Barême Général"),
    ("PERSONNELS SOUS STATUT PARTICULIER",            None),
    ("Magistrat",                                     "MAGISTRAT "),
    ("Corps préfectoral",                             "CORPS PREF"),
    ("Corps diplomatique",                            "CORPS DIPL"),
    ("Greffier",                                      "GREFFE"),
]


def normaliser_pour_matching(texte) -> str | None:
    """Normalisation textuelle pour fuzzy join — identique à l'étape 03."""
    if texte is None:
        return None
    texte = str(texte).strip()
    if not texte:
        return None
    texte = re.sub(r"[-]", " ", texte)
    texte = re.sub(r"[.,;:/()\\[\]]", " ", texte)
    texte = re.sub(r"[''’]", " ", texte)
    texte = texte.upper()
    texte = unicodedata.normalize("NFKD", texte)
    texte = "".join(c for c in texte if not unicodedata.combining(c))
    texte = re.sub(r"\s+", " ", texte).strip()
    return texte if texte else None


def corriger_bareme_emploi(emploi_norm: str | None, bareme_actuel: str | None) -> str | None:
    """
    Corrige le BAREME d'un emploi uniquement si celui-ci vaut 'Barême Général'.
    - 'informaticien' ou 'informatique' → 'Informaticien'
    - 'secretaire', 'secr.', 'secr '    → 'SECRETAIRE DIRECTION'
    """
    if bareme_actuel != "Barême Général":
        return bareme_actuel
    if emploi_norm is None:
        return bareme_actuel
    if "INFORMATICIEN" in emploi_norm or "INFORMATIQUE" in emploi_norm:
        return "Informaticien"
    if "SECRETAIRE" in emploi_norm or "SECR " in emploi_norm or emploi_norm.startswith("SECR"):
        return "SECRETAIRE DIRECTION"
    return bareme_actuel


def charger_table_bareme() -> pl.DataFrame | None:
    """
    Charge la table CORRESPONDANCE_GRILLE_EMPLOI depuis staging et
    renvoie une table de jointure (emploi_norm → bareme, grade_ref).
    Renvoie None si le fichier est absent (étape non bloquante).
    """
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        s3.download_file(BUCKET_STAGING, KEY_CORRESPONDANCE, tmp_path)
        df = pl.read_excel(tmp_path)
        df = df.rename({
            "BAREME / GRILLE SALARIALE": "bareme",
            "GRADE": "grade_ref",
            "EMPLOI": "emploi_ref",
        })
        df = df.with_columns(
            pl.col("emploi_ref")
            .map_elements(normaliser_pour_matching, return_dtype=pl.Utf8)
            .alias("emploi_ref_norm")
        )

        df = df.with_columns(
            pl.struct(["emploi_ref_norm", "bareme"])
            .map_elements(
                lambda r: corriger_bareme_emploi(r["emploi_ref_norm"], r["bareme"]),
                return_dtype=pl.Utf8
            )
            .alias("bareme")
        )

        n_info = int((df["bareme"] == "Informaticien").sum())
        n_sec  = int((df["bareme"] == "SECRETAIRE DIRECTION").sum())
        if n_info > 0:
            logger.info(f"  ✓ Correction BAREME → Informaticien      : {n_info} emplois")
        if n_sec > 0:
            logger.info(f"  ✓ Correction BAREME → SECRETAIRE DIRECTION : {n_sec} emplois")

        table = (
            df.filter(pl.col("emploi_ref_norm").is_not_null())
            .select(["emploi_ref_norm", "bareme", "grade_ref"])
            .unique(subset=["emploi_ref_norm"])
        )
        logger.info(f"  ✓ Table barème chargée : {len(table)} emplois")
        return table
    except Exception as e:
        logger.warning(f"  ⚠️  Table barème non disponible ({e}) — feuilles barème ignorées")
        return None
    finally:
        os.unlink(tmp_path)


def fuzzy_join_bareme(df: pl.DataFrame, table_join: pl.DataFrame,
                      col_emploi: str) -> pl.DataFrame:
    """Ajoute les colonnes 'bareme' et 'grade_ref' via fuzzy join sur emploi normalisé."""
    df = df.with_columns(
        pl.col(col_emploi)
        .map_elements(normaliser_pour_matching, return_dtype=pl.Utf8)
        .alias("_emploi_norm")
    )
    df = df.join(table_join, left_on="_emploi_norm", right_on="emploi_ref_norm",
                 how="left").drop("_emploi_norm")
    nb_match  = int(df["bareme"].is_not_null().sum())
    taux      = 100 * nb_match / len(df)
    logger.info(f"  ✓ Fuzzy join barème : {nb_match:,} / {len(df):,} ({taux:.1f}%)")
    if taux < 20:
        logger.warning("  ⚠️  Matching faible — vérifiez CORRESPONDANCE_GRILLE_EMPLOI.xlsx")
    return df


def ajouter_bareme_detail(df: pl.DataFrame, col_grade: str) -> pl.DataFrame:
    """Scinde ENS SEC & PRIM en Primaire (B/C/D) et Secondaire (A)."""
    return df.with_columns(
        pl.when(
            (pl.col("bareme") == "ENS SEC & PRIM")
            & pl.col(col_grade).is_in(GRADES_PRIMAIRE)
        ).then(pl.lit("ENS SEC & PRIM_PRIM"))
        .when(
            (pl.col("bareme") == "ENS SEC & PRIM")
            & ~pl.col(col_grade).is_in(GRADES_PRIMAIRE)
        ).then(pl.lit("ENS SEC & PRIM_SEC"))
        .otherwise(pl.col("bareme"))
        .alias("bareme_detail")
    )


def grade_order_value(grade: str) -> int:
    """Valeur ordinale d'un grade pour sélectionner le grade principal."""
    if re.match(r"^[ABCD][0-9]$", grade):
        base = {"A": 80, "B": 70, "C": 60, "D": 50}[grade[0]]
        return base + int(grade[1])
    return 0


def charger_anstat_2025() -> pl.DataFrame | None:
    """
    Charge le fichier anstat_2025.txt.xlsx depuis staging/referentiel/,
    crée la colonne montant_brut_sans_indemnites_sans_alloc =
        Salaire de Base + Résidence + Logement + Transport
    Cohérence exacte avec montant_brut_sans_ind_sans_alloc côté Gold
    (codes 124+228+271+425, sans allocation familiale code 421).
    Retourne None si le fichier est absent.
    """
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        s3.download_file(BUCKET_STAGING, KEY_ANSTAT_2025, tmp_path)
        df = pl.read_excel(tmp_path)
        logger.info(f"   ✓ anstat_2025.txt.xlsx chargé : {len(df):,} lignes × {len(df.columns)} cols")
        logger.info(f"     Colonnes : {df.columns}")
    except Exception as e:
        logger.warning(f"   ⚠️  Fichier anstat_2025.txt.xlsx introuvable ({e}) — étape barème ignorée")
        return None
    finally:
        os.unlink(tmp_path)

    # Composantes du salaire de référence = codes Gold 124+228+271+425
    # (Allocation familiale code 421 exclue pour l'audit de conformité)
    patterns_composantes = {
        "Salaire de Base": [r"salaire.*base", r"base.*salaire", r"traitement.*base",
                            r"^salaire$", r"^base$"],
        "Résidence":       [r"r[eé]sidence", r"indem.*r[eé]s"],
        "Logement":        [r"logement", r"indem.*log"],
        "Transport":       [r"transport", r"indem.*transp"],
    }

    def trouver_colonne(cols: list[str], patterns: list[str]) -> str | None:
        for col in cols:
            col_norm = unicodedata.normalize("NFKD", col.lower())
            col_norm = "".join(c for c in col_norm if not unicodedata.combining(c))
            for pat in patterns:
                if re.search(pat, col_norm, re.IGNORECASE):
                    return col
        return None

    composantes_trouvees = {}
    for nom, pats in patterns_composantes.items():
        col = trouver_colonne(df.columns, pats)
        if col:
            composantes_trouvees[nom] = col
            logger.info(f"     {nom} → {col}")
        else:
            logger.warning(f"     ⚠️  Composante '{nom}' non trouvée dans le fichier")

    if not composantes_trouvees:
        logger.warning("   ⚠️  Aucune composante de salaire identifiée dans anstat_2025")
        return None

    df = df.with_columns([
        pl.col(c).cast(pl.Float64, strict=False)
        for c in composantes_trouvees.values()
    ])
    df = df.with_columns(
        pl.sum_horizontal([pl.col(c) for c in composantes_trouvees.values()])
        .alias("montant_brut_sans_indemnites_sans_alloc")
    )

    n_non_null = int(df["montant_brut_sans_indemnites_sans_alloc"].is_not_null().sum())
    composantes_label = " + ".join(composantes_trouvees.keys())
    logger.info(f"   montant_brut_sans_indemnites_sans_alloc ({composantes_label}) : {n_non_null:,} valeurs")

    # Montant brut TOTAL (tous codes postes) pour Méthodo 2
    # Correspond à la colonne "Montant brut" du fichier anstat_2025
    patterns_brut_total = [r"montant.*brut$", r"^brut$", r"salaire.*brut$", r"brut.*total"]
    col_brut_total = trouver_colonne(df.columns, patterns_brut_total)
    if col_brut_total and col_brut_total != "montant_brut_sans_indemnites_sans_alloc":
        df = df.with_columns(
            pl.col(col_brut_total).cast(pl.Float64, strict=False).alias("montant_brut_total_ref")
        )
        n_bt = int(df["montant_brut_total_ref"].is_not_null().sum())
        logger.info(f"   montant_brut_total_ref (Méthodo 2, col='{col_brut_total}') : {n_bt:,} valeurs")
    else:
        logger.warning(
            f"   ⚠️  Colonne 'Montant brut total' non trouvée dans anstat_2025 "
            f"— Statut_Total ne sera pas calculé"
        )
    return df


def calculer_indicateurs_cluster(
    df_ref: pl.DataFrame,
    value_col: str = "montant_brut_sans_indemnites_sans_alloc",
    prefix: str = "bareme",
) -> pl.DataFrame | None:
    """
    Calcule min et max de `value_col` par cluster (secteur × grade)
    dans le fichier de référence anstat_2025.

    Parameters
    ----------
    value_col : colonne de référence à utiliser (montant_brut_sans_indemnites_sans_alloc
                pour Méthodo 1 / montant_brut_total_ref pour Méthodo 2)
    prefix    : préfixe des colonnes de sortie min/max
                → "bareme" produit min_bareme / max_bareme (compat 07a)
                → "total"  produit min_total  / max_total
    """
    if value_col not in df_ref.columns:
        return None

    cols_ref = df_ref.columns

    def trouver_col(cols, patterns):
        for pat in patterns:
            for col in cols:
                col_n = unicodedata.normalize("NFKD", col.lower())
                col_n = "".join(c for c in col_n if not unicodedata.combining(c))
                if re.search(pat, col_n, re.IGNORECASE):
                    return col
        return None

    col_secteur = trouver_col(cols_ref, [r"secteur", r"bareme", r"grille", r"type"])
    col_grade   = trouver_col(cols_ref, [r"^grade$", r"classe.*echelon", r"echelon",
                                          r"niveau", r"categorie"])

    cluster_cols = []
    if col_secteur:
        cluster_cols.append(col_secteur)
        logger.info(f"   Colonne secteur : {col_secteur}")
    else:
        logger.warning("   ⚠️  Colonne secteur non trouvée — agrégation sur grade uniquement")

    if col_grade:
        cluster_cols.append(col_grade)
        logger.info(f"   Colonne grade   : {col_grade}")
    else:
        logger.warning("   ⚠️  Colonne grade non trouvée — agrégation globale")

    if not cluster_cols:
        cluster_cols = [df_ref.columns[0]]
        logger.info(f"   Repli sur première colonne : {cluster_cols[0]}")

    indic = (
        df_ref
        .filter(pl.col(value_col).is_not_null() & (pl.col(value_col) > 0))
        .group_by(cluster_cols)
        .agg([
            pl.col(value_col).min().round(0).alias(f"min_{prefix}"),
            pl.col(value_col).max().round(0).alias(f"max_{prefix}"),
            pl.col(value_col).mean().round(0).alias(f"moy_{prefix}"),
            pl.col(value_col).median().round(0).alias(f"med_{prefix}"),
            pl.len().alias("nb_obs_ref"),
        ])
        .sort(cluster_cols)
    )

    rename_map = {}
    if col_secteur:
        rename_map[col_secteur] = "secteur_ref"
    if col_grade:
        rename_map[col_grade] = "grade_ref"
    if rename_map:
        indic = indic.rename(rename_map)

    logger.info(f"   Indicateurs par cluster : {len(indic):,} lignes")
    return indic, col_secteur, col_grade


def _print_tranches(
    df: "pl.DataFrame",
    statut_col: str,
    value_col: str,
    min_col: str,
    max_col: str,
    avant_recuperation: bool = True,
) -> None:
    """
    Affiche les stats d'écart par tranches pour les agents hors-barème.
    - avant_recuperation=True  → note "← seront récupérés" sur la tranche < 5%
    - avant_recuperation=False → la tranche < 5% est vide (déjà récupérés)
    """
    STATUTS_HORS  = ["Inférieur au minimum du barème", "Supérieur au maximum du barème"]
    ORDRE_TRANCHES = ["< 5%", "5% – 10%", "10% – 25%", "25% – 50%", "≥ 50%"]

    cols_ok = {value_col, min_col, max_col, statut_col}.issubset(set(df.columns))
    if not cols_ok:
        return

    hors = df.filter(pl.col(statut_col).is_in(STATUTS_HORS))
    if len(hors) == 0:
        logger.info(f"   → Aucun agent hors-barème")
        return

    for statut_label, borne_col in [
        ("Inférieur au minimum du barème", min_col),
        ("Supérieur au maximum du barème", max_col),
    ]:
        sous = hors.filter(pl.col(statut_col) == statut_label)
        n_sous = len(sous)
        if n_sous == 0:
            continue
        if statut_label == "Inférieur au minimum du barème":
            ecart_expr = (pl.col(borne_col) - pl.col(value_col)) / pl.col(borne_col) * 100
        else:
            ecart_expr = (pl.col(value_col) - pl.col(borne_col)) / pl.col(borne_col) * 100
        sous = (
            sous
            .with_columns(ecart_expr.alias("_ecart_pct"))
            .with_columns(
                pl.when(pl.col("_ecart_pct") < 5) .then(pl.lit("< 5%"))
                .when(pl.col("_ecart_pct") < 10)  .then(pl.lit("5% – 10%"))
                .when(pl.col("_ecart_pct") < 25)  .then(pl.lit("10% – 25%"))
                .when(pl.col("_ecart_pct") < 50)  .then(pl.lit("25% – 50%"))
                .otherwise(pl.lit("≥ 50%"))
                .alias("_tranche")
            )
        )
        counts = {
            row["_tranche"]: row["count"]
            for row in sous["_tranche"].value_counts().iter_rows(named=True)
        }
        logger.info(f"     Tranches d'écart — {statut_label} ({n_sous:,} agents) :")
        for t in ORDRE_TRANCHES:
            n_t = counts.get(t, 0)
            pct = 100 * n_t / n_sous if n_sous > 0 else 0.0
            if t == "< 5%" and n_t > 0:
                note = "  ← seront récupérés (règle ±5%)" if avant_recuperation else "  ← vide (déjà récupérés)"
            else:
                note = ""
            logger.info(f"       {t:<12} : {n_t:>10,}  ({pct:.1f}%){note}")


# ============================================================
# 1. CHARGEMENT
# ============================================================

logger.info("1. Chargement des fichiers Gold panel...")

pages_panel = s3.get_paginator("list_objects_v2").paginate(
    Bucket=BUCKET_GOLD, Prefix=PREFIX_PANEL
)
cles_panel = sorted([
    obj["Key"]
    for page in pages_panel
    for obj in page.get("Contents", [])
    if re.search(r"panel_20\d{2}\.parquet$", obj["Key"])
])

logger.info(f"   Fichiers panel trouvés : {len(cles_panel)}")

frames = []
for cle in cles_panel:
    df_part = lire_parquet_s3(BUCKET_GOLD, cle)
    frames.append(df_part)
    logger.info(f"   Lecture {os.path.basename(cle)} ✓ {len(df_part):,} lignes")

base = pl.concat(frames, how="diagonal")
del frames
gc.collect()
logger.info(f"   ✓ Total : {len(base):,} lignes × {len(base.columns)} colonnes")

# Exclusion défensive de l'organisme 59
if "CODE_ORGANISME" in base.columns:
    n_avant = len(base)
    base = base.filter(pl.col("CODE_ORGANISME").cast(pl.Utf8) != CODE_ORGANISME_EXCLUS)
    n_exclu = n_avant - len(base)
    if n_exclu > 0:
        logger.info(f"   Exclusion organisme {CODE_ORGANISME_EXCLUS} : {n_exclu:,} lignes supprimées")
logger.info(f"   Base finale : {len(base):,} lignes")

# ============================================================
# 2. VÉRIFICATION GRADE ET GRADE_SOURCE
# ============================================================

logger.info("2. Vérification des colonnes GRADE et GRADE_SOURCE...")

if "GRADE" not in base.columns or "GRADE_SOURCE" not in base.columns:
    raise RuntimeError(
        "Colonnes GRADE et/ou GRADE_SOURCE absentes du Gold.\n"
        "Exécutez d'abord 06b_imputer_grades.py avant ce script."
    )

n_total = len(base)
for source in ["OBSERVE", "IMPUTE", "NF_NON_IMPUTABLE"]:
    n = int((base["GRADE_SOURCE"] == source).sum())
    logger.info(f"   {source:<20} : {n:,}  ({100*n/n_total:.1f}%)")

# ============================================================
# 3. EXTRACTION ANNÉE / MOIS depuis mois_annee (format MMYYYY)
# ============================================================

logger.info("3. Extraction année/mois depuis mois_annee...")

# mois_annee est au format MMYYYY (ex. "012015")
base = base.with_columns([
    pl.col("mois_annee").str.slice(2, 4).cast(pl.Int32, strict=False).alias("ANNEE"),
    pl.col("mois_annee").str.slice(0, 2).cast(pl.Int32, strict=False).alias("MOIS_NUM"),
])

annees_dispo = sorted(base["ANNEE"].drop_nulls().unique().to_list())
logger.info(f"   Années disponibles : {annees_dispo}")

ANNEES_ANALYSE = [a for a in annees_dispo if a >= 2024] if PERIODE_RECENTE else [a for a in annees_dispo if a >= 2015]
logger.info(f"   Période retenue    : {ANNEES_ANALYSE}")

base = base.filter(pl.col("ANNEE").is_in(ANNEES_ANALYSE))
logger.info(f"   Filtrage           : {len(base):,} lignes")

if len(base) == 0:
    raise ValueError(f"Aucune donnée pour les années {ANNEES_ANALYSE}")

base = base.with_columns(
    pl.col("MOIS_NUM")
    .map_elements(lambda m: MOIS_FR.get(m, str(m) if m is not None else ""), return_dtype=pl.Utf8)
    .alias("MOIS")
)

# ============================================================
# 4. VÉRIFICATION montant_brut_sans_ind_sans_alloc
# ============================================================

logger.info("4. Vérification montant_brut_sans_ind_sans_alloc (pré-calculé en étape 06)...")

if "montant_brut_sans_ind_sans_alloc" not in base.columns:
    raise RuntimeError(
        "Colonne montant_brut_sans_ind_sans_alloc absente du Gold panel.\n"
        "Vérifiez que l'étape 06_compiler_panel.py a été exécutée avec les colonnes F2."
    )

# Cast en Float64 sur place (la colonne peut arriver en Int64 depuis le parquet)
base = base.with_columns(
    pl.col("montant_brut_sans_ind_sans_alloc").cast(pl.Float64, strict=False)
)

n_total  = len(base)
n_pos    = int((base["montant_brut_sans_ind_sans_alloc"] > 0).sum())
n_nul    = int(base["montant_brut_sans_ind_sans_alloc"].is_null().sum())
n_zero   = int((base["montant_brut_sans_ind_sans_alloc"] == 0).sum())
n_neg    = int((base["montant_brut_sans_ind_sans_alloc"] < 0).sum())
logger.info(f"   Source : montant_brut_sans_ind_sans_alloc (postes 124+228+271+425, sans allocation familiale 421, pré-agrégé en 06)")
logger.info(f"   Salaires > 0  : {n_pos:,} ({100*n_pos/n_total:.1f}%)")
logger.info(f"   Nuls (null)   : {n_nul:,} ({100*n_nul/n_total:.1f}%)")
logger.info(f"   Zéros (= 0)   : {n_zero:,} ({100*n_zero/n_total:.1f}%)")
logger.info(f"   Négatifs (< 0): {n_neg:,} ({100*n_neg/n_total:.1f}%)")

# ============================================================
# 5. STANDARDISATION DU SEXE
# ============================================================

logger.info("5. Standardisation sexe...")

if "sexe" in base.columns:
    base = base.with_columns(
        pl.col("sexe").cast(pl.Utf8)
        .str.to_uppercase()
        .str.strip_chars()
        .map_elements(
            lambda s: (
                "Homme" if s in {"MASCULIN", "M", "H", "HOMME", "MALE", "1"} else
                "Femme" if s in {"FEMININ", "F", "FEMME", "FEMALE", "2",
                                 "FÉMININ", "FEMININE"} else
                "Inconnu"
            ),
            return_dtype=pl.Utf8
        )
        .alias("SEXE_STD")
    )
    n_inconnu = int((base["SEXE_STD"] == "Inconnu").sum())
    if n_inconnu > 0:
        base = base.filter(pl.col("SEXE_STD") != "Inconnu")
        logger.info(f"   Exclusion sexe inconnu : {n_inconnu:,}")
    for sexe_val in ["Homme", "Femme"]:
        n = int((base["SEXE_STD"] == sexe_val).sum())
        logger.info(f"   {sexe_val} : {n:,}")
else:
    base = base.with_columns(pl.lit("Non renseigné").alias("SEXE_STD"))

# ============================================================
# 6. DÉTECTION DES MULTI-POSTES
# ============================================================

logger.info("6. Détection multi-postes (par matricule)...")

if "NB_POSTES" in base.columns:
    total_multi  = int((base["NB_POSTES"] > 1).sum())
    total_agents = len(base)
    logger.info(f"   NB_POSTES issu du Gold (pré-calculé en étape 06)")
    logger.info(f"   Multi-postes : {total_multi:,} / {total_agents:,} "
                f"({100*total_multi/total_agents:.1f}%)")
elif "matricule" in base.columns:
    logger.warning("   ⚠️  NB_POSTES absent du Gold — recalcul depuis les lignes")
    nb_postes = (
        base.group_by(["mois_annee", "matricule"])
        .agg(pl.len().alias("NB_POSTES"))
    )
    base = base.join(nb_postes, on=["mois_annee", "matricule"], how="left")
    total_multi  = int(
        base.filter(pl.col("NB_POSTES") > 1)
        .select(["mois_annee", "matricule"]).unique().select(pl.len()).item()
    )
    total_agents = int(
        base.select(["mois_annee", "matricule"]).unique().select(pl.len()).item()
    )
    logger.info(f"   Multi-postes : {total_multi:,} / {total_agents:,} "
                f"({100*total_multi/total_agents:.1f}%)")
else:
    base = base.with_columns(pl.lit(1).alias("NB_POSTES"))
    logger.info("   Colonne matricule absente, NB_POSTES = 1")

# ============================================================
# 7. FILTRAGE SUR GRADES VALIDES
# ============================================================

logger.info("7. Filtrage grades valides avant agrégation...")

if INCLURE_ZEROS:
    base_gv = base.filter(pl.col("GRADE").is_in(GRADES_VALIDES))
else:
    base_gv = base.filter(
        pl.col("GRADE").is_in(GRADES_VALIDES) & (pl.col("montant_brut_sans_ind_sans_alloc") > 0)
    )

n_gv = len(base_gv)
for source in ["OBSERVE", "IMPUTE"]:
    n = int((base_gv["GRADE_SOURCE"] == source).sum())
    logger.info(f"   {source:<10} : {n:,}  ({100*n/n_gv:.1f}%)")
logger.info(f"   Total retenu : {n_gv:,} ({100*n_gv/len(base):.1f}%)")

# ============================================================
# 7b. FUZZY JOIN EMPLOI → BARÈME
# ============================================================

logger.info("7b. Fuzzy join EMPLOI → BARÈME (via table de correspondance)...")

TABLE_BAREME = charger_table_bareme()

CANDIDATES_EMPLOI = ["emploi", "CODE_EMPLOI", "EMPLOI", "corps", "CORPS"]
col_emploi_panel = next(
    (c for c in CANDIDATES_EMPLOI if c in base_gv.columns), None
)

if TABLE_BAREME is not None and col_emploi_panel is not None:
    base_gv = fuzzy_join_bareme(base_gv, TABLE_BAREME, col_emploi_panel)
    col_grade_bareme = "GRADE" if "GRADE" in base_gv.columns else "grade_ref"
    if col_grade_bareme in base_gv.columns:
        base_gv = ajouter_bareme_detail(base_gv, col_grade_bareme)
    else:
        base_gv = base_gv.with_columns(pl.col("bareme").alias("bareme_detail"))
    BAREME_DISPONIBLE = True
else:
    if TABLE_BAREME is None:
        logger.warning("  ⚠️  Table barème absente — feuilles barème non produites")
    else:
        logger.warning(f"  ⚠️  Colonne emploi absente dans le panel — feuilles barème non produites")
    BAREME_DISPONIBLE = False

# --- Exclusion police (non-fonctionnaires — pas de statut barème) ---
if "bareme" in base_gv.columns:
    n_police = int((base_gv["bareme"] == "police").sum())
    base_gv = base_gv.filter(pl.col("bareme") != "police")
    logger.info(f"   Exclusion police (NF) : {n_police:,} agents retirés")

# --- Exclusion montant_brut_sans_ind_sans_alloc = 0 ou null ---
if "montant_brut_sans_ind_sans_alloc" in base_gv.columns:
    n_zero = int(
        (base_gv["montant_brut_sans_ind_sans_alloc"].is_null() | (base_gv["montant_brut_sans_ind_sans_alloc"] == 0)).sum()
    )
    base_gv = base_gv.filter(
        pl.col("montant_brut_sans_ind_sans_alloc").is_not_null() & (pl.col("montant_brut_sans_ind_sans_alloc") != 0)
    )
    logger.info(f"   Exclusion sans salaire (montant_brut_sans_ind_sans_alloc ≤ 0) : {n_zero:,} agents retirés")

logger.info(f"   Base après exclusions : {len(base_gv):,} agents")

# ============================================================
# 7c. STATUT BARÈME — MÉTHODO 1
# ============================================================

logger.info("7c. Statut (Méthodo 1)")
logger.info("   Objectif  : vérifier si le salaire de base de l'agent (sans indemnités ni")
logger.info("               allocation familiale) est conforme au barème de son grade et secteur.")
logger.info("   Gold      : montant_brut_sans_ind_sans_alloc  (codes postes 124+228+271+425)")
logger.info("   Référence : montant_brut_sans_indemnites_sans_alloc  (Salaire de Base + Résidence")
logger.info("               + Logement + Transport  —  source anstat_2025)")

logger.info("  7c-1. Chargement du référentiel anstat_2025...")
df_anstat_ref   = charger_anstat_2025()
indic_cluster   = None
col_secteur_ref = None
col_grade_ref   = None

if df_anstat_ref is not None and "montant_brut_sans_indemnites_sans_alloc" in df_anstat_ref.columns:
    logger.info("  7c-2. Calcul des bornes de conformité par cluster (secteur × grade) depuis anstat_2025...")
    result_cluster = calculer_indicateurs_cluster(
        df_anstat_ref, value_col="montant_brut_sans_indemnites_sans_alloc", prefix="bareme"
    )
    if result_cluster is not None:
        indic_cluster, col_secteur_ref, col_grade_ref = result_cluster

base_avec_statut = pl.DataFrame()
# Initialisations — utilisées en 7c-3 ET en 7d (Statut_Total)
col_secteur_main = next(
    (c for c in ["bareme", "organisme", "CODE_ORGANISME"] if c in base_gv.columns), None
)
col_grade_main = next(
    (c for c in ["GRADE", "CODE_GRADE", "grade"] if c in base_gv.columns), None
)

if indic_cluster is not None and "montant_brut_sans_ind_sans_alloc" in base_gv.columns:
    logger.info("  7c-3. Attribution du Statut à chaque agent — comparaison au barème de son cluster (secteur × grade)...")

    MAPPING_BAREME_ANSTAT = {
        "Cadre-Sup-Santé Generaliste & Specialiste" : "Cadre-Sup-Santé",
    }
    # col_secteur_main et col_grade_main initialisés avant le bloc (réutilisés en 7d)

    if col_secteur_main and col_secteur_main in base_gv.columns:
        base_gv = base_gv.with_columns(
            pl.col(col_secteur_main)
            .map_elements(
                lambda v: MAPPING_BAREME_ANSTAT.get(v, v) if v is not None else v,
                return_dtype=pl.Utf8
            )
            .alias(col_secteur_main)
        )
        for ancien, nouveau in MAPPING_BAREME_ANSTAT.items():
            n_rem = int((base_gv[col_secteur_main] == nouveau).sum())
            if n_rem > 0:
                logger.info(f"   Mapping barème : '{ancien}' → '{nouveau}' ({n_rem:,} agents)")

    cle_join_cluster = []
    if "secteur_ref" in indic_cluster.columns and col_secteur_main:
        cle_join_cluster.append(("secteur_ref", col_secteur_main))
    if "grade_ref" in indic_cluster.columns and col_grade_main:
        cle_join_cluster.append(("grade_ref", col_grade_main))

    if cle_join_cluster:
        left_on  = [pair[1] for pair in cle_join_cluster]
        right_on = [pair[0] for pair in cle_join_cluster]

        logger.info(f"   Clés de jointure Gold ↔ Référentiel : {left_on} ↔ {right_on}")

        for l, r in zip(left_on, right_on):
            set_gold = set(base_gv[l].drop_nulls().unique().to_list())
            set_ref  = set(indic_cluster[r].drop_nulls().unique().to_list())
            dans_ref_pas_gold = sorted(set_ref - set_gold)
            dans_gold_pas_ref = sorted(set_gold - set_ref)
            logger.info(f"   [{l}] Gold ({len(set_gold)} modalités) : {sorted(set_gold)}")
            logger.info(f"   [{r}] Référentiel ({len(set_ref)} modalités) : {sorted(set_ref)}")
            if dans_ref_pas_gold:
                logger.warning(f"     ⚠️  Dans référentiel mais absent du Gold : {dans_ref_pas_gold}")
            if dans_gold_pas_ref:
                logger.warning(f"     ⚠️  Dans Gold mais absent du référentiel : {dans_gold_pas_ref}")

        if len(left_on) > 1:
            combis_gold = set(
                base_gv.select(left_on).drop_nulls().unique().iter_rows()
            )
            combis_ref = set(
                indic_cluster.select(right_on).drop_nulls().unique().iter_rows()
            )
            dans_ref_pas_gold_c = sorted(combis_ref - combis_gold)
            dans_gold_pas_ref_c = sorted(combis_gold - combis_ref)
            logger.info(f"   Combinaisons ({' × '.join(left_on)}) :")
            logger.info(f"     Gold         : {len(combis_gold):,} combinaisons")
            logger.info(f"     Référentiel  : {len(combis_ref):,} combinaisons")
            if dans_ref_pas_gold_c:
                logger.warning(f"     ⚠️  Dans référentiel mais absent du Gold : {dans_ref_pas_gold_c}")
            if dans_gold_pas_ref_c:
                logger.warning(
                    f"     ⚠️  Dans Gold mais absent du référentiel : {dans_gold_pas_ref_c[:20]}"
                    f"{'...' if len(dans_gold_pas_ref_c) > 20 else ''}"
                )
            if not dans_ref_pas_gold_c and not dans_gold_pas_ref_c:
                logger.info(f"     ✓ Combinaisons identiques des deux côtés")

        indic_join = indic_cluster.with_columns(
            [pl.col(c).cast(pl.Utf8) for c in right_on]
        )
        base_gv = base_gv.with_columns(
            [pl.col(c).cast(pl.Utf8) for c in left_on]
        ).join(
            indic_join.select(right_on + ["min_bareme", "max_bareme"]),
            left_on=left_on, right_on=right_on, how="left",
        )
        base_gv = base_gv.with_columns(
            pl.when(pl.col("min_bareme").is_null() | pl.col("max_bareme").is_null())
            .then(pl.lit("Cluster non déterminé"))
            .when(pl.col("montant_brut_sans_ind_sans_alloc") < pl.col("min_bareme"))
            .then(pl.lit("Inférieur au minimum du barème"))
            .when(pl.col("montant_brut_sans_ind_sans_alloc") > pl.col("max_bareme"))
            .then(pl.lit("Supérieur au maximum du barème"))
            .otherwise(pl.lit("Conforme au barème"))
            .alias("Statut")
        )
        base_avec_statut = base_gv.clone()
    else:
        logger.warning("   ⚠️  Jointure cluster impossible — Statut non calculé")
        base_gv = base_gv.with_columns([
            pl.lit("Cluster non déterminé").alias("Statut"),
            pl.lit(None).cast(pl.Float64).alias("min_bareme"),
            pl.lit(None).cast(pl.Float64).alias("max_bareme"),
        ])
        base_avec_statut = base_gv.clone()

    # --- Vérification para-militaires ---
    if col_secteur_main and col_secteur_main in base_gv.columns:
        n_para = int((base_gv[col_secteur_main] == "para militaire").sum())
        if n_para == 0:
            logger.info(f"   ✓ Vérification para-militaires : aucun agent para-militaire dans la base")
        else:
            logger.warning(f"   ⚠️  ALERTE : {n_para:,} agents avec barème 'para militaire' détectés")

    n_statut  = base_gv["Statut"].value_counts().sort("Statut")
    n_total_s = n_statut["count"].sum()
    logger.info(f"\n   ── Statut M1 · avant repêchage ±5% ──────────────────────────────────")
    logger.info(f"   (salaire sans ind. ni alloc. vs barème anstat_2025 par secteur × grade)")
    for row in n_statut.iter_rows(named=True):
        pct = 100 * row["count"] / n_total_s if n_total_s > 0 else 0.0
        logger.info(f"     {row['Statut']:<40} : {row['count']:>10,}  ({pct:.1f}%)")
    _print_tranches(
        base_gv, "Statut", "montant_brut_sans_ind_sans_alloc",
        "min_bareme", "max_bareme", avant_recuperation=True
    )

    # --- Récupération agents hors-barème à moins de 5% des bornes ---
    cols_ok = {"montant_brut_sans_ind_sans_alloc", "min_bareme", "max_bareme"}.issubset(set(base_gv.columns))
    if cols_ok:
        n_rec_inf = int(
            base_gv.filter(
                (pl.col("Statut") == "Inférieur au minimum du barème") &
                ((pl.col("min_bareme") - pl.col("montant_brut_sans_ind_sans_alloc")) / pl.col("min_bareme") * 100 < 5)
            ).shape[0]
        )
        n_rec_sup = int(
            base_gv.filter(
                (pl.col("Statut") == "Supérieur au maximum du barème") &
                ((pl.col("montant_brut_sans_ind_sans_alloc") - pl.col("max_bareme")) / pl.col("max_bareme") * 100 < 5)
            ).shape[0]
        )
        base_gv = base_gv.with_columns(
            pl.when(
                (pl.col("Statut") == "Inférieur au minimum du barème") &
                ((pl.col("min_bareme") - pl.col("montant_brut_sans_ind_sans_alloc")) / pl.col("min_bareme") * 100 < 5)
            ).then(pl.lit("Conforme au barème"))
            .when(
                (pl.col("Statut") == "Supérieur au maximum du barème") &
                ((pl.col("montant_brut_sans_ind_sans_alloc") - pl.col("max_bareme")) / pl.col("max_bareme") * 100 < 5)
            ).then(pl.lit("Conforme au barème"))
            .otherwise(pl.col("Statut"))
            .alias("Statut")
        )
        base_avec_statut = base_gv.clone()
        logger.info(f"\n   Repêchage des agents légèrement hors-barème (écart < 5% des bornes) :")
        logger.info(f"     En dessous du minimum repêchés : {n_rec_inf:,}")
        logger.info(f"     Au-dessus du maximum repêchés  : {n_rec_sup:,}")

        n_statut2  = base_gv["Statut"].value_counts().sort("Statut")
        n_total_s2 = n_statut2["count"].sum()
        logger.info(f"\n   ── Statut M1 · après repêchage ±5% ──────────────────────────────────")
        for row in n_statut2.iter_rows(named=True):
            pct = 100 * row["count"] / n_total_s2 if n_total_s2 > 0 else 0.0
            logger.info(f"     {row['Statut']:<40} : {row['count']:>10,}  ({pct:.1f}%)")
        _print_tranches(
            base_gv, "Statut", "montant_brut_sans_ind_sans_alloc",
            "min_bareme", "max_bareme", avant_recuperation=False
        )
        logger.info(f"   → Les agents encore hors-barème seront imputés à l'étape 07a")

elif "montant_brut_sans_ind_sans_alloc" not in base_gv.columns:
    logger.warning("   ⚠️  montant_brut_sans_ind_sans_alloc absent du Gold — régénérer le Gold avec programme 06")
    base_gv = base_gv.with_columns([
        pl.lit("Cluster non déterminé").alias("Statut"),
        pl.lit(None).cast(pl.Float64).alias("min_bareme"),
        pl.lit(None).cast(pl.Float64).alias("max_bareme"),
    ])
    base_avec_statut = base_gv.clone()
else:
    base_gv = base_gv.with_columns([
        pl.lit("Cluster non déterminé").alias("Statut"),
        pl.lit(None).cast(pl.Float64).alias("min_bareme"),
        pl.lit(None).cast(pl.Float64).alias("max_bareme"),
    ])
    base_avec_statut = base_gv.clone()

# ============================================================
# 7d. STATUT_TOTAL — MÉTHODO 2
# ============================================================

logger.info("7d. Statut_Total (Méthodo 2)")
logger.info("   Objectif  : vérifier si le salaire brut total de l'agent (tous codes postes)")
logger.info("               est conforme au barème de son grade et secteur.")
logger.info("   Gold      : montant_brut_total  (Feuille 1 — tous codes postes confondus)")
logger.info("   Référence : montant brut  (colonne 'montant brut'  —  source anstat_2025)")

if (df_anstat_ref is not None
        and "montant_brut_total_ref" in df_anstat_ref.columns
        and "montant_brut_total" in base_gv.columns):

    result_cluster_total = calculer_indicateurs_cluster(
        df_anstat_ref, value_col="montant_brut_total_ref", prefix="total"
    )
    if result_cluster_total is not None:
        indic_cluster_total, _, _ = result_cluster_total

        # Jointure des bornes total sur base_gv
        cle_join_total = []
        if "secteur_ref" in indic_cluster_total.columns and col_secteur_main:
            cle_join_total.append(("secteur_ref", col_secteur_main))
        if "grade_ref" in indic_cluster_total.columns and col_grade_main:
            cle_join_total.append(("grade_ref", col_grade_main))

        if cle_join_total:
            left_on_t  = [p[1] for p in cle_join_total]
            right_on_t = [p[0] for p in cle_join_total]
            indic_t_join = indic_cluster_total.with_columns(
                [pl.col(c).cast(pl.Utf8) for c in right_on_t]
            )
            base_avec_statut = base_avec_statut.with_columns(
                pl.col("montant_brut_total").cast(pl.Float64, strict=False)
            ).join(
                indic_t_join.select(right_on_t + ["min_total", "max_total"]),
                left_on=left_on_t, right_on=right_on_t, how="left",
            )

            # Calcul Statut_Total (même logique que Statut)
            base_avec_statut = base_avec_statut.with_columns(
                pl.when(pl.col("min_total").is_null() | pl.col("max_total").is_null())
                .then(pl.lit("Cluster non déterminé"))
                .when(pl.col("montant_brut_total") < pl.col("min_total"))
                .then(pl.lit("Inférieur au minimum du barème"))
                .when(pl.col("montant_brut_total") > pl.col("max_total"))
                .then(pl.lit("Supérieur au maximum du barème"))
                .otherwise(pl.lit("Conforme au barème"))
                .alias("Statut_Total")
            )

            # Distribution Statut_Total AVANT récupération + tranches
            n_st_av  = base_avec_statut["Statut_Total"].value_counts().sort("Statut_Total")
            n_tst_av = n_st_av["count"].sum()
            logger.info(f"\n   ── Statut_Total M2 · avant repêchage ±5% ───────────────────────────")
            logger.info(f"   (salaire brut total vs colonne 'montant brut' anstat_2025 par secteur × grade)")
            for row in n_st_av.iter_rows(named=True):
                pct = 100 * row["count"] / n_tst_av if n_tst_av > 0 else 0.0
                logger.info(f"     {row['Statut_Total']:<40} : {row['count']:>10,}  ({pct:.1f}%)")
            _print_tranches(
                base_avec_statut, "Statut_Total", "montant_brut_total",
                "min_total", "max_total", avant_recuperation=True
            )

            # Récupération ±5% pour Statut_Total
            n_rec_inf_t = int(
                base_avec_statut.filter(
                    (pl.col("Statut_Total") == "Inférieur au minimum du barème") &
                    ((pl.col("min_total") - pl.col("montant_brut_total")) / pl.col("min_total") * 100 < 5)
                ).shape[0]
            )
            n_rec_sup_t = int(
                base_avec_statut.filter(
                    (pl.col("Statut_Total") == "Supérieur au maximum du barème") &
                    ((pl.col("montant_brut_total") - pl.col("max_total")) / pl.col("max_total") * 100 < 5)
                ).shape[0]
            )
            base_avec_statut = base_avec_statut.with_columns(
                pl.when(
                    (pl.col("Statut_Total") == "Inférieur au minimum du barème") &
                    ((pl.col("min_total") - pl.col("montant_brut_total"))
                     / pl.col("min_total") * 100 < 5)
                ).then(pl.lit("Conforme au barème"))
                .when(
                    (pl.col("Statut_Total") == "Supérieur au maximum du barème") &
                    ((pl.col("montant_brut_total") - pl.col("max_total"))
                     / pl.col("max_total") * 100 < 5)
                ).then(pl.lit("Conforme au barème"))
                .otherwise(pl.col("Statut_Total"))
                .alias("Statut_Total")
            )
            logger.info(f"\n   Repêchage des agents légèrement hors-barème M2 (écart < 5% des bornes) :")
            logger.info(f"     En dessous du minimum repêchés : {n_rec_inf_t:,}")
            logger.info(f"     Au-dessus du maximum repêchés  : {n_rec_sup_t:,}")

            n_st  = base_avec_statut["Statut_Total"].value_counts().sort("Statut_Total")
            n_tst = n_st["count"].sum()
            logger.info(f"\n   ── Statut_Total M2 · après repêchage ±5% ───────────────────────────")
            for row in n_st.iter_rows(named=True):
                pct = 100 * row["count"] / n_tst if n_tst > 0 else 0.0
                logger.info(f"     {row['Statut_Total']:<40} : {row['count']:>10,}  ({pct:.1f}%)")
            _print_tranches(
                base_avec_statut, "Statut_Total", "montant_brut_total",
                "min_total", "max_total", avant_recuperation=False
            )
        else:
            logger.warning("   ⚠️  Jointure cluster_total impossible — Statut_Total non calculé")
            base_avec_statut = base_avec_statut.with_columns([
                pl.lit("Cluster non déterminé").alias("Statut_Total"),
                pl.lit(None).cast(pl.Float64).alias("min_total"),
                pl.lit(None).cast(pl.Float64).alias("max_total"),
            ])
    else:
        logger.warning("   ⚠️  Indicateurs cluster_total non calculés — Statut_Total non calculé")
        base_avec_statut = base_avec_statut.with_columns([
            pl.lit("Cluster non déterminé").alias("Statut_Total"),
            pl.lit(None).cast(pl.Float64).alias("min_total"),
            pl.lit(None).cast(pl.Float64).alias("max_total"),
        ])
else:
    raison = []
    if df_anstat_ref is None:
        raison.append("anstat_2025 absent")
    elif "montant_brut_total_ref" not in (df_anstat_ref.columns if df_anstat_ref is not None else []):
        raison.append("colonne montant_brut_total_ref absente d'anstat_2025")
    if "montant_brut_total" not in base_gv.columns:
        raison.append("colonne montant_brut_total absente du Gold panel")
    logger.warning(f"   ⚠️  Statut_Total non calculé : {' / '.join(raison)}")
    base_avec_statut = base_avec_statut.with_columns([
        pl.lit("Non calculé").alias("Statut_Total"),
        pl.lit(None).cast(pl.Float64).alias("min_total"),
        pl.lit(None).cast(pl.Float64).alias("max_total"),
    ])

# ============================================================
# 8. SAUVEGARDE base_avec_statut.parquet → Gold
# ============================================================

logger.info("8. Sauvegarde base_avec_statut.parquet...")

if len(base_avec_statut) > 0:
    buf_pq = io.BytesIO()
    base_avec_statut.write_parquet(buf_pq, compression="snappy")
    buf_pq.seek(0)
    s3.put_object(
        Bucket=BUCKET_GOLD,
        Key=KEY_BASE_AVEC_STATUT,
        Body=buf_pq.getvalue(),
    )
    logger.info(f"   ✅ s3://{BUCKET_GOLD}/{KEY_BASE_AVEC_STATUT}")
    logger.info(f"   {len(base_avec_statut):,} lignes × {len(base_avec_statut.columns)} colonnes")

    # Résumé final
    total = len(base_avec_statut)
    if "Statut" in base_avec_statut.columns:
        logger.info(f"\n   ── Résumé final Statut M1 (salaire sans ind. ni alloc.) ─────────────")
        for row in (
            base_avec_statut["Statut"].value_counts().sort("Statut").iter_rows(named=True)
        ):
            pct = 100 * row["count"] / total if total > 0 else 0.0
            logger.info(f"     {row['Statut']:<40} : {row['count']:>10,}  ({pct:.1f}%)")

    if "Statut_Total" in base_avec_statut.columns:
        logger.info(f"\n   ── Résumé final Statut_Total M2 (salaire brut total) ────────────────")
        for row in (
            base_avec_statut["Statut_Total"].value_counts().sort("Statut_Total").iter_rows(named=True)
        ):
            pct = 100 * row["count"] / total if total > 0 else 0.0
            logger.info(f"     {row['Statut_Total']:<40} : {row['count']:>10,}  ({pct:.1f}%)")
else:
    logger.warning("   ⚠️  base_avec_statut vide — fichier non sauvegardé")

logger.info("=" * 70)
logger.info("✓ CALCUL STATUT BARÈME TERMINÉ")
logger.info("=" * 70)
logger.info("Prochaine étape : exécuter 07a_imputer_hors_bareme.py")
