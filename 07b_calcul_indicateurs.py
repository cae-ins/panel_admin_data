# ============================================================
# PANEL ADMIN — ÉTAPE 7b : CALCUL DES INDICATEURS SALAIRES
# ============================================================
# Produit les indicateurs statistiques complets sur les salaires
# à partir de base_avec_statut_corrigee.parquet (Gold).
# Seuls les agents "Conformes au barème" (observés + imputés)
# sont inclus dans les calculs.
#
# Prérequis :
#   07_calcul_indicateur.py  → base_avec_statut.parquet
#   07a_imputer_hors_bareme.py → base_avec_statut_corrigee.parquet
#
# Paramètres configurables :
#   MODE_SALAIRE_BRUT : True  = agrégation par cle_unique (salaire brut)
#                       False = agrégation par matricule (revenu salarial)
#   PERIODE_RECENTE   : True  = années récentes uniquement (2024-2025)
#                       False = période complète (2015 → dernière année)
#   INCLURE_ZEROS     : True  = inclure salaires = 0 ou NA
#                       False = salaire strictement > 0
#
# Tables produites (Excel multi-feuilles) :
#   REVENU_Grade_Detail      : Effectif, moy, méd, min, max, P25, P75 par grade
#   REVENU_Grade_Sexe        : Idem, par grade × sexe
#   REVENU_CITP_Detail       : Idem, par CITP
#   montant_brut_bareme_Moyen  : Salaire brut moyen par année × mois × grade (pivot)
#   montant_brut_bareme_Median : Salaire brut médian par année × mois × grade (pivot)
#   MULTI_Par_Grade_NbPostes : Effectifs par grade × nombre de postes (pivot)
#   MULTI_Distribution       : Distribution globale mono/multi-postes
#   MODELE_ANSTAT_2024       : Modèle statistique ANSTAT — année 2024
#   MODELE_ANSTAT_2025       : Modèle statistique ANSTAT — année 2025
#
# Source  : s3://gold/panel_admin/base_avec_statut_corrigee.parquet
# Sortie  : s3://staging/panel_admin/exports_gold/indicateurs_*.xlsx
#
# Dépendances :
#   pip install polars boto3 python-dotenv openpyxl
# ============================================================

import gc
import io
import os
import re
import unicodedata
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import polars as pl
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment

load_dotenv(".env")

# ============================================================
# PARAMÈTRES DE CONFIGURATION
# ============================================================

# True = Salaire brut (cle_unique) | False = Revenu salarial (matricule)
MODE_SALAIRE_BRUT = True

# True = 2024-2025 | False = 2015 → dernière année disponible
PERIODE_RECENTE   = True

# True = inclure zéros | False = salaire strictement > 0
INCLURE_ZEROS     = True

# Seuil de winsorisation (ex: 0.01 = écrêtage à P1/P99)
# Mettre à None pour désactiver
SEUIL_WINS        = 0.01

# Effectif minimum par cellule mois × grade × CITP
MIN_OBS_WINS      = 100

# ============================================================

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "datalab-team")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio-datalabteam123")

BUCKET_GOLD    = os.getenv("BUCKET_GOLD",    "gold")
BUCKET_STAGING = os.getenv("BUCKET_STAGING", "staging")
PREFIX_PANEL   = "panel_admin"
PREFIX_EXPORTS = "panel_admin/exports_gold"

KEY_BASE_CORRIGEE = f"{PREFIX_PANEL}/base_avec_statut_corrigee.parquet"
KEY_BASE_AVEC_STATUT = f"{PREFIX_PANEL}/base_avec_statut.parquet"

_mode_label    = "salaire_brut"  if MODE_SALAIRE_BRUT else "revenu_salarial"
_periode_label = "2024_2025"     if PERIODE_RECENTE   else "2015_recent"
_zeros_label   = "avec_zeros"    if INCLURE_ZEROS     else "sans_zeros"
OUTPUT_KEY     = (f"{PREFIX_EXPORTS}/indicateurs_{_mode_label}"
                  f"_{_periode_label}_{_zeros_label}.xlsx")

# Libellés mois français
MOIS_FR = {
    1: "Janvier",  2: "Février",   3: "Mars",      4: "Avril",
    5: "Mai",      6: "Juin",      7: "Juillet",   8: "Août",
    9: "Septembre",10: "Octobre",  11: "Novembre", 12: "Décembre",
}

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

print("=" * 70)
print(f"INDICATEURS SALAIRES — Mode : "
      f"{'Salaire brut (cle_unique)' if MODE_SALAIRE_BRUT else 'Revenu salarial (matricule)'}"
      f" | Période : {'2024-2025' if PERIODE_RECENTE else '2015 → récent'}")
print("=" * 70)
print()

# ============================================================
# FONCTIONS UTILITAIRES
# ============================================================

def grade_order_value(grade: str) -> int:
    """Valeur ordinale d'un grade pour sélectionner le grade principal."""
    if re.match(r"^[ABCD][0-9]$", grade):
        base = {"A": 80, "B": 70, "C": 60, "D": 50}[grade[0]]
        return base + int(grade[1])
    return 0


def compute_indicators(df: pl.DataFrame, group_cols: list[str],
                       value_col: str = "montant_brut_bareme") -> pl.DataFrame:
    """Calcule les indicateurs statistiques complets par groupe."""
    return (
        df.group_by(group_cols)
        .agg([
            pl.len().alias("Effectif"),
            pl.col(value_col).mean().round(0).alias("Revenu_moyen"),
            pl.col(value_col).median().round(0).alias("Revenu_median"),
            pl.col(value_col).min().round(0).alias("Revenu_min"),
            pl.col(value_col).max().round(0).alias("Revenu_max"),
            pl.col(value_col).quantile(0.25, interpolation="nearest")
              .round(0).alias("Revenu_p25"),
            pl.col(value_col).quantile(0.75, interpolation="nearest")
              .round(0).alias("Revenu_p75"),
        ])
        .sort(group_cols)
    )


def construire_feuille_anstat(
    wb: Workbook,
    sheet_name: str,
    annee_label: str,
    global_brut: dict,
    stats_bareme_detail: pl.DataFrame,
) -> None:
    """Crée une feuille MODELE_ANSTAT pour une année donnée."""
    ws = wb.create_sheet(sheet_name)
    ws.sheet_view.showGridLines = False

    fill_titre   = PatternFill("solid", fgColor="1F4E79")
    fill_entete  = PatternFill("solid", fgColor="2E75B6")
    fill_global  = PatternFill("solid", fgColor="FFF2CC")
    fill_secteur = PatternFill("solid", fgColor="D6E4F0")
    fill_alt     = [PatternFill("solid", fgColor="EBF3FB"),
                    PatternFill("solid", fgColor="FFFFFF")]

    font_titre   = Font(bold=True, color="FFFFFF", size=12)
    font_entete  = Font(bold=True, color="FFFFFF", size=10)
    font_global  = Font(bold=True, color="7F6000", size=11)
    font_secteur = Font(bold=True, color="1F4E79", size=10)
    font_sous    = Font(size=10, italic=True)
    font_montant = Font(size=10)

    al_centre = Alignment(horizontal="center", vertical="center")
    al_gauche = Alignment(horizontal="left",   vertical="center")
    al_droite = Alignment(horizontal="right",  vertical="center")
    al_indent = Alignment(horizontal="left",   vertical="center", indent=2)

    ws.merge_cells("A1:D1")
    titre_cell = ws.cell(
        row=1, column=1,
        value=f"MODÈLE DE DONNÉES DE SALAIRE DES FONCTIONNAIRES — {annee_label}"
    )
    titre_cell.font      = font_titre
    titre_cell.fill      = fill_titre
    titre_cell.alignment = al_centre
    ws.row_dimensions[1].height = 28

    ws.cell(row=2, column=1, value="salaires en FCFA").font = Font(size=10)
    for col_i, label in enumerate(
        ["salaire brut minimum", "salaire brut moyen", "salaire brut maximum"],
        start=2
    ):
        c = ws.cell(row=2, column=col_i, value=label)
        c.font      = font_entete
        c.fill      = fill_entete
        c.alignment = al_centre
    ws.row_dimensions[2].height = 22

    def get_val(key: str, col: str):
        if key == "_GLOBAL_":
            return global_brut.get(col)
        rows = stats_bareme_detail.filter(pl.col("bareme_detail") == key)
        if len(rows) == 0:
            return None
        return rows.row(0, named=True).get(col)

    row_cur  = 3
    sous_idx = 0
    for libelle, bareme_key in ORDRE_MODELE_ANSTAT:
        est_global   = bareme_key == "_GLOBAL_"
        est_titre    = bareme_key is None
        est_sous_gpe = not est_global and not est_titre

        if est_global:
            for col_i, val in enumerate([
                "TOUS SECTEURS CONFONDUS",
                get_val("_GLOBAL_", "salaire_brut_min"),
                get_val("_GLOBAL_", "salaire_brut_moyen"),
                get_val("_GLOBAL_", "salaire_brut_max"),
            ], start=1):
                c = ws.cell(row=row_cur, column=col_i, value=val)
                c.font      = font_global
                c.fill      = fill_global
                c.alignment = al_gauche if col_i == 1 else al_centre
                if col_i > 1 and isinstance(val, (int, float)):
                    c.number_format = "#,##0"
            ws.row_dimensions[row_cur].height = 22

        elif est_titre:
            ws.merge_cells(
                start_row=row_cur, start_column=1,
                end_row=row_cur,   end_column=4
            )
            c = ws.cell(row=row_cur, column=1, value=libelle)
            c.font      = font_secteur
            c.fill      = fill_secteur
            c.alignment = al_gauche
            ws.row_dimensions[row_cur].height = 20

        else:
            bg = fill_alt[sous_idx % 2]
            sous_idx += 1
            c1 = ws.cell(row=row_cur, column=1, value=libelle)
            c1.font      = font_sous
            c1.fill      = bg
            c1.alignment = al_indent
            for col_i, stat_key in enumerate(
                ["salaire_brut_min", "salaire_brut_moyen", "salaire_brut_max"],
                start=2
            ):
                val = get_val(bareme_key, stat_key)
                c = ws.cell(row=row_cur, column=col_i, value=val)
                c.font          = font_montant
                c.fill          = bg
                c.alignment     = al_droite
                c.number_format = "#,##0"
            ws.row_dimensions[row_cur].height = 18

        row_cur += 1

    ws.column_dimensions["A"].width = 55
    for col_letter in ["B", "C", "D"]:
        ws.column_dimensions[col_letter].width = 22
    ws.freeze_panes = "A3"
    print(f"   ✓ {sheet_name} ({row_cur - 3} lignes)")


def appliquer_style_entete(ws, n_cols: int) -> None:
    fill  = PatternFill("solid", fgColor="4F81BD")
    font  = Font(color="FFFFFF", bold=True, size=11)
    align = Alignment(horizontal="center", vertical="center")
    for col in range(1, n_cols + 1):
        cell = ws.cell(row=1, column=col)
        cell.fill      = fill
        cell.font      = font
        cell.alignment = align


def ecrire_feuille(wb: Workbook, nom: str, df: pl.DataFrame) -> None:
    if len(df) == 0:
        return
    ws = wb.create_sheet(title=nom)
    for j, col in enumerate(df.columns, start=1):
        ws.cell(row=1, column=j, value=col)
    for i, row in enumerate(df.iter_rows(named=True), start=2):
        for j, col in enumerate(df.columns, start=1):
            ws.cell(row=i, column=j, value=row[col])
    appliquer_style_entete(ws, len(df.columns))
    ws.freeze_panes = "A2"
    print(f"   ✓ {nom} ({len(df):,} lignes)")


# ============================================================
# 1. CHARGEMENT base_avec_statut_corrigee.parquet
# ============================================================

print("1. Chargement base_avec_statut_corrigee.parquet...")

# Essayer la base corrigée d'abord, repli sur la base sans imputation
try:
    buf = io.BytesIO()
    s3.download_fileobj(BUCKET_GOLD, KEY_BASE_CORRIGEE, buf)
    buf.seek(0)
    base_avec_statut = pl.read_parquet(buf)
    print(f"   Source : base_avec_statut_corrigee.parquet (avec imputation)")
except Exception:
    print(f"   ⚠️  base_avec_statut_corrigee.parquet absent — repli sur base_avec_statut.parquet")
    buf = io.BytesIO()
    s3.download_fileobj(BUCKET_GOLD, KEY_BASE_AVEC_STATUT, buf)
    buf.seek(0)
    base_avec_statut = pl.read_parquet(buf)
    print(f"   Source : base_avec_statut.parquet (sans imputation)")

print(f"   {len(base_avec_statut):,} lignes × {len(base_avec_statut.columns)} colonnes")
print(f"   Agents uniques : {base_avec_statut.select('matricule').n_unique():,}")

# Distribution Statut
total = len(base_avec_statut)
print(f"\n   Distribution Statut (entrée) :")
for row in base_avec_statut["Statut"].value_counts().sort("Statut").iter_rows(named=True):
    pct = 100 * row["count"] / total if total > 0 else 0.0
    print(f"     {row['Statut']:<40} : {row['count']:>10,}  ({pct:.1f}%)")
print()

# ============================================================
# 2. FILTRAGE SUR "Conforme au barème"
# ============================================================

print("2. Filtrage sur Conforme au barème...")

n_avant = len(base_avec_statut)
base_gv = base_avec_statut.filter(pl.col("Statut") == "Conforme au barème")
n_apres = len(base_gv)
pct = 100 * n_apres / n_avant if n_avant > 0 else 0.0
print(f"   {n_apres:,} / {n_avant:,} ({pct:.1f}% retenus pour les indicateurs)")

# Stats GRADE_SOURCE dans les conformes
if "GRADE_SOURCE" in base_gv.columns:
    n_gv = len(base_gv)
    for source in ["OBSERVE", "IMPUTE"]:
        n = int((base_gv["GRADE_SOURCE"] == source).sum())
        print(f"   GRADE_{source:<10} : {n:,}  ({100*n/n_gv:.1f}%)")

BAREME_DISPONIBLE = ("bareme" in base_gv.columns
                     and base_gv["bareme"].is_not_null().sum() > 0)

# ── Contrôle intégrité des conformes IMPUTÉS uniquement ─────────────
# Les agents récupérés par la règle ±5% (OBSERVE) peuvent être
# légèrement hors bornes strictes — c'est intentionnel.
# Seuls les IMPUTÉS doivent être strictement dans [min_bareme, max_bareme].
cols_borne = {"montant_brut_bareme", "min_bareme", "max_bareme",
              "montant_brut_bareme_source"}
if cols_borne.issubset(set(base_gv.columns)):
    n_total_conf = len(base_gv)
    n_observe = int((base_gv["montant_brut_bareme_source"] == "OBSERVE").sum())
    n_impute  = int((base_gv["montant_brut_bareme_source"] == "IMPUTE").sum())
    print(f"   Dont OBSERVE : {n_observe:,}  ({100*n_observe/n_total_conf:.1f}%)")
    print(f"   Dont IMPUTE  : {n_impute:,}  ({100*n_impute/n_total_conf:.1f}%)")

    # Contrôle strict uniquement sur les imputés
    base_imputes = base_gv.filter(pl.col("montant_brut_bareme_source") == "IMPUTE")

    n_sous_min = int(
        base_imputes.filter(
            pl.col("min_bareme").is_not_null() &
            (pl.col("montant_brut_bareme") < pl.col("min_bareme"))
        ).shape[0]
    )
    n_sur_max = int(
        base_imputes.filter(
            pl.col("max_bareme").is_not_null() &
            (pl.col("montant_brut_bareme") > pl.col("max_bareme"))
        ).shape[0]
    )

    print(f"\n   Contrôle intégrité des conformes IMPUTÉS ({n_impute:,}) :")
    if n_sous_min == 0:
        print(f"     ✅ Aucun imputé avec montant_brut_bareme < min_bareme")
    else:
        print(f"     ❌ Imputés avec montant_brut_bareme < min_bareme : {n_sous_min:,}  ← ANOMALIE")

    if n_sur_max == 0:
        print(f"     ✅ Aucun imputé avec montant_brut_bareme > max_bareme")
    else:
        print(f"     ❌ Imputés avec montant_brut_bareme > max_bareme : {n_sur_max:,}  ← ANOMALIE")

    if n_impute > 0 and n_sous_min == 0 and n_sur_max == 0:
        print(f"     ✅ Tous les imputés sont dans [min_bareme, max_bareme]")

    if n_sous_min > 0 or n_sur_max > 0:
        raise ValueError(
            f"ARRÊT — {n_sous_min + n_sur_max:,} agents imputés ont un "
            f"montant_brut_bareme hors [min_bareme, max_bareme].\n"
            "Vérifiez l'étape 07a_imputer_hors_bareme.py."
        )
elif {"montant_brut_bareme", "min_bareme", "max_bareme"}.issubset(set(base_gv.columns)):
    print("   ℹ️  Contrôle imputés ignoré (colonne montant_brut_bareme_source absente)")
else:
    print("   ℹ️  Contrôle intégrité ignoré (colonnes min/max_bareme absentes)")

# Dériver ANNEES_ANALYSE depuis les données
if "ANNEE" not in base_gv.columns:
    if "mois_annee" in base_gv.columns:
        base_gv = base_gv.with_columns(
            pl.col("mois_annee").str.slice(2, 4).cast(pl.Int32, strict=False).alias("ANNEE")
        )
    else:
        raise RuntimeError("Colonne ANNEE absente — régénérer base_avec_statut.parquet")

ANNEES_ANALYSE = sorted(base_gv["ANNEE"].drop_nulls().unique().to_list())
print(f"   Années disponibles : {ANNEES_ANALYSE}")
print()

# ============================================================
# 3. AGRÉGATION → REVENU TOTAL
# ============================================================

agg_col   = "cle_unique" if MODE_SALAIRE_BRUT else "matricule"
if agg_col not in base_gv.columns:
    agg_col = "matricule"
print(f"3. Agrégation par {agg_col} → montant_brut_bareme...")

# SALAIRE_BRUT depuis montant_brut_bareme
if "SALAIRE_BRUT" not in base_gv.columns:
    base_gv = base_gv.with_columns(
        pl.col("montant_brut_bareme").cast(pl.Float64, strict=False).alias("SALAIRE_BRUT")
    )

# MOIS label si absent
if "MOIS" not in base_gv.columns and "MOIS_NUM" in base_gv.columns:
    base_gv = base_gv.with_columns(
        pl.col("MOIS_NUM")
        .map_elements(lambda m: MOIS_FR.get(m, str(m) if m is not None else ""), return_dtype=pl.Utf8)
        .alias("MOIS")
    )

base_gv = base_gv.with_columns(
    pl.col("GRADE").map_elements(grade_order_value, return_dtype=pl.Int32)
    .alias("GRADE_ORDER")
)

cols_agg = ["ANNEE", "MOIS_NUM", "MOIS", agg_col]

base_agrege = (
    base_gv
    .group_by(cols_agg)
    .agg([
        pl.col("SALAIRE_BRUT").first().alias("montant_brut_bareme"),
        pl.col("montant_brut_total").first().alias("montant_brut_total")
          if "montant_brut_total" in base_gv.columns
          else pl.lit(None).cast(pl.Float64).alias("montant_brut_total"),
        pl.col("NB_POSTES").first().alias("NB_POSTES")
          if "NB_POSTES" in base_gv.columns
          else pl.lit(1).alias("NB_POSTES"),
        pl.col("GRADE").sort_by("GRADE_ORDER", descending=True).first().alias("GRADE_PRINCIPAL"),
        pl.col("GRADE_SOURCE").sort_by("GRADE_ORDER", descending=True).first().alias("GRADE_SOURCE_PRINCIPAL")
          if "GRADE_SOURCE" in base_gv.columns
          else pl.lit(None).cast(pl.Utf8).alias("GRADE_SOURCE_PRINCIPAL"),
        pl.col("SEXE_STD").first().alias("SEXE_STD")
          if "SEXE_STD" in base_gv.columns
          else pl.lit("Non renseigné").alias("SEXE_STD"),
        pl.col("Code_CITP").first().alias("Code_CITP")
          if "Code_CITP" in base_gv.columns
          else pl.lit(None).cast(pl.Utf8).alias("Code_CITP"),
        pl.col("Metier_CITP").first().alias("Metier_CITP")
          if "Metier_CITP" in base_gv.columns
          else pl.lit(None).cast(pl.Utf8).alias("Metier_CITP"),
        pl.col("bareme").first().alias("bareme")
          if "bareme" in base_gv.columns
          else pl.lit(None).cast(pl.Utf8).alias("bareme"),
        pl.col("bareme_detail").first().alias("bareme_detail")
          if "bareme_detail" in base_gv.columns
          else pl.lit(None).cast(pl.Utf8).alias("bareme_detail"),
    ])
)

base_gv = base_gv.drop("GRADE_ORDER")

if not INCLURE_ZEROS:
    base_agrege = base_agrege.filter(pl.col("montant_brut_bareme") > 0)
base_agrege = base_agrege.filter(pl.col("montant_brut_bareme").is_not_null())

print(f"   {len(base_agrege):,} observations agrégées (matricule × période)\n")

# ============================================================
# 3b. WINSORISATION SUR MONTANT_BRUT_TOTAL
# ============================================================

value_col_indic = "montant_brut_bareme"

if SEUIL_WINS is not None and "montant_brut_total" in base_agrege.columns:
    wins_full     = [c for c in ["ANNEE", "MOIS_NUM", "GRADE_PRINCIPAL", "Code_CITP"]
                     if c in base_agrege.columns]
    wins_fallback = [c for c in ["ANNEE", "MOIS_NUM", "GRADE_PRINCIPAL"]
                     if c in base_agrege.columns]
    print(f"3b. Winsorisation MONTANT_BRUT_TOTAL "
          f"(P{int(SEUIL_WINS*100)}/P{int((1-SEUIL_WINS)*100)}) "
          f"— seuil groupe : {MIN_OBS_WINS} obs...")
    print(f"   Clusters principaux : ANNEE × MOIS × GRADE × CITP")
    print(f"   Repli               : ANNEE × MOIS × GRADE")

    base_agrege = base_agrege.with_columns([
        pl.len().over(wins_full).alias("_n_citp"),
        pl.col("montant_brut_total")
          .quantile(SEUIL_WINS, interpolation="nearest")
          .over(wins_full).alias("_pb_citp"),
        pl.col("montant_brut_total")
          .quantile(1 - SEUIL_WINS, interpolation="nearest")
          .over(wins_full).alias("_ph_citp"),
        pl.col("montant_brut_total")
          .quantile(SEUIL_WINS, interpolation="nearest")
          .over(wins_fallback).alias("_pb_grade"),
        pl.col("montant_brut_total")
          .quantile(1 - SEUIL_WINS, interpolation="nearest")
          .over(wins_fallback).alias("_ph_grade"),
    ])

    n_obs_citp  = int((base_agrege["_n_citp"] >= MIN_OBS_WINS).sum())
    n_obs_grade = len(base_agrege) - n_obs_citp

    base_agrege = base_agrege.with_columns(
        pl.when(pl.col("_n_citp") >= MIN_OBS_WINS)
        .then(pl.col("montant_brut_total").clip(pl.col("_pb_citp"), pl.col("_ph_citp")))
        .otherwise(pl.col("montant_brut_total").clip(pl.col("_pb_grade"), pl.col("_ph_grade")))
        .alias("montant_brut_total_W")
    ).drop(["_n_citp", "_pb_citp", "_ph_citp", "_pb_grade", "_ph_grade"])

    n_modif = int((base_agrege["montant_brut_total_W"] != base_agrege["montant_brut_total"]).sum())
    n_total_agrege = len(base_agrege)
    pct_modif = (100 * n_modif / n_total_agrege) if n_total_agrege > 0 else 0.0
    print(f"   Groupe année × mois × grade × CITP (≥{MIN_OBS_WINS} obs) : {n_obs_citp:,} obs")
    print(f"   Repli  année × mois × grade         (<{MIN_OBS_WINS} obs) : {n_obs_grade:,} obs")
    print(f"   Observations écrêtées : {n_modif:,}  ({pct_modif:.2f}%)")
    print(f"   → montant_brut_total_W : winsorisé, disponible pour usage futur")
    print(f"   → Indicateurs Excel    : montant_brut_bareme (non winsorisé)\n")
elif SEUIL_WINS is not None:
    print(f"3b. ⚠️  montant_brut_total absent — winsorisation ignorée\n")

# ============================================================
# 4. INDICATEURS SUR montant_brut_bareme
# ============================================================

print("4. Calcul indicateurs montant_brut_bareme...")

indic_grade = compute_indicators(
    base_agrege, ["ANNEE", "MOIS_NUM", "MOIS", "GRADE_PRINCIPAL"], value_col_indic
)
indic_grade_sexe = compute_indicators(
    base_agrege, ["ANNEE", "MOIS_NUM", "MOIS", "SEXE_STD", "GRADE_PRINCIPAL"], value_col_indic
)

if "Code_CITP" in base_agrege.columns:
    indic_citp = compute_indicators(
        base_agrege.filter(
            pl.col("Code_CITP").is_not_null() & (pl.col("Code_CITP") != "Non renseigné")
        ),
        ["ANNEE", "MOIS_NUM", "MOIS", "Code_CITP", "Metier_CITP"],
        value_col_indic,
    )
else:
    indic_citp = pl.DataFrame()

# Ventilations barème
indic_bareme       = pl.DataFrame()
indic_bareme_grade = pl.DataFrame()
indic_bareme_sexe  = pl.DataFrame()

if BAREME_DISPONIBLE:
    base_b = base_agrege.filter(pl.col("bareme").is_not_null())
    indic_bareme = compute_indicators(
        base_b, ["ANNEE", "MOIS_NUM", "MOIS", "bareme"], value_col_indic
    )
    indic_bareme_grade = compute_indicators(
        base_b, ["ANNEE", "MOIS_NUM", "MOIS", "bareme", "GRADE_PRINCIPAL"], value_col_indic
    )
    indic_bareme_sexe = compute_indicators(
        base_b, ["ANNEE", "MOIS_NUM", "MOIS", "bareme", "SEXE_STD"], value_col_indic
    )

total_indic = (len(indic_grade) + len(indic_grade_sexe) + len(indic_citp)
               + len(indic_bareme) + len(indic_bareme_grade) + len(indic_bareme_sexe))
print(f"   Grade            : {len(indic_grade):,} lignes")
print(f"   Grade × Sexe     : {len(indic_grade_sexe):,} lignes")
print(f"   CITP             : {len(indic_citp):,} lignes")
if BAREME_DISPONIBLE:
    print(f"   Barème           : {len(indic_bareme):,} lignes")
    print(f"   Barème × Grade   : {len(indic_bareme_grade):,} lignes")
    print(f"   Barème × Sexe    : {len(indic_bareme_sexe):,} lignes")
print(f"   Total            : {total_indic:,} lignes\n")

# ============================================================
# 5. INDICATEURS SALAIRE BRUT (pivot grade)
# ============================================================

print("5. Calcul indicateurs montant_brut_bareme (pivot grade)...")

brut_moyen = (
    base_gv
    .group_by(["ANNEE", "MOIS_NUM", "MOIS", "GRADE"])
    .agg(pl.col("montant_brut_bareme").mean().round(0).alias("montant_brut_bareme_moyen"))
    .sort(["ANNEE", "MOIS_NUM"])
)
brut_median = (
    base_gv
    .group_by(["ANNEE", "MOIS_NUM", "MOIS", "GRADE"])
    .agg(pl.col("montant_brut_bareme").median().round(0).alias("montant_brut_bareme_median"))
    .sort(["ANNEE", "MOIS_NUM"])
)

brut_moyen_wide = brut_moyen.pivot(
    values="montant_brut_bareme_moyen",
    index=["ANNEE", "MOIS_NUM", "MOIS"],
    on="GRADE"
).sort(["ANNEE", "MOIS_NUM"])

brut_median_wide = brut_median.pivot(
    values="montant_brut_bareme_median",
    index=["ANNEE", "MOIS_NUM", "MOIS"],
    on="GRADE"
).sort(["ANNEE", "MOIS_NUM"])

print(f"   Moyen  : {len(brut_moyen_wide)} lignes × {len(brut_moyen_wide.columns)} cols")
print(f"   Médian : {len(brut_median_wide)} lignes × {len(brut_median_wide.columns)} cols\n")

# ============================================================
# 6. MULTI-POSTES
# ============================================================

print("6. Création feuilles multi-postes...")

multi_detail = (
    base_agrege
    .group_by(["ANNEE", "MOIS_NUM", "MOIS", "GRADE_PRINCIPAL", "NB_POSTES"])
    .agg([
        pl.len().alias("Effectif"),
        pl.col("montant_brut_bareme").mean().round(0).alias("Revenu_moyen"),
    ])
    .sort(["ANNEE", "MOIS_NUM", "GRADE_PRINCIPAL", "NB_POSTES"])
)

multi_detail = multi_detail.with_columns(
    pl.when(pl.col("NB_POSTES") >= 5)
    .then(pl.lit("5+"))
    .otherwise(pl.col("NB_POSTES").cast(pl.Utf8))
    .alias("NB_POSTES_LABEL")
)

multi_effectif = multi_detail.pivot(
    values="Effectif",
    index=["ANNEE", "MOIS_NUM", "MOIS", "GRADE_PRINCIPAL"],
    on="NB_POSTES_LABEL",
    aggregate_function="sum",
).sort(["ANNEE", "MOIS_NUM", "GRADE_PRINCIPAL"])

for c in ["1", "2", "3", "4", "5+"]:
    if c in multi_effectif.columns:
        multi_effectif = multi_effectif.with_columns(pl.col(c).fill_null(0))

distrib_postes = (
    base_agrege
    .group_by("NB_POSTES")
    .agg([
        pl.len().alias("Effectif"),
        (pl.len() * 100 / len(base_agrege)).round(2).alias("Pct"),
        pl.col("montant_brut_bareme").mean().round(0).alias("Revenu_moyen"),
    ])
    .sort("NB_POSTES")
)

print(f"   Multi-postes détail  : {len(multi_effectif)} lignes")
print(f"   Distribution globale : {len(distrib_postes)} lignes\n")

# ============================================================
# 7. PRÉ-CALCUL DONNÉES MODELE_ANSTAT PAR ANNÉE
# ============================================================

anstat_data_par_annee: dict[int, tuple[dict, pl.DataFrame]] = {}

if BAREME_DISPONIBLE:
    print("7. Pré-calcul données MODELE_ANSTAT par année...")
    for annee in [2024, 2025]:
        if annee not in ANNEES_ANALYSE:
            print(f"   ⚠️  Année {annee} absente de la période — ignorée")
            continue

        base_gv_b_annee = base_gv.filter(
            (pl.col("ANNEE") == annee)
            & pl.col("bareme_detail").is_not_null()
            & pl.col("SALAIRE_BRUT").is_not_null()
            & (pl.col("SALAIRE_BRUT") > 0)
        )

        if len(base_gv_b_annee) == 0:
            print(f"   ⚠️  Aucune donnée barème pour {annee} — feuille ignorée")
            continue

        global_brut_annee = base_gv_b_annee.select([
            pl.col("SALAIRE_BRUT").min().round(0).alias("salaire_brut_min"),
            pl.col("SALAIRE_BRUT").mean().round(0).alias("salaire_brut_moyen"),
            pl.col("SALAIRE_BRUT").max().round(0).alias("salaire_brut_max"),
            pl.col("SALAIRE_BRUT").median().round(0).alias("salaire_brut_mediane"),
        ]).row(0, named=True)

        stats_annee = (
            base_gv_b_annee.group_by("bareme_detail")
            .agg([
                pl.col("SALAIRE_BRUT").min().round(0).alias("salaire_brut_min"),
                pl.col("SALAIRE_BRUT").mean().round(0).alias("salaire_brut_moyen"),
                pl.col("SALAIRE_BRUT").max().round(0).alias("salaire_brut_max"),
                pl.col("SALAIRE_BRUT").median().round(0).alias("salaire_brut_mediane"),
                pl.len().alias("nb_obs"),
            ])
            .sort("bareme_detail")
        )

        anstat_data_par_annee[annee] = (global_brut_annee, stats_annee)
        print(f"   ✓ {annee} : {len(stats_annee)} barèmes, "
              f"{len(base_gv_b_annee):,} observations")
    print()

# ============================================================
# 8. EXPORT EXCEL
# ============================================================

print("8. Export Excel...")

wb = Workbook()
wb.remove(wb.active)

ecrire_feuille(wb, "REVENU_Grade_Detail",      indic_grade)
ecrire_feuille(wb, "REVENU_Grade_Sexe",         indic_grade_sexe)
if len(indic_citp) > 0:
    ecrire_feuille(wb, "REVENU_CITP_Detail",    indic_citp)
ecrire_feuille(wb, "montant_brut_bareme_Moyen",  brut_moyen_wide)
ecrire_feuille(wb, "montant_brut_bareme_Median", brut_median_wide)
ecrire_feuille(wb, "MULTI_Par_Grade_NbPostes",  multi_effectif)
ecrire_feuille(wb, "MULTI_Distribution",        distrib_postes)

# --- Feuilles barème (si disponibles) ---
if BAREME_DISPONIBLE:
    ecrire_feuille(wb, "REVENU_Bareme_Detail",  indic_bareme)
    ecrire_feuille(wb, "REVENU_Bareme_Grade",   indic_bareme_grade)
    ecrire_feuille(wb, "REVENU_Bareme_Sexe",    indic_bareme_sexe)

    for annee in [2024, 2025]:
        if annee not in anstat_data_par_annee:
            print(f"   ⚠️  MODELE_ANSTAT_{annee} ignorée (données absentes)")
            continue
        global_brut_annee, stats_annee = anstat_data_par_annee[annee]
        construire_feuille_anstat(
            wb=wb,
            sheet_name=f"MODELE_ANSTAT_{annee}",
            annee_label=str(annee),
            global_brut=global_brut_annee,
            stats_bareme_detail=stats_annee,
        )

# Feuille RESUME_STATUT (distribution Statut de la base d'entrée)
if "Statut" in base_avec_statut.columns:
    total = len(base_avec_statut)
    resume_statut = (
        base_avec_statut
        .group_by("Statut")
        .agg(pl.len().alias("Effectif"))
        .with_columns(
            (pl.col("Effectif") * 100 / total).round(2).alias("Pourcentage (%)")
        )
        .sort("Statut")
    )
    ecrire_feuille(wb, "RESUME_STATUT", resume_statut)

# ── Vérification : au moins une feuille doit contenir des données ──
feuilles_visibles  = [ws for ws in wb.worksheets if ws.sheet_state == "visible"]
feuilles_avec_data = [ws for ws in feuilles_visibles if ws.max_row > 1]
if not feuilles_avec_data:
    raise ValueError(
        "ARRÊT — Le classeur Excel ne contient aucune feuille avec données. "
        "Vérifiez que base_avec_statut_corrigee.parquet contient des agents "
        "conformes au barème."
    )

buf_xlsx = io.BytesIO()
wb.save(buf_xlsx)
buf_xlsx.seek(0)
xlsx_bytes = buf_xlsx.getvalue()

s3.put_object(
    Bucket=BUCKET_STAGING,
    Key=OUTPUT_KEY,
    Body=xlsx_bytes,
    ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

taille_kb = len(xlsx_bytes) / 1024
print(f"\n✅ Fichier créé : s3://{BUCKET_STAGING}/{OUTPUT_KEY}")
print(f"   Taille   : {taille_kb:.1f} KB")
print(f"   Feuilles : {len(wb.sheetnames)}")

gc.collect()
print()
print("=" * 70)
print("✓ CALCUL INDICATEURS TERMINÉ")
print("=" * 70)
print()
print("Prochaine étape : exécuter 08_creation_fichier_excel_avec_sommaire.py")
