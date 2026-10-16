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
# Deux populations distinctes :
#   M1 — Statut conforme (barème)        → grade / CITP / sexe
#        Variable : montant_brut_sans_ind_sans_alloc (codes 124+228+271+425)
#        Hors MAGISTRAT, GREFFE, CORPS PREF, CORPS DIPL et police (gardés en M2)
#   M2 — Statut_Total conforme (barème)  → secteur / barème
#        Variable : montant_brut_total (Feuille 1, tous codes postes)
#        Statut_Total est jugé en 07 sur montant_brut_controle_m2
#        (montant_brut_total − codes 461 à 464) ; les indicateurs restent sur le total complet
#   M3 — Même méthode que M2, hors MAGISTRAT, GREFFE, CORPS PREF, CORPS DIPL et police
#        Feuilles REVENU_M3_* et EFFECTIFS_M3_PAR_ANNEE
#
# Tables produites (Excel multi-feuilles) :
#   M1 — REVENU_Grade_Detail      : Effectif, moy, méd, min, max, P25, P75 par grade
#   M1 — REVENU_Grade_Sexe        : Idem, par grade × sexe
#   M1 — REVENU_CITP_Detail       : Idem, par CITP
#   M1 — Sal_Bareme_Moyen         : Salaire moyen par année × mois × grade (pivot)
#   M1 — Sal_Bareme_Median        : Salaire médian par année × mois × grade (pivot)
#   M1 — MULTI_Par_Grade_NbPostes : Effectifs par grade × nombre de postes (pivot)
#   M1 — MULTI_Distribution       : Distribution globale mono/multi-postes
#   M1 — REVENU_Bareme_M1         : Idem, par barème (population M1)
#   M2 — REVENU_Total_Bareme      : Idem, par barème/secteur
#   M2 — REVENU_Total_Bar_Grade   : Idem, par barème × grade
#   M2 — REVENU_Total_Bar_Sexe    : Idem, par barème × sexe
#   M2 — REVENU_Total_Grade       : Idem, par grade (population M2)
#   M2 — MODELE_ANSTAT_2024       : Modèle statistique ANSTAT — année 2024
#   M2 — MODELE_ANSTAT_2025       : Modèle statistique ANSTAT — année 2025
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
import tempfile
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
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "")

BUCKET_GOLD    = os.getenv("BUCKET_GOLD",    "gold")
BUCKET_STAGING = os.getenv("BUCKET_STAGING", "staging")
PREFIX_PANEL   = "panel_admin"
PREFIX_EXPORTS = "panel_admin/exports_gold"

KEY_BASE_CORRIGEE    = f"{PREFIX_PANEL}/base_avec_statut_corrigee.parquet"
KEY_BASE_AVEC_STATUT = f"{PREFIX_PANEL}/base_avec_statut.parquet"
KEY_CORRESPONDANCE   = "panel_admin/references/CORRESPONDANCE_GRILLE_EMPLOI.xlsx"

GRADES_PRIMAIRE = {"B3", "C3", "D1", "D2"}

CODE_ORGANISME_EXCLU = "59"   # exclu de tous les calculs (cohérent avec 07 et 11)

# Grilles retirées de la population M1 seulement (conservées en M2).
# La police est déjà retirée de tout en étape 07.
BAREMES_EXCLUS_M1 = ("MAGISTRAT", "GREFFE", "CORPS PREF", "CORPS DIPL", "POLICE")

# M3 = méthode M2 (Statut_Total, montant_brut_total) sans ces corps spécifiques
BAREMES_EXCLUS_M3 = ("MAGISTRAT", "GREFFE", "CORPS PREF", "CORPS DIPL", "POLICE")

_mode_label  = "salaire_brut" if MODE_SALAIRE_BRUT else "revenu_salarial"
_zeros_label = "avec_zeros"  if INCLURE_ZEROS     else "sans_zeros"
# OUTPUT_KEY sera défini dynamiquement après chargement des données (années réelles)

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
    ("Para-militaire",                                 "PARAMILITAIRE"),
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

def normaliser_pour_matching(texte) -> str | None:
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


def corriger_bareme_emploi(emploi_norm, bareme_actuel):
    if bareme_actuel != "Barême Général":
        return bareme_actuel
    if emploi_norm is None:
        return bareme_actuel
    if "INFORMATICIEN" in emploi_norm or "INFORMATIQUE" in emploi_norm:
        return "Informaticien"
    if ("SECRETAIRE" in emploi_norm or "SECR " in emploi_norm
            or emploi_norm.startswith("SECR")):
        return "SECRETAIRE DIRECTION"
    return bareme_actuel


def charger_table_bareme() -> "pl.DataFrame | None":
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
                return_dtype=pl.Utf8,
            )
            .alias("bareme")
        )
        table = (
            df.filter(pl.col("emploi_ref_norm").is_not_null())
            .select(["emploi_ref_norm", "bareme", "grade_ref"])
            .unique(subset=["emploi_ref_norm"])
        )
        print(f"  ✓ Table barème chargée : {len(table)} emplois")
        return table
    except Exception as e:
        print(f"  ⚠️  Table barème non disponible ({e}) — feuilles barème ignorées")
        return None
    finally:
        os.unlink(tmp_path)


def fuzzy_join_bareme(df: pl.DataFrame, table_join: pl.DataFrame,
                      col_emploi: str) -> pl.DataFrame:
    df = df.with_columns(
        pl.col(col_emploi)
        .map_elements(normaliser_pour_matching, return_dtype=pl.Utf8)
        .alias("_emploi_norm")
    )
    df = df.join(table_join, left_on="_emploi_norm", right_on="emploi_ref_norm",
                 how="left").drop("_emploi_norm")
    nb_match = int(df["bareme"].is_not_null().sum())
    taux = 100 * nb_match / len(df)
    print(f"  ✓ Fuzzy join barème : {nb_match:,} / {len(df):,} ({taux:.1f}%)")
    if taux < 20:
        print("  ⚠️  Matching faible — vérifiez CORRESPONDANCE_GRILLE_EMPLOI.xlsx")
    return df


def ajouter_bareme_detail(df: pl.DataFrame, col_grade: str) -> pl.DataFrame:
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


def grade_order_expr(col_name: str = "GRADE") -> pl.Expr:
    """Expression Polars native équivalente à grade_order_value — sans map_elements."""
    _c = pl.col(col_name)
    return (
        pl.when(_c.str.contains(r"^[ABCD]\d$"))
        .then(
            pl.when(_c.str.starts_with("A")).then(pl.lit(80))
            .when(_c.str.starts_with("B")).then(pl.lit(70))
            .when(_c.str.starts_with("C")).then(pl.lit(60))
            .when(_c.str.starts_with("D")).then(pl.lit(50))
            .otherwise(pl.lit(0))
            + _c.str.slice(1, 1).cast(pl.Int32, strict=False).fill_null(0)
        )
        .otherwise(pl.lit(0))
        .cast(pl.Int32)
        .alias("GRADE_ORDER")
    )


def compute_indicators(df: pl.DataFrame, group_cols: list[str],
                       value_col: str = "montant_brut_sans_ind_sans_alloc",
                       id_col: str = "matricule") -> pl.DataFrame:
    """Calcule les indicateurs statistiques complets par groupe.

    L'effectif est compté en matricules distincts (n_unique sur 'matricule'
    quand la colonne est disponible, sinon n_unique sur id_col).
    """
    if "matricule" in df.columns:
        eff_expr = pl.col("matricule").n_unique().alias("Effectif")
    elif id_col in df.columns:
        eff_expr = pl.col(id_col).n_unique().alias("Effectif")
    else:
        eff_expr = pl.len().alias("Effectif")
    return (
        df.group_by(group_cols)
        .agg([
            eff_expr,
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


TABLES_BRUT_TOTAL = ["bareme", "bareme_grade", "bareme_sexe", "grade",
                     "grade_sexe", "sexe", "citp", "sexe_trimestre"]


def indicateurs_brut_total(base_ag: pl.DataFrame, id_col: str, label: str) -> dict:
    """Tableaux M2 / M3 sur montant_brut_total (même méthode, population fournie)."""
    res = {k: pl.DataFrame() for k in TABLES_BRUT_TOTAL}
    v = "montant_brut_total"
    if len(base_ag) == 0 or v not in base_ag.columns:
        print(f"   {label} — population vide : tableaux non produits")
        return res
    base_ag = base_ag.filter(pl.col(v).is_not_null())
    cols = base_ag.columns
    per = ["ANNEE", "MOIS_NUM", "MOIS"]

    if BAREME_DISPONIBLE and "bareme" in cols:
        b = base_ag.filter(pl.col("bareme").is_not_null())
        if len(b) > 0:
            res["bareme"]       = compute_indicators(b, per + ["bareme"], v, id_col)
            res["bareme_grade"] = compute_indicators(b, per + ["bareme", "GRADE_PRINCIPAL"], v, id_col)
            res["bareme_sexe"]  = compute_indicators(b, per + ["bareme", "SEXE_STD"], v, id_col)

    if "GRADE_PRINCIPAL" in cols:
        g = base_ag.filter(pl.col("GRADE_PRINCIPAL").is_not_null())
        if len(g) > 0:
            res["grade"] = compute_indicators(g, per + ["GRADE_PRINCIPAL"], v, id_col)
            if "SEXE_STD" in cols:
                gs = g.filter(pl.col("SEXE_STD").is_not_null())
                if len(gs) > 0:
                    res["grade_sexe"] = compute_indicators(
                        gs, per + ["GRADE_PRINCIPAL", "SEXE_STD"], v, id_col)

    if "SEXE_STD" in cols:
        s = base_ag.filter(pl.col("SEXE_STD").is_not_null())
        if len(s) > 0:
            res["sexe"] = compute_indicators(s, per + ["SEXE_STD"], v, id_col)
            t = s.with_columns(
                pl.when(pl.col("MOIS_NUM") <= 3).then(pl.lit("T1"))
                .when(pl.col("MOIS_NUM") <= 6).then(pl.lit("T2"))
                .when(pl.col("MOIS_NUM") <= 9).then(pl.lit("T3"))
                .otherwise(pl.lit("T4"))
                .alias("Trimestre")
            )
            par_sexe = compute_indicators(t, ["ANNEE", "Trimestre", "SEXE_STD"], v, id_col
                                          ).rename({"SEXE_STD": "SEXE"})
            ensemble = compute_indicators(t, ["ANNEE", "Trimestre"], v, id_col
                                          ).with_columns(pl.lit("Ensemble").alias("SEXE"))
            ordre = ["ANNEE", "Trimestre", "SEXE"] + [
                c for c in par_sexe.columns if c not in ("ANNEE", "Trimestre", "SEXE")]
            res["sexe_trimestre"] = (pl.concat([ensemble, par_sexe], how="diagonal")
                                     .sort(["ANNEE", "Trimestre", "SEXE"]).select(ordre))

    if "Code_CITP" in cols:
        c = base_ag.filter(pl.col("Code_CITP").is_not_null())
        if len(c) > 0:
            res["citp"] = compute_indicators(c, per + ["Code_CITP", "Metier_CITP"], v, id_col)

    for k in TABLES_BRUT_TOTAL:
        print(f"   {label} — {k:<15} : {len(res[k]):,} lignes")
    return res


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

# Organisme 59 exclu de tous les calculs (déjà retiré en 07 ; sécurité si base ancienne)
_org59 = pl.col("CODE_ORGANISME").cast(pl.Utf8).str.strip_chars() == CODE_ORGANISME_EXCLU
_n_org59 = int(base_avec_statut.select(_org59.fill_null(False).sum()).item())
base_avec_statut = base_avec_statut.filter(~_org59.fill_null(False))
if _n_org59 > 0:
    print(f"   Exclusion organisme {CODE_ORGANISME_EXCLU} : {_n_org59:,} lignes retirées")
print(f"   Agents uniques : {base_avec_statut.select('matricule').n_unique():,}")

# Distribution Statut M1 et M2
total = len(base_avec_statut)
print(f"\n   Distribution Statut — Méthodo 1 (montant_brut_sans_ind_sans_alloc) :")
for row in base_avec_statut["Statut"].value_counts().sort("Statut").iter_rows(named=True):
    pct = 100 * row["count"] / total if total > 0 else 0.0
    print(f"     {row['Statut']:<40} : {row['count']:>10,}  ({pct:.1f}%)")

if "Statut_Total" in base_avec_statut.columns:
    print(f"\n   Distribution Statut_Total — Méthodo 2 (montant_brut_total) :")
    for row in base_avec_statut["Statut_Total"].value_counts().sort("Statut_Total").iter_rows(named=True):
        pct = 100 * row["count"] / total if total > 0 else 0.0
        print(f"     {row['Statut_Total']:<40} : {row['count']:>10,}  ({pct:.1f}%)")
print()

# Flag M2 : actif si Statut_Total ET montant_brut_total sont disponibles
M2_ACTIVE = (
    "Statut_Total" in base_avec_statut.columns
    and "montant_brut_total" in base_avec_statut.columns
)
print(f"   Méthodo 2 (Statut_Total) : {'activée' if M2_ACTIVE else 'non disponible (Statut_Total ou montant_brut_total absent)'}\n")

# ── Fuzzy join barème si absent du parquet ────────────────────────────────
_bareme_absent = (
    "bareme" not in base_avec_statut.columns
    or base_avec_statut["bareme"].is_not_null().sum() == 0
)
if _bareme_absent:
    print("   ℹ️  Colonne 'bareme' absente du parquet — fuzzy join inline...")
    _table_bareme = charger_table_bareme()
    _candidates_emploi = ["emploi", "CODE_EMPLOI", "EMPLOI"]
    _col_emploi = next(
        (c for c in _candidates_emploi if c in base_avec_statut.columns), None
    )
    if _table_bareme is not None and _col_emploi is not None:
        base_avec_statut = fuzzy_join_bareme(base_avec_statut, _table_bareme, _col_emploi)
        _col_grade = next(
            (c for c in ["GRADE", "grade"] if c in base_avec_statut.columns), None
        )
        if _col_grade:
            base_avec_statut = ajouter_bareme_detail(base_avec_statut, _col_grade)
        print("   ✓ Colonne 'bareme' ajoutée via fuzzy join")
    else:
        print("   ⚠️  Fuzzy join impossible (table ou colonne emploi absente) — feuilles barème non produites")
print()

# ============================================================
# 2. FILTRAGE SUR "Conforme au barème"
# ============================================================

print("2. Filtrage sur Conforme au barème...")

n_avant = len(base_avec_statut)
base_gv = base_avec_statut.filter(pl.col("Statut") == "Conforme au barème")

# Corps sous statut particulier : retirés du M1 uniquement (conservés en M2)
if "bareme" in base_gv.columns:
    _bareme_norm = pl.col("bareme").cast(pl.Utf8).str.strip_chars().str.to_uppercase()
    _exclus_m1 = base_gv.filter(_bareme_norm.is_in(BAREMES_EXCLUS_M1).fill_null(False))
    for _row in (_exclus_m1.group_by(_bareme_norm.alias("_b")).agg(pl.len().alias("n"))
                 .sort("_b").iter_rows(named=True)):
        print(f"   M1 — Exclusion barème {_row['_b']} : {_row['n']:,} lignes")
    base_gv = base_gv.filter(~_bareme_norm.is_in(BAREMES_EXCLUS_M1).fill_null(False))

n_apres = len(base_gv)
pct = 100 * n_apres / n_avant if n_avant > 0 else 0.0
print(f"   {n_apres:,} / {n_avant:,} ({pct:.1f}% retenus pour les indicateurs M1)")

# ── Bilan détaillé des conformes retenus ─────────────────────────────
n_gv = len(base_gv)
# 1. Par statut fonctionnaire
col_statut_fonct = next(
    (c for c in ["statut_fonctionnaire", "situation_normalisee"] if c in base_gv.columns), None
)
if col_statut_fonct:
    print(f"   Par statut fonctionnaire ({col_statut_fonct}) :")
    statut_counts = (
        base_gv.group_by(col_statut_fonct)
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
    )
    for row in statut_counts.iter_rows(named=True):
        pct = 100 * row["n"] / n_gv if n_gv > 0 else 0.0
        print(f"     {str(row[col_statut_fonct]):<35} : {row['n']:>10,}  ({pct:.1f}%)")
# 2. Source du grade (GRADE_SOURCE = observé ou imputé par 03b)
if "GRADE_SOURCE" in base_gv.columns:
    print(f"   Source du grade (GRADE_SOURCE) :")
    for source, label in [("OBSERVE", "Grade observé"), ("IMPUTE", "Grade imputé")]:
        n = int((base_gv["GRADE_SOURCE"] == source).sum())
        print(f"     {label:<35} : {n:>10,}  ({100*n/n_gv:.1f}%)")

BAREME_DISPONIBLE = ("bareme" in base_gv.columns
                     and base_gv["bareme"].is_not_null().sum() > 0)

# ── Filtrage Méthodo 2 (Statut_Total) ───────────────────────────────
base_gv_m2 = pl.DataFrame()
base_agrege_m2 = pl.DataFrame()
if M2_ACTIVE:
    n_av_m2 = len(base_avec_statut)
    base_gv_m2 = base_avec_statut.filter(pl.col("Statut_Total") == "Conforme au barème")
    # Cast montant_brut_total en Float64
    base_gv_m2 = base_gv_m2.with_columns(
        pl.col("montant_brut_total").cast(pl.Float64, strict=False)
    )
    pct_m2 = 100 * len(base_gv_m2) / n_av_m2 if n_av_m2 > 0 else 0.0
    print(f"\n   Méthodo 2 — Statut_Total Conforme : {len(base_gv_m2):,} ({pct_m2:.1f}%)")
print()

# ── Contrôle intégrité des conformes IMPUTÉS uniquement ─────────────
# Les agents récupérés par la règle ±5% (OBSERVE) peuvent être
# légèrement hors bornes strictes — c'est intentionnel.
# Seuls les IMPUTÉS doivent être strictement dans [min_bareme, max_bareme].
cols_borne = {"montant_brut_sans_ind_sans_alloc", "min_bareme", "max_bareme",
              "montant_brut_sans_ind_sans_alloc_source"}
if cols_borne.issubset(set(base_gv.columns)):
    n_total_conf = len(base_gv)
    n_observe = int((base_gv["montant_brut_sans_ind_sans_alloc_source"] == "OBSERVE").sum())
    n_impute  = int((base_gv["montant_brut_sans_ind_sans_alloc_source"] == "IMPUTE").sum())
    # 3. Source du salaire (montant_brut_sans_ind_sans_alloc_source)
    #    OBSERVE = valeur lue directement dans le Gold panel
    #    IMPUTE  = valeur imputée par 07a (forward/backward fill, agent hors-barème ≤ 5 mois)
    print(f"   Source du salaire (montant_brut_sans_ind_sans_alloc_source) :")
    print(f"     Salaire observé              : {n_observe:>10,}  ({100*n_observe/n_total_conf:.1f}%)")
    print(f"     Salaire imputé (via 07a)     : {n_impute:>10,}  ({100*n_impute/n_total_conf:.1f}%)")

    # Contrôle strict uniquement sur les imputés
    base_imputes = base_gv.filter(pl.col("montant_brut_sans_ind_sans_alloc_source") == "IMPUTE")

    n_sous_min = int(
        base_imputes.filter(
            pl.col("min_bareme").is_not_null() &
            (pl.col("montant_brut_sans_ind_sans_alloc") < pl.col("min_bareme"))
        ).shape[0]
    )
    n_sur_max = int(
        base_imputes.filter(
            pl.col("max_bareme").is_not_null() &
            (pl.col("montant_brut_sans_ind_sans_alloc") > pl.col("max_bareme"))
        ).shape[0]
    )

    print(f"\n   Contrôle intégrité des conformes IMPUTÉS ({n_impute:,}) :")
    if n_sous_min == 0:
        print(f"     ✅ Aucun imputé avec montant_brut_sans_ind_sans_alloc < min_bareme")
    else:
        print(f"     ❌ Imputés avec montant_brut_sans_ind_sans_alloc < min_bareme : {n_sous_min:,}  ← ANOMALIE")

    if n_sur_max == 0:
        print(f"     ✅ Aucun imputé avec montant_brut_sans_ind_sans_alloc > max_bareme")
    else:
        print(f"     ❌ Imputés avec montant_brut_sans_ind_sans_alloc > max_bareme : {n_sur_max:,}  ← ANOMALIE")

    if n_impute > 0 and n_sous_min == 0 and n_sur_max == 0:
        print(f"     ✅ Tous les imputés sont dans [min_bareme, max_bareme]")

    if n_sous_min > 0 or n_sur_max > 0:
        raise ValueError(
            f"ARRÊT — {n_sous_min + n_sur_max:,} agents imputés ont un "
            f"montant_brut_sans_ind_sans_alloc hors [min_bareme, max_bareme].\n"
            "Vérifiez l'étape 07a_imputer_hors_bareme.py."
        )
elif {"montant_brut_sans_ind_sans_alloc", "min_bareme", "max_bareme"}.issubset(set(base_gv.columns)):
    print("   ℹ️  Contrôle imputés ignoré (colonne montant_brut_sans_ind_sans_alloc_source absente)")
else:
    print("   ℹ️  Contrôle intégrité ignoré (colonnes min/max_bareme absentes)")

# ── Diagnostic : conformes avec montant_brut_sans_ind_sans_alloc anormalement bas ─
# Seuil = minimum théorique absolu du référentiel (D1 min = ~201 000 FCFA)
# On descend à 100 000 pour capturer les cas vraiment aberrants
SEUIL_DIAGNOSTIC = 100_000

cols_diag = {"montant_brut_sans_ind_sans_alloc", "GRADE", "bareme", "min_bareme", "max_bareme"}
if cols_diag.issubset(set(base_gv.columns)):
    suspects = base_gv.filter(
        pl.col("montant_brut_sans_ind_sans_alloc") < SEUIL_DIAGNOSTIC
    )
    n_suspects = len(suspects)

    if n_suspects == 0:
        print(f"\n   ✅ Aucun conforme avec montant_brut_sans_ind_sans_alloc < {SEUIL_DIAGNOSTIC:,} FCFA")
    else:
        print(f"\n   ⚠️  DIAGNOSTIC — Conformes avec montant_brut_sans_ind_sans_alloc < "
              f"{SEUIL_DIAGNOSTIC:,} FCFA : {n_suspects:,} observations")

        # Résumé par GRADE × bareme × min_bareme
        cols_id = [c for c in ["matricule", "mois_annee", "GRADE", "bareme",
                                "montant_brut_sans_ind_sans_alloc", "min_bareme", "max_bareme",
                                "montant_brut_sans_ind_sans_alloc_source"]
                   if c in base_gv.columns]
        apercu = (
            suspects
            .select(cols_id)
            .sort(["montant_brut_sans_ind_sans_alloc"])
        )
        print(f"   Détail (jusqu'à 20 lignes) :")
        print(apercu.head(20))

        # Résumé groupé : GRADE × bareme × min_bareme
        resume = (
            suspects
            .group_by([c for c in ["GRADE", "bareme", "min_bareme"] if c in suspects.columns])
            .agg([
                pl.len().alias("nb_obs"),
                pl.col("montant_brut_sans_ind_sans_alloc").min().alias("montant_min"),
                pl.col("montant_brut_sans_ind_sans_alloc").max().alias("montant_max"),
            ])
            .sort("nb_obs", descending=True)
        )
        print(f"\n   Résumé par GRADE × barème × min_bareme :")
        print(resume)
        print()
        print("   ℹ️  Ces cas peuvent indiquer un conflit de colonnes dans le join")
        print("      (min_bareme résiduel d'un run précédent) ou une anomalie source.")
        print("      Vérifier base_avec_statut.parquet → colonnes min_bareme/max_bareme.")

# Dériver ANNEES_ANALYSE depuis les données
if "ANNEE" not in base_gv.columns:
    if "mois_annee" in base_gv.columns:
        base_gv = base_gv.with_columns(
            pl.col("mois_annee").str.slice(2, 4).cast(pl.Int32, strict=False).alias("ANNEE")
        )
    else:
        raise RuntimeError("Colonne ANNEE absente — régénérer base_avec_statut.parquet")

# ── Filtre période récente (PERIODE_RECENTE = True → 2024-2025) ──────────────
if PERIODE_RECENTE:
    _annees_vals   = base_gv["ANNEE"].drop_nulls().unique().to_list()
    _seuil_annee   = 2024 if (_annees_vals and max(_annees_vals) > 100) else 24
    _annees_cibles = [a for a in _annees_vals if a >= _seuil_annee]
    _n_avant = len(base_gv)
    base_gv = base_gv.filter(pl.col("ANNEE").is_in(_annees_cibles))
    print(f"   Filtre période ≥ {_seuil_annee} : {_n_avant:,} → {len(base_gv):,} lignes (M1)")
    if M2_ACTIVE and len(base_gv_m2) > 0:
        if "ANNEE" not in base_gv_m2.columns and "mois_annee" in base_gv_m2.columns:
            base_gv_m2 = base_gv_m2.with_columns(
                pl.col("mois_annee").str.slice(2, 4).cast(pl.Int32, strict=False).alias("ANNEE")
            )
        _n_avant_m2 = len(base_gv_m2)
        base_gv_m2 = base_gv_m2.filter(pl.col("ANNEE").is_in(_annees_cibles))
        print(f"   Filtre période ≥ {_seuil_annee} : {_n_avant_m2:,} → {len(base_gv_m2):,} lignes (M2)")

ANNEES_ANALYSE = sorted(base_gv["ANNEE"].drop_nulls().unique().to_list())
print(f"   Années retenues    : {ANNEES_ANALYSE}")
print()

# Nom du fichier de sortie — dérivé dynamiquement des années réelles
_annee_min = min(ANNEES_ANALYSE) if ANNEES_ANALYSE else 2024
_annee_max = max(ANNEES_ANALYSE) if ANNEES_ANALYSE else 2024
OUTPUT_KEY = (f"{PREFIX_EXPORTS}/indicateurs_{_mode_label}"
              f"_{_annee_min}_{_annee_max}_{_zeros_label}.xlsx")

# ── Sélection dynamique de la variable M1 ──────────────────────────────
# Priorité : montant_brut_sans_ind_sans_alloc (codes 124+228+271+425)
# Repli    : montant_brut_bareme (ancien format parquet)
if "montant_brut_sans_ind_sans_alloc" in base_gv.columns:
    value_col_m1 = "montant_brut_sans_ind_sans_alloc"
elif "montant_brut_bareme" in base_gv.columns:
    value_col_m1 = "montant_brut_bareme"
    print("   ⚠️  Repli M1 : montant_brut_sans_ind_sans_alloc absent → montant_brut_bareme")
else:
    raise ValueError(
        "Variable M1 introuvable : ni montant_brut_sans_ind_sans_alloc "
        "ni montant_brut_bareme dans le parquet — régénérer base_avec_statut.parquet"
    )
print(f"   Variable M1 retenue : {value_col_m1}\n")

# ============================================================
# 3. AGRÉGATION → REVENU TOTAL
# ============================================================

agg_col   = "cle_unique" if MODE_SALAIRE_BRUT else "matricule"
if agg_col not in base_gv.columns:
    agg_col = "matricule"
print(f"3. Agrégation par {agg_col} → {value_col_m1}...")

# variable M1 est la référence directe — pas d'alias intermédiaire

# MOIS label si absent
if "MOIS" not in base_gv.columns and "MOIS_NUM" in base_gv.columns:
    base_gv = base_gv.with_columns(
        pl.col("MOIS_NUM")
        .map_elements(lambda m: MOIS_FR.get(m, str(m) if m is not None else ""), return_dtype=pl.Utf8)
        .alias("MOIS")
    )

base_gv = base_gv.with_columns(grade_order_expr("GRADE"))

cols_agg = ["ANNEE", "MOIS_NUM", "MOIS", agg_col]

base_agrege = (
    base_gv
    .group_by(cols_agg)
    .agg([
        pl.col(value_col_m1).first().alias(value_col_m1),
        pl.col("montant_brut_total").first().alias("montant_brut_total")
          if "montant_brut_total" in base_gv.columns
          else pl.lit(None).cast(pl.Float64).alias("montant_brut_total"),
        pl.col("NB_POSTES").first().alias("NB_POSTES")
          if "NB_POSTES" in base_gv.columns
          else pl.lit(1).alias("NB_POSTES"),
        pl.col("matricule").first().alias("matricule")
          if "matricule" in base_gv.columns and agg_col != "matricule"
          else pl.lit(None).cast(pl.Utf8).alias("matricule"),
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
    base_agrege = base_agrege.filter(pl.col(value_col_m1) > 0)
base_agrege = base_agrege.filter(pl.col(value_col_m1).is_not_null())

print(f"   M1 : {len(base_agrege):,} observations agrégées (matricule × période)")

# ── Agrégation M2 ────────────────────────────────────────────────────
agg_col_m2 = agg_col
if M2_ACTIVE and len(base_gv_m2) > 0:
    base_gv_m2 = base_gv_m2.with_columns(grade_order_expr("GRADE"))
    agg_col_m2 = agg_col if agg_col in base_gv_m2.columns else "matricule"
    cols_agg_m2 = ["ANNEE", "MOIS_NUM", "MOIS", agg_col_m2]
    base_agrege_m2 = (
        base_gv_m2
        .group_by(cols_agg_m2)
        .agg([
            pl.col("montant_brut_total").first().alias("montant_brut_total"),
            pl.col("NB_POSTES").first().alias("NB_POSTES")
              if "NB_POSTES" in base_gv_m2.columns
              else pl.lit(1).alias("NB_POSTES"),
            pl.col("matricule").first().alias("matricule")
              if "matricule" in base_gv_m2.columns and agg_col_m2 != "matricule"
              else pl.lit(None).cast(pl.Utf8).alias("matricule"),
            pl.col("GRADE").sort_by("GRADE_ORDER", descending=True).first().alias("GRADE_PRINCIPAL"),
            pl.col("SEXE_STD").first().alias("SEXE_STD")
              if "SEXE_STD" in base_gv_m2.columns
              else pl.lit("Non renseigné").alias("SEXE_STD"),
            pl.col("Code_CITP").first().alias("Code_CITP")
              if "Code_CITP" in base_gv_m2.columns
              else pl.lit(None).cast(pl.Utf8).alias("Code_CITP"),
            pl.col("Metier_CITP").first().alias("Metier_CITP")
              if "Metier_CITP" in base_gv_m2.columns
              else pl.lit(None).cast(pl.Utf8).alias("Metier_CITP"),
            pl.col("bareme").first().alias("bareme")
              if "bareme" in base_gv_m2.columns
              else pl.lit(None).cast(pl.Utf8).alias("bareme"),
        ])
    )
    base_gv_m2 = base_gv_m2.drop("GRADE_ORDER")
    base_agrege_m2 = base_agrege_m2.filter(pl.col("montant_brut_total").is_not_null())
    if not INCLURE_ZEROS:
        base_agrege_m2 = base_agrege_m2.filter(pl.col("montant_brut_total") > 0)
    print(f"   M2 : {len(base_agrege_m2):,} observations agrégées (matricule × période)")

# ── Population M3 : M2 sans les corps spécifiques ────────────────────
def _hors_corps_m3(df: pl.DataFrame) -> pl.DataFrame:
    if len(df) == 0 or "bareme" not in df.columns:
        return df
    _b = pl.col("bareme").cast(pl.Utf8).str.strip_chars().str.to_uppercase()
    return df.filter(~_b.is_in(BAREMES_EXCLUS_M3).fill_null(False))

base_gv_m3     = _hors_corps_m3(base_gv_m2)
base_agrege_m3 = _hors_corps_m3(base_agrege_m2)
if M2_ACTIVE and len(base_agrege_m2) > 0:
    print(f"   M3 : {len(base_agrege_m3):,} observations agrégées "
          f"({len(base_agrege_m2) - len(base_agrege_m3):,} retirées : {', '.join(BAREMES_EXCLUS_M3)})")

print()

# ============================================================
# 3b. WINSORISATION SUR MONTANT_BRUT_TOTAL
# ============================================================

value_col_indic = value_col_m1

# Winsorisation appliquée sur base_agrege_m2 (M2) car c'est montant_brut_total
# qui est utilisé pour les indicateurs barème/secteur.
if SEUIL_WINS is not None and M2_ACTIVE and len(base_agrege_m2) > 0 \
        and "montant_brut_total" in base_agrege_m2.columns:
    wins_full     = [c for c in ["ANNEE", "MOIS_NUM", "GRADE_PRINCIPAL", "Code_CITP"]
                     if c in base_agrege_m2.columns]
    wins_fallback = [c for c in ["ANNEE", "MOIS_NUM", "GRADE_PRINCIPAL"]
                     if c in base_agrege_m2.columns]
    print(f"3b. Winsorisation MONTANT_BRUT_TOTAL sur M2 "
          f"(P{int(SEUIL_WINS*100)}/P{int((1-SEUIL_WINS)*100)}) "
          f"— seuil groupe : {MIN_OBS_WINS} obs...")
    print(f"   Population  : conformes Statut_Total (M2) — {len(base_agrege_m2):,} obs")
    print(f"   Clusters    : ANNEE × MOIS × GRADE × CITP  |  repli : ANNEE × MOIS × GRADE")

    base_agrege_m2 = base_agrege_m2.with_columns([
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

    n_obs_citp  = int((base_agrege_m2["_n_citp"] >= MIN_OBS_WINS).sum())
    n_obs_grade = len(base_agrege_m2) - n_obs_citp

    base_agrege_m2 = base_agrege_m2.with_columns(
        pl.when(pl.col("_n_citp") >= MIN_OBS_WINS)
        .then(pl.col("montant_brut_total").clip(pl.col("_pb_citp"), pl.col("_ph_citp")))
        .otherwise(pl.col("montant_brut_total").clip(pl.col("_pb_grade"), pl.col("_ph_grade")))
        .alias("montant_brut_total_W")
    ).drop(["_n_citp", "_pb_citp", "_ph_citp", "_pb_grade", "_ph_grade"])

    n_modif = int((base_agrege_m2["montant_brut_total_W"] != base_agrege_m2["montant_brut_total"]).sum())
    n_total_m2 = len(base_agrege_m2)
    pct_modif = (100 * n_modif / n_total_m2) if n_total_m2 > 0 else 0.0
    print(f"   Groupe ANNEE × MOIS × GRADE × CITP (≥{MIN_OBS_WINS} obs) : {n_obs_citp:,} obs")
    print(f"   Repli  ANNEE × MOIS × GRADE         (<{MIN_OBS_WINS} obs) : {n_obs_grade:,} obs")
    print(f"   Observations écrêtées : {n_modif:,}  ({pct_modif:.2f}%)")
    print(f"   → montant_brut_total_W disponible dans base_agrege_m2 pour usage futur\n")
elif SEUIL_WINS is not None:
    print(f"3b. Winsorisation ignorée "
          f"({'M2 inactif' if not M2_ACTIVE else 'base_agrege_m2 vide ou montant_brut_total absent'})\n")

# ============================================================
# 3c. EFFECTIFS UNIQUES PAR GROUPE SUR LA PÉRIODE COMPLÈTE
# ============================================================
# Pour le sommaire : compter les agents uniques (matricule) sur l'ensemble
# de la période, pas la somme des effectifs mensuels.
# Un agent présent 24 mois compte pour 1, pas pour 24.

print("3c. Calcul effectifs uniques par groupe (période complète)...")

col_id = "matricule" if "matricule" in base_gv.columns else agg_col

eff_unique_grade = (
    base_gv
    .group_by("GRADE_PRINCIPAL" if "GRADE_PRINCIPAL" in base_gv.columns else "GRADE")
    .agg(pl.col(col_id).n_unique().alias("nb_agents_uniques"))
    .rename({"GRADE_PRINCIPAL" if "GRADE_PRINCIPAL" in base_gv.columns else "GRADE": "groupe_valeur"})
    .with_columns(pl.lit("GRADE").alias("groupe_type"))
) if "GRADE" in base_gv.columns else pl.DataFrame()

eff_unique_sexe = (
    base_gv
    .filter(pl.col("SEXE_STD").is_not_null())
    .group_by("SEXE_STD")
    .agg(pl.col(col_id).n_unique().alias("nb_agents_uniques"))
    .rename({"SEXE_STD": "groupe_valeur"})
    .with_columns(pl.lit("SEXE").alias("groupe_type"))
) if "SEXE_STD" in base_gv.columns else pl.DataFrame()

eff_unique_citp = (
    base_gv
    .filter(pl.col("Code_CITP").is_not_null() & (pl.col("Code_CITP") != "Non renseigné"))
    .group_by("Code_CITP")
    .agg(pl.col(col_id).n_unique().alias("nb_agents_uniques"))
    .rename({"Code_CITP": "groupe_valeur"})
    .with_columns(pl.lit("CITP").alias("groupe_type"))
) if "Code_CITP" in base_gv.columns else pl.DataFrame()

eff_unique_bareme = (
    base_gv
    .filter(pl.col("bareme").is_not_null())
    .group_by("bareme")
    .agg(pl.col(col_id).n_unique().alias("nb_agents_uniques"))
    .rename({"bareme": "groupe_valeur"})
    .with_columns(pl.lit("BAREME").alias("groupe_type"))
) if "bareme" in base_gv.columns else pl.DataFrame()

# Table globale unique — toutes les dimensions
frames_eff = [f for f in [eff_unique_grade, eff_unique_sexe, eff_unique_citp, eff_unique_bareme]
              if len(f) > 0]
if frames_eff:
    effectifs_uniques_periode = pl.concat(frames_eff, how="diagonal")
else:
    effectifs_uniques_periode = pl.DataFrame({"groupe_type": [], "groupe_valeur": [],
                                               "nb_agents_uniques": []})

# Total global unique
total_agents_uniques = int(base_gv[col_id].n_unique()) if col_id in base_gv.columns else 0
print(f"   Total agents uniques sur la période : {total_agents_uniques:,}")
print(f"   Lignes table effectifs uniques : {len(effectifs_uniques_periode):,}")

# ── Effectifs M1 par année — pour SOMMAIRE_2024 / SOMMAIRE_2025 ──────────
# Source : base_gv (M1 conformes), groupé par (ANNEE × groupe)
# Chaque agent compte 1 fois par année, indépendamment du nombre de mois.
effectifs_m1_par_annee = pl.DataFrame()
if "ANNEE" in base_gv.columns:
    _frames_m1_an = []
    for _dim, _col_val in [
        ("GRADE", "GRADE_PRINCIPAL" if "GRADE_PRINCIPAL" in base_gv.columns else "GRADE"),
        ("SEXE",  "SEXE_STD"),
        ("CITP",  "Code_CITP"),
    ]:
        if _col_val not in base_gv.columns:
            continue
        _df_dim = (
            base_gv
            .filter(pl.col(_col_val).is_not_null())
            .group_by(["ANNEE", _col_val])
            .agg(pl.col(col_id).n_unique().alias("nb_agents_uniques"))
            .rename({_col_val: "groupe_valeur"})
            .with_columns(pl.lit(_dim).alias("groupe_type"))
        )
        _frames_m1_an.append(_df_dim)
    if _frames_m1_an:
        effectifs_m1_par_annee = pl.concat(_frames_m1_an, how="diagonal").sort(
            ["groupe_type", "groupe_valeur", "ANNEE"]
        )
    print(f"   Effectifs M1 par année : {len(effectifs_m1_par_annee):,} lignes")

# ── Effectifs M2 par année — pour section Barème du SOMMAIRE ─────────────
# Source : base_gv_m2 (M2 conformes Statut_Total), groupé par (ANNEE × barème)
effectifs_m2_par_annee = pl.DataFrame()
if M2_ACTIVE and len(base_gv_m2) > 0 and "bareme" in base_gv_m2.columns:
    _col_id_m2 = "matricule" if "matricule" in base_gv_m2.columns else agg_col
    _bm2 = base_gv_m2
    if "ANNEE" not in _bm2.columns and "mois_annee" in _bm2.columns:
        _bm2 = _bm2.with_columns(
            pl.col("mois_annee").str.slice(2, 4).cast(pl.Int32, strict=False).alias("ANNEE")
        )
    if "ANNEE" in _bm2.columns:
        effectifs_m2_par_annee = (
            _bm2.filter(pl.col("bareme").is_not_null())
            .group_by(["ANNEE", "bareme"])
            .agg(pl.col(_col_id_m2).n_unique().alias("nb_agents_uniques"))
            .rename({"bareme": "groupe_valeur"})
            .with_columns(pl.lit("BAREME").alias("groupe_type"))
            .sort(["groupe_valeur", "ANNEE"])
        )
        print(f"   Effectifs M2 par année : {len(effectifs_m2_par_annee):,} lignes")

# ── Effectifs M3 par année (M2 hors corps spécifiques) ───────────────────
effectifs_m3_par_annee = (
    effectifs_m2_par_annee.filter(
        ~pl.col("groupe_valeur").cast(pl.Utf8).str.strip_chars().str.to_uppercase()
        .is_in(BAREMES_EXCLUS_M3).fill_null(False)
    ) if len(effectifs_m2_par_annee) > 0 else pl.DataFrame()
)
if len(effectifs_m3_par_annee) > 0:
    print(f"   Effectifs M3 par année : {len(effectifs_m3_par_annee):,} lignes")

print()

# ============================================================
# 4. INDICATEURS SUR variable M1
# ============================================================

print(f"4. Calcul indicateurs {value_col_m1}...")

indic_grade = compute_indicators(
    base_agrege, ["ANNEE", "MOIS_NUM", "MOIS", "GRADE_PRINCIPAL"], value_col_indic, agg_col
)
indic_grade_sexe = compute_indicators(
    base_agrege, ["ANNEE", "MOIS_NUM", "MOIS", "SEXE_STD", "GRADE_PRINCIPAL"], value_col_indic, agg_col
)

if "Code_CITP" in base_agrege.columns:
    indic_citp = compute_indicators(
        base_agrege.filter(
            pl.col("Code_CITP").is_not_null() & (pl.col("Code_CITP") != "Non renseigné")
        ),
        ["ANNEE", "MOIS_NUM", "MOIS", "Code_CITP", "Metier_CITP"],
        value_col_indic,
        agg_col,
    )
else:
    indic_citp = pl.DataFrame()

# Ventilations barème : calculées en section 4b depuis les conformes Statut_Total (M2)
# Population M1 (Statut conforme) → grade / CITP / sexe uniquement

total_indic = len(indic_grade) + len(indic_grade_sexe) + len(indic_citp)
print(f"   M1 — Grade            : {len(indic_grade):,} lignes")
print(f"   M1 — Grade × Sexe     : {len(indic_grade_sexe):,} lignes")
print(f"   M1 — CITP             : {len(indic_citp):,} lignes")
print(f"   M1 — Total            : {total_indic:,} lignes")

# ── Indicateurs Méthodo 2 et Méthodo 3 (brut total) ──────────────────────
# Même fonction pour M2 et M3 : seule la population change.
#   M2 : conformes Statut_Total
#   M3 : conformes Statut_Total, hors BAREMES_EXCLUS_M3
print(f"\n4b. Calcul indicateurs brut total — Méthodo 2 et Méthodo 3...")

res_m2 = (indicateurs_brut_total(base_agrege_m2, agg_col_m2, "M2")
          if M2_ACTIVE else indicateurs_brut_total(pl.DataFrame(), agg_col, "M2"))
indic_bareme_m2         = res_m2["bareme"]
indic_bareme_grade_m2   = res_m2["bareme_grade"]
indic_bareme_sexe_m2    = res_m2["bareme_sexe"]
indic_grade_m2          = res_m2["grade"]
indic_grade_sexe_m2     = res_m2["grade_sexe"]
indic_sexe_m2           = res_m2["sexe"]
indic_citp_m2           = res_m2["citp"]
indic_sexe_trimestre_m2 = res_m2["sexe_trimestre"]

res_m3 = indicateurs_brut_total(base_agrege_m3, agg_col_m2, "M3")

# ── M1 par barème / barème × grade / barème × sexe ──────────────────────
indic_bareme_m1       = pl.DataFrame()
indic_bareme_grade_m1 = pl.DataFrame()
indic_bareme_sexe_m1  = pl.DataFrame()
if BAREME_DISPONIBLE and "bareme" in base_agrege.columns:
    base_b_m1 = base_agrege.filter(
        pl.col("bareme").is_not_null() & pl.col(value_col_indic).is_not_null()
    )
    if len(base_b_m1) > 0:
        indic_bareme_m1 = compute_indicators(
            base_b_m1, ["ANNEE", "MOIS_NUM", "MOIS", "bareme"], value_col_indic, agg_col
        )
        print(f"   M1 — Barème           : {len(indic_bareme_m1):,} lignes")
        if "GRADE_PRINCIPAL" in base_b_m1.columns:
            indic_bareme_grade_m1 = compute_indicators(
                base_b_m1,
                ["ANNEE", "MOIS_NUM", "MOIS", "bareme", "GRADE_PRINCIPAL"],
                value_col_indic, agg_col
            )
            print(f"   M1 — Barème × Grade   : {len(indic_bareme_grade_m1):,} lignes")
        if "SEXE_STD" in base_b_m1.columns:
            indic_bareme_sexe_m1 = compute_indicators(
                base_b_m1,
                ["ANNEE", "MOIS_NUM", "MOIS", "bareme", "SEXE_STD"],
                value_col_indic, agg_col
            )
            print(f"   M1 — Barème × Sexe    : {len(indic_bareme_sexe_m1):,} lignes")

print()

# ============================================================
# 4c. INDICATEURS PAR SEXE × TRIMESTRE (M1)
# ============================================================

print(f"4c. Calcul indicateurs {value_col_m1} par Sexe × Trimestre (M1)...")

indic_sexe_trimestre = pl.DataFrame()

if "SEXE_STD" in base_agrege.columns and "MOIS_NUM" in base_agrege.columns:
    # Dériver le trimestre depuis MOIS_NUM
    base_trim = base_agrege.with_columns(
        pl.when(pl.col("MOIS_NUM") <= 3).then(pl.lit("T1"))
        .when(pl.col("MOIS_NUM") <= 6).then(pl.lit("T2"))
        .when(pl.col("MOIS_NUM") <= 9).then(pl.lit("T3"))
        .otherwise(pl.lit("T4"))
        .alias("Trimestre")
    ).filter(pl.col("SEXE_STD").is_not_null())

    # Indicateurs par SEXE × ANNEE × Trimestre
    indic_par_sexe = compute_indicators(
        base_trim,
        ["ANNEE", "Trimestre", "SEXE_STD"],
        value_col_indic,
        agg_col,
    ).rename({"SEXE_STD": "SEXE"})

    # Indicateurs Ensemble (tous sexes confondus)
    indic_ensemble = compute_indicators(
        base_trim,
        ["ANNEE", "Trimestre"],
        value_col_indic,
        agg_col,
    ).with_columns(pl.lit("Ensemble").alias("SEXE"))

    # Empiler Ensemble + par sexe
    indic_sexe_trimestre = (
        pl.concat([indic_ensemble, indic_par_sexe], how="diagonal")
        .sort(["ANNEE", "Trimestre", "SEXE"])
    )
    # Réordonner colonnes : ANNEE, Trimestre, SEXE en premier
    cols_ordre = ["ANNEE", "Trimestre", "SEXE"] + [
        c for c in indic_sexe_trimestre.columns
        if c not in ("ANNEE", "Trimestre", "SEXE")
    ]
    indic_sexe_trimestre = indic_sexe_trimestre.select(cols_ordre)

    print(f"   M1 — Sexe × Trimestre : {len(indic_sexe_trimestre):,} lignes")
    del base_trim
else:
    print("   ⚠️  SEXE_STD ou MOIS_NUM absent — feuille Sexe × Trimestre non produite")

print()

# ============================================================
# 5. INDICATEURS SALAIRE BRUT (pivot grade)
# ============================================================

print(f"5. Calcul indicateurs {value_col_m1} (pivot grade)...")

brut_moyen = (
    base_gv
    .group_by(["ANNEE", "MOIS_NUM", "MOIS", "GRADE"])
    .agg(pl.col(value_col_m1).mean().round(0).alias(f"{value_col_m1}_moyen"))
    .sort(["ANNEE", "MOIS_NUM"])
)
brut_median = (
    base_gv
    .group_by(["ANNEE", "MOIS_NUM", "MOIS", "GRADE"])
    .agg(pl.col(value_col_m1).median().round(0).alias(f"{value_col_m1}_median"))
    .sort(["ANNEE", "MOIS_NUM"])
)

brut_moyen_wide = brut_moyen.pivot(
    values=f"{value_col_m1}_moyen",
    index=["ANNEE", "MOIS_NUM", "MOIS"],
    on="GRADE"
).sort(["ANNEE", "MOIS_NUM"])

brut_median_wide = brut_median.pivot(
    values=f"{value_col_m1}_median",
    index=["ANNEE", "MOIS_NUM", "MOIS"],
    on="GRADE"
).sort(["ANNEE", "MOIS_NUM"])

print(f"   M1 — Moyen  : {len(brut_moyen_wide)} lignes × {len(brut_moyen_wide.columns)} cols")
print(f"   M1 — Médian : {len(brut_median_wide)} lignes × {len(brut_median_wide.columns)} cols")
# Note : pas de pivot M2 par grade — les indicateurs grade sont M1 uniquement
print()

# ============================================================
# 6. MULTI-POSTES
# ============================================================

print("6. Création feuilles multi-postes...")

# ── Détail mensuel (Revenu moyen par mois × grade × NB_POSTES) ───────
multi_detail = (
    base_agrege
    .group_by(["ANNEE", "MOIS_NUM", "MOIS", "GRADE_PRINCIPAL", "NB_POSTES"])
    .agg([
        pl.col(agg_col).n_unique().alias("Effectif"),
        pl.col(value_col_m1).mean().round(0).alias("Revenu_moyen"),
    ])
    .sort(["ANNEE", "MOIS_NUM", "GRADE_PRINCIPAL", "NB_POSTES"])
)

# ── Effectif multi-postes : TOUS les agents depuis le Gold panel ────────────
# Source : panel_YYYY.parquet (step 06) — NB_POSTES calculé avant déduplication,
# SANS filtre barème ni filtre salaire.
# base_avec_statut exclurait les agents avec montant_brut_sans_ind_sans_alloc
# nul ou invalide, ce qui sous-estime les effectifs multi-postes.

print("   Chargement Gold panel pour multi-postes (tous agents, sans filtre barème)...")

_pages_mp = s3.get_paginator("list_objects_v2").paginate(
    Bucket=BUCKET_GOLD, Prefix=PREFIX_PANEL
)
_cles_panel_mp = sorted([
    obj["Key"]
    for page in _pages_mp
    for obj in page.get("Contents", [])
    if re.search(r"panel_20\d{2}\.parquet$", obj["Key"])
])

_cols_mp    = ["matricule", "mois_annee", "GRADE", "GRADE_SOURCE", "NB_POSTES", "emploi",
               "CODE_ORGANISME"]
_frames_mp  = []
for _cle_mp in _cles_panel_mp:
    _buf_mp2 = io.BytesIO()
    s3.download_fileobj(BUCKET_GOLD, _cle_mp, _buf_mp2)
    _buf_mp2.seek(0)
    _df_mp = pl.read_parquet(_buf_mp2)
    _cols_dispo_mp = [c for c in _cols_mp if c in _df_mp.columns]
    _frames_mp.append(_df_mp.select(_cols_dispo_mp))
    del _df_mp

_bms = pl.concat(_frames_mp, how="diagonal")
del _frames_mp
gc.collect()
if "GRADE" not in _bms.columns:
    raise RuntimeError(
        "ARRÊT — Colonne GRADE absente des fichiers panel_YYYY.parquet (Gold).\n"
        "Le Silver a été régénéré par 03_bronze_to_silver.py sans relancer\n"
        "03b_imputer_grades_silver.py. Ordre à respecter :\n"
        "  03 → 03b → 03c → 06 → 07 → 07a → 07b → 08"
    )
print(f"   Gold panel chargé : {len(_bms):,} lignes — {_bms['matricule'].n_unique():,} agents uniques")

# Ajouter ANNEE si absente
if "ANNEE" not in _bms.columns and "mois_annee" in _bms.columns:
    _bms = _bms.with_columns(
        pl.col("mois_annee").str.slice(2, 4).cast(pl.Int32, strict=False).alias("ANNEE")
    )

# Filtre période récente 2024-2025 (cohérent avec le reste de l'analyse)
if PERIODE_RECENTE and "ANNEE" in _bms.columns:
    _seuil_multi  = 2024 if (ANNEES_ANALYSE and max(ANNEES_ANALYSE) > 100) else 24
    _annees_multi = [a for a in ANNEES_ANALYSE if a >= _seuil_multi]
    _bms = _bms.filter(pl.col("ANNEE").is_in(_annees_multi))

# Exclure les NF (non-fonctionnaires) des stats multi-postes
if "GRADE" in _bms.columns:
    _n_nf = int((_bms["GRADE"] == "NF").sum())
    _bms = _bms.filter(pl.col("GRADE") != "NF")
    if _n_nf > 0:
        print(f"   [DIAG multi-postes] Exclusion NF : {_n_nf:,} lignes retirées")

# Exclure l'organisme 59, comme partout ailleurs
if "CODE_ORGANISME" in _bms.columns:
    _org59_mp = pl.col("CODE_ORGANISME").cast(pl.Utf8).str.strip_chars() == CODE_ORGANISME_EXCLU
    _n_org59_mp = int(_bms.select(_org59_mp.fill_null(False).sum()).item())
    _bms = _bms.filter(~_org59_mp.fill_null(False)).drop("CODE_ORGANISME")
    print(f"   [DIAG multi-postes] Exclusion organisme {CODE_ORGANISME_EXCLU} : {_n_org59_mp:,} lignes retirées")

# Exclure la police, comme en étape 07 (les paramilitaires sont conservés)
if "emploi" in _bms.columns:
    _table_bareme_mp = charger_table_bareme()
    if _table_bareme_mp is not None:
        _emplois_police = (
            _bms.select(pl.col("emploi").drop_nulls().unique())
            .with_columns(pl.col("emploi")
                          .map_elements(normaliser_pour_matching, return_dtype=pl.Utf8)
                          .alias("_emploi_norm"))
            .join(_table_bareme_mp.select(["emploi_ref_norm", "bareme"]),
                  left_on="_emploi_norm", right_on="emploi_ref_norm", how="inner")
            .filter(pl.col("bareme") == "police")
            .get_column("emploi").to_list()
        )
        _n_police = int(_bms["emploi"].is_in(_emplois_police).sum())
        _bms = _bms.filter(~pl.col("emploi").is_in(_emplois_police).fill_null(False))
        print(f"   [DIAG multi-postes] Exclusion police : {_n_police:,} lignes retirées")
    else:
        print("   ⚠️  [DIAG multi-postes] Table de correspondance absente — police non retirée")
_bms = _bms.drop("emploi", strict=False)

# Diagnostic APRÈS filtre période — pour voir ce qui reste réellement
if "NB_POSTES" in _bms.columns:
    _nb_postes_vals = _bms["NB_POSTES"].drop_nulls().unique().sort().to_list()
    _n_multi = int((_bms["NB_POSTES"] > 1).sum())
    print(f"   [DIAG multi-postes] NB_POSTES présent — valeurs : {_nb_postes_vals}  |  lignes multi : {_n_multi:,}")
else:
    print("   [DIAG multi-postes] ⚠️  NB_POSTES ABSENT → relancer step 07")

_grades_bms = sorted(_bms["GRADE"].drop_nulls().unique().to_list()) if "GRADE" in _bms.columns else []
_n_d2 = int((_bms["GRADE"] == "D2").sum()) if "GRADE" in _bms.columns else 0
print(f"   [DIAG multi-postes] Grades présents (2024-2025) : {_grades_bms}")
print(f"   [DIAG multi-postes] Lignes D2 : {_n_d2:,}  |  Agents uniques : {_bms['matricule'].n_unique():,}")

# ── Sauvegarde des agents multi-postes AVANT agrégation ──────────────────────
# Conserve toutes les lignes où NB_POSTES > 1, avec toutes les colonnes utiles,
# avant que le group_by(matricule × ANNEE) ne les agrège et perde le détail.
if "NB_POSTES" in _bms.columns:
    _multi_raw = _bms.filter(pl.col("NB_POSTES") > 1)
    if len(_multi_raw) > 0:
        _key_multi_raw = f"{PREFIX_PANEL}/multi_postes_detail.parquet"
        _buf_mp = io.BytesIO()
        _multi_raw.write_parquet(_buf_mp, compression="snappy")
        _buf_mp.seek(0)
        s3.put_object(Bucket=BUCKET_GOLD, Key=_key_multi_raw, Body=_buf_mp.getvalue())
        print(f"   ✓ Multi-postes brut sauvegardé : s3://{BUCKET_GOLD}/{_key_multi_raw}"
              f"  ({len(_multi_raw):,} lignes × {_multi_raw['matricule'].n_unique():,} agents)")
    else:
        print("   ℹ️  Aucun agent multi-postes détecté dans la période — fichier non créé")

# GRADE_ORDER pour identifier le grade principal de chaque agent
_bms = _bms.with_columns(grade_order_expr("GRADE"))

# 1 ligne par (matricule × ANNEE) : grade principal + NB_POSTES
_multi_base = (
    _bms
    .group_by(["matricule", "ANNEE"])
    .agg([
        pl.col("GRADE").sort_by("GRADE_ORDER", descending=True).first().alias("GRADE_PRINCIPAL"),
        pl.col("NB_POSTES").max().alias("NB_POSTES")
          if "NB_POSTES" in _bms.columns
          else pl.lit(1).cast(pl.Int64).alias("NB_POSTES"),
    ])
)

_multi_annuel = (
    _multi_base
    .group_by(["ANNEE", "GRADE_PRINCIPAL", "NB_POSTES"])
    .agg(pl.col("matricule").n_unique().alias("Effectif"))
    .with_columns(
        pl.when(pl.col("NB_POSTES") >= 5)
        .then(pl.lit("5+"))
        .otherwise(pl.col("NB_POSTES").cast(pl.Utf8))
        .alias("NB_POSTES_LABEL")
    )
)

multi_effectif = _multi_annuel.pivot(
    values="Effectif",
    index=["ANNEE", "GRADE_PRINCIPAL"],
    on="NB_POSTES_LABEL",
    aggregate_function="sum",
).sort(["ANNEE", "GRADE_PRINCIPAL"])

for c in ["1", "2", "3", "4", "5+"]:
    if c in multi_effectif.columns:
        multi_effectif = multi_effectif.with_columns(pl.col(c).fill_null(0))
    else:
        multi_effectif = multi_effectif.with_columns(pl.lit(0).cast(pl.Int64).alias(c))

# distrib_postes : effectifs uniquement (pas de revenu moyen)
_total_agents_multi = _multi_base["matricule"].n_unique()
distrib_postes = (
    _multi_base
    .group_by("NB_POSTES")
    .agg(pl.col("matricule").n_unique().alias("Effectif"))
    .with_columns(
        (pl.col("Effectif") * 100 / _total_agents_multi).round(2).alias("Pct")
    )
    .sort("NB_POSTES")
)

print(f"   Multi-postes détail  : {len(multi_effectif)} lignes")
print(f"   Distribution globale : {len(distrib_postes)} lignes\n")

# ============================================================
# 7. PRÉ-CALCUL DONNÉES MODELE_ANSTAT PAR ANNÉE
# ============================================================

anstat_data_par_annee: dict[int, tuple[dict, pl.DataFrame]] = {}

if BAREME_DISPONIBLE and M2_ACTIVE and len(base_gv_m2) > 0:
    print("7. Pré-calcul données MODELE_ANSTAT par année (M2 — montant_brut_total)...")
    # Source : conformes Statut_Total (M2), variable montant_brut_total
    # Cohérence : les feuilles MODELE_ANSTAT sont des indicateurs secteur/barème
    #             → même population et variable que les feuilles REVENU_Total_Bareme

    # S'assurer que ANNEE est disponible dans base_gv_m2
    if "ANNEE" not in base_gv_m2.columns and "mois_annee" in base_gv_m2.columns:
        base_gv_m2 = base_gv_m2.with_columns(
            pl.col("mois_annee").str.slice(2, 4).cast(pl.Int32, strict=False).alias("ANNEE")
        )

    for annee in ANNEES_ANALYSE:
        if "ANNEE" in base_gv_m2.columns and annee not in base_gv_m2["ANNEE"].drop_nulls().unique().to_list():
            print(f"   ⚠️  Année {annee} absente de la période — ignorée")
            continue

        base_gv_b_annee = base_gv_m2.filter(
            (pl.col("ANNEE") == annee)
            & pl.col("bareme_detail").is_not_null()
            & pl.col("montant_brut_total").is_not_null()
            & (pl.col("montant_brut_total") > 0)
        ) if "bareme_detail" in base_gv_m2.columns else pl.DataFrame()

        if len(base_gv_b_annee) == 0:
            print(f"   ⚠️  Aucune donnée barème M2 pour {annee} — feuille ignorée")
            continue

        global_brut_annee = base_gv_b_annee.select([
            pl.col("montant_brut_total").min().round(0).alias("salaire_brut_min"),
            pl.col("montant_brut_total").mean().round(0).alias("salaire_brut_moyen"),
            pl.col("montant_brut_total").max().round(0).alias("salaire_brut_max"),
            pl.col("montant_brut_total").median().round(0).alias("salaire_brut_mediane"),
        ]).row(0, named=True)

        stats_annee = (
            base_gv_b_annee.group_by("bareme_detail")
            .agg([
                pl.col("montant_brut_total").min().round(0).alias("salaire_brut_min"),
                pl.col("montant_brut_total").mean().round(0).alias("salaire_brut_moyen"),
                pl.col("montant_brut_total").max().round(0).alias("salaire_brut_max"),
                pl.col("montant_brut_total").median().round(0).alias("salaire_brut_mediane"),
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
if len(indic_sexe_trimestre) > 0:
    ecrire_feuille(wb, "REVENU_Sexe_Trimestre", indic_sexe_trimestre)
ecrire_feuille(wb, "Sal_Bareme_Moyen",   brut_moyen_wide)   # montant_brut_sans_ind_sans_alloc moyen (pivot grade)
ecrire_feuille(wb, "Sal_Bareme_Median",  brut_median_wide)  # montant_brut_sans_ind_sans_alloc médian (pivot grade)
ecrire_feuille(wb, "MULTI_Par_Grade_NbPostes",  multi_effectif)
ecrire_feuille(wb, "MULTI_Distribution",        distrib_postes)

# --- Feuilles barème M2 (conformes Statut_Total, montant_brut_total) ---
if BAREME_DISPONIBLE and M2_ACTIVE:
    if len(indic_bareme_m2) > 0:
        ecrire_feuille(wb, "REVENU_Total_Bareme",    indic_bareme_m2)
    if len(indic_bareme_grade_m2) > 0:
        ecrire_feuille(wb, "REVENU_Total_Bar_Grade", indic_bareme_grade_m2)
    if len(indic_bareme_sexe_m2) > 0:
        ecrire_feuille(wb, "REVENU_Total_Bar_Sexe",  indic_bareme_sexe_m2)

if M2_ACTIVE and len(indic_grade_m2) > 0:
    ecrire_feuille(wb, "REVENU_Total_Grade",         indic_grade_m2)

if len(indic_bareme_m1) > 0:
    ecrire_feuille(wb, "REVENU_Bareme_M1",           indic_bareme_m1)
if len(indic_bareme_grade_m1) > 0:
    ecrire_feuille(wb, "REVENU_Bareme_Grade_M1",     indic_bareme_grade_m1)
if len(indic_bareme_sexe_m1) > 0:
    ecrire_feuille(wb, "REVENU_Bareme_Sexe_M1",      indic_bareme_sexe_m1)

if M2_ACTIVE and len(indic_grade_sexe_m2) > 0:
    ecrire_feuille(wb, "REVENU_Total_Grade_Sexe",    indic_grade_sexe_m2)
if M2_ACTIVE and len(indic_sexe_m2) > 0:
    ecrire_feuille(wb, "REVENU_Total_Sexe",          indic_sexe_m2)
if M2_ACTIVE and len(indic_citp_m2) > 0:
    ecrire_feuille(wb, "REVENU_CITP_Total",          indic_citp_m2)
if M2_ACTIVE and len(indic_sexe_trimestre_m2) > 0:
    ecrire_feuille(wb, "REVENU_Sexe_Trimestre_M2",   indic_sexe_trimestre_m2)

# --- Feuilles M3 (méthode M2 hors MAGISTRAT, GREFFE, CORPS PREF, CORPS DIPL, police) ---
FEUILLES_M3 = {
    "grade":          "REVENU_M3_Grade",
    "grade_sexe":     "REVENU_M3_Grade_Sexe",
    "sexe":           "REVENU_M3_Sexe",
    "citp":           "REVENU_M3_CITP",
    "sexe_trimestre": "REVENU_M3_Sexe_Trimestre",
    "bareme":         "REVENU_M3_Bareme",
    "bareme_grade":   "REVENU_M3_Bar_Grade",
    "bareme_sexe":    "REVENU_M3_Bar_Sexe",
}
for _cle, _nom in FEUILLES_M3.items():
    if len(res_m3[_cle]) > 0:
        ecrire_feuille(wb, _nom, res_m3[_cle])

if BAREME_DISPONIBLE and M2_ACTIVE:
    for annee in ANNEES_ANALYSE:
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

# Feuille EFFECTIFS_UNIQUES_PERIODE
# Table de référence pour le sommaire : nb agents uniques par groupe sur la période
if len(effectifs_uniques_periode) > 0:
    eff_export = effectifs_uniques_periode.select(
        ["groupe_type", "groupe_valeur", "nb_agents_uniques"]
    ).sort(["groupe_type", "groupe_valeur"])
    ecrire_feuille(wb, "EFFECTIFS_UNIQUES_PERIODE", eff_export)
    # Ajouter aussi le total global comme métadonnée
    ws_eff = wb["EFFECTIFS_UNIQUES_PERIODE"]
    ws_eff.cell(row=len(eff_export) + 3, column=1, value="TOTAL_GLOBAL")
    ws_eff.cell(row=len(eff_export) + 3, column=2, value="Tous groupes")
    ws_eff.cell(row=len(eff_export) + 3, column=3, value=total_agents_uniques)

# Effectifs annuels M1 et M2 (pour SOMMAIRE_2024 / SOMMAIRE_2025 dans step 08)
if len(effectifs_m1_par_annee) > 0:
    ecrire_feuille(wb, "EFFECTIFS_M1_PAR_ANNEE", effectifs_m1_par_annee)
if len(effectifs_m2_par_annee) > 0:
    ecrire_feuille(wb, "EFFECTIFS_M2_PAR_ANNEE", effectifs_m2_par_annee)
if len(effectifs_m3_par_annee) > 0:
    ecrire_feuille(wb, "EFFECTIFS_M3_PAR_ANNEE", effectifs_m3_par_annee)

# Note : feuilles barème M2 déjà écrites dans le bloc BAREME_DISPONIBLE ci-dessus

# Feuille RESUME_STATUT (distribution Statut M1 de la base d'entrée)
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

# Feuille RESUME_STATUT_TOTAL (distribution Statut_Total M2)
if "Statut_Total" in base_avec_statut.columns:
    total = len(base_avec_statut)
    resume_statut_total = (
        base_avec_statut
        .group_by("Statut_Total")
        .agg(pl.len().alias("Effectif"))
        .with_columns(
            (pl.col("Effectif") * 100 / total).round(2).alias("Pourcentage (%)")
        )
        .sort("Statut_Total")
    )
    ecrire_feuille(wb, "RESUME_STATUT_TOTAL", resume_statut_total)

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
