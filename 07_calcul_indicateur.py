# ============================================================
# PANEL ADMIN — ÉTAPE 7 : CALCUL DES INDICATEURS SALAIRES
# ============================================================
# Produit les indicateurs statistiques complets sur les salaires
# à partir des fichiers Gold panel.
#
# Prérequis : 03b_imputer_grades_silver.py et 06_compiler_panel.py
# doivent avoir été exécutés. Les colonnes GRADE et GRADE_SOURCE
# sont lues directement depuis le Gold panel — aucune imputation
# n'est recalculée ici.
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
#   SALAIRE_BRUT_Moyen       : Salaire brut moyen par année × mois × grade (pivot)
#   SALAIRE_BRUT_Median      : Salaire brut médian par année × mois × grade (pivot)
#   MULTI_Par_Grade_NbPostes : Effectifs par grade × nombre de postes (pivot)
#   MULTI_Distribution       : Distribution globale mono/multi-postes
#   MODELE_ANSTAT_2024       : Modèle statistique ANSTAT — année 2024
#   MODELE_ANSTAT_2025       : Modèle statistique ANSTAT — année 2025
#
# Source  : s3://gold/panel_admin/panel_YYYY.parquet
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
from openpyxl.utils import get_column_letter

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

# Effectif minimum par cellule mois × grade × CITP pour appliquer la
# winsorisation CITP. En dessous, repli sur mois × grade uniquement.
MIN_OBS_WINS      = 100

# ============================================================

# LORSQU'ON TRAVAILLE DEPUIS SA MACHINE LOCAL
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "datalab-team")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio-datalabteam123")

BUCKET_GOLD    = os.getenv("BUCKET_GOLD",    "gold")
BUCKET_STAGING = os.getenv("BUCKET_STAGING", "staging")
PREFIX_PANEL   = "panel_admin"
PREFIX_EXPORTS = "panel_admin/exports_gold"
KEY_CORRESPONDANCE = "panel_admin/references/CORRESPONDANCE_GRILLE_EMPLOI.xlsx"

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

GRADES_VALIDES = (
    [f"A{i}" for i in range(1, 8)] +
    [f"B{i}" for i in range(1, 7)] +
    [f"C{i}" for i in range(1, 6)] +
    [f"D{i}" for i in range(1, 4)]
)

# Composantes du salaire brut (codes F2, suffixe _F2 dans le Gold après normalisation Bronze)
# Recherche insensible à la casse : on résout dynamiquement au moment de l'utilisation
COMPOSANTES_SALAIRE_NOMS = {"124_F2", "228_F2", "271_F2", "421_F2", "425_F2"}

# Codes poste éligibles pour le calcul du revenu (filtrage individus)
CODES_POSTE_ELIGIBLES = {"124", "228", "271", "421", "425"}

# Code organisme à exclure de tous les traitements
CODE_ORGANISME_EXCLUS = "59"

# Fichier de référence barème 2025 (dans staging/panel_admin/references/)
KEY_ANSTAT_2025 = "panel_admin/references/anstat_2025.txt.xlsx"

# Clé de sortie pour les indicateurs barème sans indemnités
KEY_INDIC_BAREME_SANS_INDEM = f"{PREFIX_EXPORTS}/indicateurs_bareme_sans_indemnites.xlsx"

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

def lire_parquet_s3(bucket: str, key: str) -> pl.DataFrame:
    buf = io.BytesIO()
    s3.download_fileobj(bucket, key, buf)
    buf.seek(0)
    return pl.read_parquet(buf)


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
    ("Para-militaire / Douanes / Eaux & Forêts",      "police"),
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
    texte = re.sub(r"[''\u2019]", " ", texte)
    texte = texte.upper()
    texte = unicodedata.normalize("NFKD", texte)
    texte = "".join(c for c in texte if not unicodedata.combining(c))
    texte = re.sub(r"\s+", " ", texte).strip()
    return texte if texte else None


def corriger_bareme_emploi(emploi_norm: str | None, bareme_actuel: str | None) -> str | None:
    """
    Corrige le BAREME d'un emploi uniquement si celui-ci vaut 'Barême Général'.
    Recherche insensible à la casse, aux espaces et aux accents (déjà normalisés).
    - 'informaticien' ou 'informatique' → 'Informaticien'
    - 'secretaire', 'secr.', 'secr '    → 'SECRETAIRE DIRECTION'
      (détection via préfixe SECR — couvre toutes les abréviations)
    Si le barème n'est pas 'Barême Général', on ne touche pas.
    """
    if bareme_actuel != "Barême Général":
        return bareme_actuel
    if emploi_norm is None:
        return bareme_actuel
    # emploi_norm est déjà normalisé (majuscules, sans accents, sans ponctuation)
    if "INFORMATICIEN" in emploi_norm or "INFORMATIQUE" in emploi_norm:
        return "Informaticien"
    if "SECRETAIRE" in emploi_norm or "SECR " in emploi_norm or emploi_norm.startswith("SECR"):
        return "SECRETAIRE DIRECTION"
    return bareme_actuel


def charger_table_bareme() -> pl.DataFrame | None:
    """
    Charge la table CORRESPONDANCE_GRILLE_EMPLOI depuis staging et
    renvoie une table de jointure (emploi_norm → bareme, grade_ref).
    Corrige le BAREME des informaticiens et secrétaires de direction
    lorsque leur BAREME actuel est 'Barême Général'.
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

        # Correction BAREME pour Informaticien et SECRETAIRE DIRECTION
        # Uniquement si le bareme actuel est 'Barême Général'
        # Insensible à la casse, aux espaces et aux accents (emploi_ref_norm déjà normalisé)
        avant_correction = df.filter(
            pl.col("bareme") == "Barême Général"
        ).select(pl.len()).item()

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
            print(f"  ✓ Correction BAREME → Informaticien      : {n_info} emplois")
        if n_sec > 0:
            print(f"  ✓ Correction BAREME → SECRETAIRE DIRECTION : {n_sec} emplois")

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
    """
    Ajoute les colonnes 'bareme' et 'grade_ref' au DataFrame
    via fuzzy join sur le libellé emploi normalisé.
    """
    df = df.with_columns(
        pl.col(col_emploi)
        .map_elements(normaliser_pour_matching, return_dtype=pl.Utf8)
        .alias("_emploi_norm")
    )
    df = df.join(table_join, left_on="_emploi_norm", right_on="emploi_ref_norm",
                 how="left").drop("_emploi_norm")
    nb_match  = int(df["bareme"].is_not_null().sum())
    taux      = 100 * nb_match / len(df)
    print(f"  ✓ Fuzzy join barème : {nb_match:,} / {len(df):,} ({taux:.1f}%)")
    if taux < 20:
        print("  ⚠️  Matching faible — vérifiez CORRESPONDANCE_GRILLE_EMPLOI.xlsx")
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


def compute_indicators(df: pl.DataFrame, group_cols: list[str],
                       value_col: str = "REVENU_TOTAL") -> pl.DataFrame:
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
    """
    Crée une feuille MODELE_ANSTAT pour une année donnée.
    Appelée en boucle pour 2024 et 2025.
    """
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

    # Titre avec l'année
    ws.merge_cells("A1:D1")
    titre_cell = ws.cell(
        row=1, column=1,
        value=f"MODÈLE DE DONNÉES DE SALAIRE DES FONCTIONNAIRES — {annee_label}"
    )
    titre_cell.font      = font_titre
    titre_cell.fill      = fill_titre
    titre_cell.alignment = al_centre
    ws.row_dimensions[1].height = 28

    # En-têtes colonnes
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

    # Largeurs colonnes
    ws.column_dimensions["A"].width = 55
    for col_letter in ["B", "C", "D"]:
        ws.column_dimensions[col_letter].width = 22
    ws.freeze_panes = "A3"
    print(f"   ✓ {sheet_name} ({row_cur - 3} lignes)")


def charger_silver_filtres_poste() -> pl.DataFrame | None:
    """
    Lit les fichiers Silver, exclut l'organisme 59, filtre les individus
    ayant au moins une ligne avec CODE_POSTE dans CODES_POSTE_ELIGIBLES,
    retient TOUTES leurs lignes (autres postes inclus) et calcule
    REVENU = sum(montant_brut) par individu × période.
    Retourne None si la colonne CODE_POSTE est absente.
    """
    bucket_silver = os.getenv("BUCKET_SILVER", "silver")
    prefix_silver = "panel_admin"

    pages_s = s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket_silver, Prefix=prefix_silver
    )
    cles_silver = sorted([
        obj["Key"]
        for page in pages_s
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ])

    if not cles_silver:
        print("   ⚠️  Aucun fichier Silver trouvé — analyse postes ignorée")
        return None

    frames_silver = []
    for cle in cles_silver:
        buf = io.BytesIO()
        s3.download_fileobj(bucket_silver, cle, buf)
        buf.seek(0)
        df_s = pl.read_parquet(buf)

        # Exclusion organisme 59
        if "CODE_ORGANISME" in df_s.columns:
            df_s = df_s.filter(pl.col("CODE_ORGANISME").cast(pl.Utf8) != CODE_ORGANISME_EXCLUS)

        frames_silver.append(df_s)

    if not frames_silver:
        return None

    silver = pl.concat(frames_silver, how="diagonal")
    del frames_silver
    gc.collect()

    if "CODE_POSTE" not in silver.columns:
        print("   ⚠️  Colonne CODE_POSTE absente du Silver — analyse postes ignorée")
        return None

    if "montant_brut" not in silver.columns:
        print("   ⚠️  Colonne montant_brut absente du Silver — analyse postes ignorée")
        return None

    # Cast CODE_POSTE en string pour la comparaison
    silver = silver.with_columns(
        pl.col("CODE_POSTE").cast(pl.Utf8).str.strip_chars().alias("CODE_POSTE")
    )

    # Identifier les individus éligibles (matricule × mois_annee avec CODE_POSTE éligible)
    cle_ind = [c for c in ["matricule", "mois_annee"] if c in silver.columns]
    if not cle_ind:
        print("   ⚠️  Colonnes matricule/mois_annee absentes — analyse postes ignorée")
        return None

    individus_eligibles = (
        silver
        .filter(pl.col("CODE_POSTE").is_in(CODES_POSTE_ELIGIBLES))
        .select(cle_ind)
        .unique()
        .with_columns(pl.lit(True).alias("_eligible"))
    )

    # Conserver TOUTES les lignes des individus éligibles
    silver_filtre = (
        silver
        .join(individus_eligibles, on=cle_ind, how="inner")
        .drop("_eligible")
    )

    n_individus = silver_filtre.select(cle_ind).unique().select(pl.len()).item()
    print(f"   Individus éligibles (postes {sorted(CODES_POSTE_ELIGIBLES)}) : "
          f"{n_individus:,}")
    print(f"   Lignes retenues (tous postes) : {len(silver_filtre):,}")

    # Calcul REVENU = sum(montant_brut) UNIQUEMENT sur les lignes
    # avec CODE_POSTE éligible (124, 228, 271, 421, 425)
    silver_eligibles = silver_filtre.filter(
        pl.col("CODE_POSTE").is_in(CODES_POSTE_ELIGIBLES)
    )
    print(f"   Lignes éligibles pour le calcul REVENU : {len(silver_eligibles):,}")

    revenu_calcule = (
        silver_eligibles
        .group_by(cle_ind)
        .agg(
            pl.col("montant_brut").cast(pl.Float64, strict=False).sum().alias("REVENU")
        )
    )

    # Métadonnées récupérées depuis silver_filtre (toutes les lignes)
    cols_meta = [
        c for c in ["CODE_ORGANISME", "CODE_GRADE", "GRADE", "bareme",
                    "organisme", "sexe", "Code_CITP", "Metier_CITP",
                    "emploi", "situation_normalisee"]
        if c in silver_filtre.columns
    ]
    meta = (
        silver_filtre
        .group_by(cle_ind)
        .agg([pl.col(c).first().alias(c) for c in cols_meta])
    )

    revenu = revenu_calcule.join(meta, on=cle_ind, how="left")
    print(f"   REVENU calculé pour {len(revenu):,} observations individu × période\n")
    return revenu


def charger_anstat_2025() -> pl.DataFrame | None:
    """
    Charge le fichier anstat_2025.txt.xlsx depuis staging/referentiel/,
    crée la colonne montant_brut_sans_indemnites =
        salaire_de_base + résidence + logement + transport + allocation_familiale.
    Retourne None si le fichier est absent.
    """
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        s3.download_file(BUCKET_STAGING, KEY_ANSTAT_2025, tmp_path)
        df = pl.read_excel(tmp_path)
        print(f"   ✓ anstat_2025.txt.xlsx chargé : {len(df):,} lignes × {len(df.columns)} cols")
        print(f"     Colonnes : {df.columns}")
    except Exception as e:
        print(f"   ⚠️  Fichier anstat_2025.txt.xlsx introuvable ({e}) — étape barème ignorée")
        return None
    finally:
        os.unlink(tmp_path)

    # Renommage de la colonne allocation (ex : "Allocations\n (2 enfants)") → allocation_familiale
    col_alloc = next((c for c in df.columns if "alloc" in c.lower()), None)
    if col_alloc and col_alloc != "allocation_familiale":
        df = df.rename({col_alloc: "allocation_familiale"})
        print(f"     Renommage : '{col_alloc}' → 'allocation_familiale'")

    # Mapping flexible des colonnes de composantes de salaire
    # Patterns de recherche pour chaque composante
    patterns_composantes = {
        "salaire_base":       [r"salaire.*base", r"base.*salaire", r"traitement.*base",
                               r"^salaire$", r"^base$"],
        "residence":          [r"r[eé]sidence", r"indem.*r[eé]s"],
        "logement":           [r"logement", r"indem.*log"],
        "transport":          [r"transport", r"indem.*transp"],
        "allocation_famille": [r"alloc.*fam", r"fam.*alloc", r"allocation.*familiale",
                               r"fam.*charge"],
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
            print(f"     {nom} → {col}")
        else:
            print(f"     ⚠️  Composante '{nom}' non trouvée dans le fichier")

    if not composantes_trouvees:
        print("   ⚠️  Aucune composante de salaire identifiée")
        return None

    # Cast et création de montant_brut_sans_indemnites
    df = df.with_columns([
        pl.col(c).cast(pl.Float64, strict=False)
        for c in composantes_trouvees.values()
    ])
    df = df.with_columns(
        pl.sum_horizontal([pl.col(c) for c in composantes_trouvees.values()])
        .alias("montant_brut_sans_indemnites")
    )

    n_non_null = int(df["montant_brut_sans_indemnites"].is_not_null().sum())
    print(f"   montant_brut_sans_indemnites : {n_non_null:,} valeurs non nulles")
    return df


def calculer_indicateurs_cluster(df_ref: pl.DataFrame) -> pl.DataFrame | None:
    """
    Calcule min, max, moyenne, médiane de montant_brut_sans_indemnites
    par cluster (secteur, grade) dans le fichier de référence.
    """
    if "montant_brut_sans_indemnites" not in df_ref.columns:
        return None

    # Trouver colonnes secteur et grade dans le fichier de référence
    cols_ref = df_ref.columns

    def trouver_col(cols, patterns):
        # Itérer patterns en premier pour respecter l'ordre de priorité
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
        print(f"   Colonne secteur : {col_secteur}")
    else:
        print("   ⚠️  Colonne secteur non trouvée — agrégation sur grade uniquement")

    if col_grade:
        cluster_cols.append(col_grade)
        print(f"   Colonne grade   : {col_grade}")
    else:
        print("   ⚠️  Colonne grade non trouvée — agrégation globale")

    if not cluster_cols:
        cluster_cols = [df_ref.columns[0]]
        print(f"   Repli sur première colonne : {cluster_cols[0]}")

    indic = (
        df_ref
        .filter(pl.col("montant_brut_sans_indemnites").is_not_null()
                & (pl.col("montant_brut_sans_indemnites") > 0))
        .group_by(cluster_cols)
        .agg([
            pl.col("montant_brut_sans_indemnites").min().round(0).alias("min_bareme"),
            pl.col("montant_brut_sans_indemnites").max().round(0).alias("max_bareme"),
            pl.col("montant_brut_sans_indemnites").mean().round(0).alias("moy_bareme"),
            pl.col("montant_brut_sans_indemnites").median().round(0).alias("med_bareme"),
            pl.len().alias("nb_obs_ref"),
        ])
        .sort(cluster_cols)
    )

    # Normaliser les noms de colonnes cluster → secteur_ref / grade_ref
    rename_map = {}
    if col_secteur:
        rename_map[col_secteur] = "secteur_ref"
    if col_grade:
        rename_map[col_grade] = "grade_ref"
    if rename_map:
        indic = indic.rename(rename_map)

    print(f"   Indicateurs par cluster : {len(indic):,} lignes")
    return indic, col_secteur, col_grade


# ============================================================
# 1. CHARGEMENT
# ============================================================

print("1. Chargement des fichiers Gold panel...")

pages_panel = s3.get_paginator("list_objects_v2").paginate(
    Bucket=BUCKET_GOLD, Prefix=PREFIX_PANEL
)
cles_panel = sorted([
    obj["Key"]
    for page in pages_panel
    for obj in page.get("Contents", [])
    if re.search(r"panel_20\d{2}\.parquet$", obj["Key"])
])

print(f"   Fichiers panel trouvés : {len(cles_panel)}")

frames = []
for cle in cles_panel:
    print(f"   Lecture {os.path.basename(cle)}...", end=" ", flush=True)
    df_part = lire_parquet_s3(BUCKET_GOLD, cle)
    frames.append(df_part)
    print(f"✓ {len(df_part):,} lignes")

base = pl.concat(frames, how="diagonal")
del frames
gc.collect()
print(f"   ✓ Total : {len(base):,} lignes × {len(base.columns)} colonnes")

# Exclusion défensive de l'organisme 59 (au cas où le Gold
# aurait été produit avant l'ajout du filtre dans 03_bronze_to_silver.py)
if "CODE_ORGANISME" in base.columns:
    n_avant = len(base)
    base = base.filter(pl.col("CODE_ORGANISME").cast(pl.Utf8) != CODE_ORGANISME_EXCLUS)
    n_exclu = n_avant - len(base)
    if n_exclu > 0:
        print(f"   Exclusion organisme {CODE_ORGANISME_EXCLUS} : {n_exclu:,} lignes supprimées")
print(f"   Base finale : {len(base):,} lignes\n")

# ============================================================
# 2. VÉRIFICATION GRADE ET GRADE_SOURCE
# ============================================================

print("2. Vérification des colonnes GRADE et GRADE_SOURCE...")

if "GRADE" not in base.columns or "GRADE_SOURCE" not in base.columns:
    raise RuntimeError(
        "Colonnes GRADE et/ou GRADE_SOURCE absentes du Gold.\n"
        "Exécutez d'abord 06b_imputer_grades.py avant ce script."
    )

n_total = len(base)
for source in ["OBSERVE", "IMPUTE", "NF_NON_IMPUTABLE"]:
    n = int((base["GRADE_SOURCE"] == source).sum())
    print(f"   {source:<20} : {n:,}  ({100*n/n_total:.1f}%)")
print()

# ============================================================
# 3. EXTRACTION ANNÉE / MOIS depuis mois_annee (format MMYYYY)
# ============================================================

print("3. Extraction année/mois depuis mois_annee...")

# mois_annee est au format MMYYYY (ex. "012015")
# mois  = slice(0, 2)  →  "01"
# annee = slice(2, 4)  →  "2015" (4 chiffres à partir de la position 2)
base = base.with_columns([
    pl.col("mois_annee").str.slice(2, 4).cast(pl.Int32, strict=False).alias("ANNEE"),
    pl.col("mois_annee").str.slice(0, 2).cast(pl.Int32, strict=False).alias("MOIS_NUM"),
])

annees_dispo = sorted(base["ANNEE"].drop_nulls().unique().to_list())
print(f"   Années disponibles : {annees_dispo}")

ANNEES_ANALYSE = [2024, 2025] if PERIODE_RECENTE else [a for a in annees_dispo if a >= 2015]
print(f"   Période retenue    : {ANNEES_ANALYSE}")

base = base.filter(pl.col("ANNEE").is_in(ANNEES_ANALYSE))
print(f"   Filtrage           : {len(base):,} lignes\n")

if len(base) == 0:
    raise ValueError(f"Aucune donnée pour les années {ANNEES_ANALYSE}")

base = base.with_columns(
    pl.col("MOIS_NUM")
    .map_elements(lambda m: MOIS_FR.get(m, str(m) if m is not None else ""), return_dtype=pl.Utf8)
    .alias("MOIS")
)

# ============================================================
# 4. CALCUL DU SALAIRE BRUT (montant_brut_bareme prioritaire)
# ============================================================

print("4. Calcul du salaire brut (montant_brut_bareme prioritaire)...")

if "montant_brut_bareme" in base.columns:
    base = base.with_columns(
        pl.col("montant_brut_bareme").cast(pl.Float64, strict=False).alias("SALAIRE_BRUT")
    )
    print("   Source : montant_brut_bareme (postes 124/228/271/421/425, pré-agrégé en 06)")
else:
    # Repli sur composantes élémentaires ou montant_brut
    # Détection insensible à la casse (124_F2 ou 124_f2)
    composantes_presentes = [c for c in base.columns if c.upper() in COMPOSANTES_SALAIRE_NOMS]
    print(f"   ⚠️  montant_brut_bareme absent — repli composantes : {composantes_presentes}")
    if composantes_presentes:
        base = base.with_columns([
            pl.col(c).cast(pl.Float64, strict=False) for c in composantes_presentes
        ])
        base = base.with_columns(
            pl.sum_horizontal([pl.col(c) for c in composantes_presentes])
            .alias("SALAIRE_BRUT")
        )
    else:
        base = base.with_columns(
            pl.col("montant_brut").cast(pl.Float64, strict=False).alias("SALAIRE_BRUT")
            if "montant_brut" in base.columns
            else pl.lit(0.0).alias("SALAIRE_BRUT")
        )

n_total  = len(base)
n_pos    = int((base["SALAIRE_BRUT"] > 0).sum())
n_nul    = int(base["SALAIRE_BRUT"].is_null().sum())
n_zero   = int((base["SALAIRE_BRUT"] == 0).sum())
n_neg    = int((base["SALAIRE_BRUT"] < 0).sum())
print(f"   Salaires > 0  : {n_pos:,} ({100*n_pos/n_total:.1f}%)")
print(f"   Nuls (null)   : {n_nul:,} ({100*n_nul/n_total:.1f}%)")
print(f"   Zéros (= 0)   : {n_zero:,} ({100*n_zero/n_total:.1f}%)")
print(f"   Négatifs (< 0): {n_neg:,} ({100*n_neg/n_total:.1f}%)\n")

# ============================================================
# 5. STANDARDISATION DU SEXE
# ============================================================

print("5. Standardisation sexe...")

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
        print(f"   Exclusion sexe inconnu : {n_inconnu:,}")
    for sexe_val in ["Homme", "Femme"]:
        n = int((base["SEXE_STD"] == sexe_val).sum())
        print(f"   {sexe_val} : {n:,}")
else:
    base = base.with_columns(pl.lit("Non renseigné").alias("SEXE_STD"))
print()

# ============================================================
# 6. DÉTECTION DES MULTI-POSTES
# ============================================================

print("6. Détection multi-postes (par matricule)...")

if "NB_POSTES" in base.columns:
    # NB_POSTES déjà pré-calculé dans le Gold par le programme 06
    total_multi  = int((base["NB_POSTES"] > 1).sum())
    total_agents = len(base)
    print(f"   NB_POSTES issu du Gold (pré-calculé en étape 06)")
    print(f"   Multi-postes : {total_multi:,} / {total_agents:,} "
          f"({100*total_multi/total_agents:.1f}%)\n")
elif "matricule" in base.columns:
    # Repli : recalcul depuis les lignes (Gold non régénéré avec programme 06 modifié)
    print("   ⚠️  NB_POSTES absent du Gold — recalcul depuis les lignes (Gold à régénérer)")
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
    print(f"   Multi-postes : {total_multi:,} / {total_agents:,} "
          f"({100*total_multi/total_agents:.1f}%)\n")
else:
    base = base.with_columns(pl.lit(1).alias("NB_POSTES"))
    print("   Colonne matricule absente, NB_POSTES = 1\n")

# ============================================================
# 7. FILTRAGE SUR GRADES VALIDES
# ============================================================
# On filtre sur GRADE valide (issu de 06b, OBSERVE ou IMPUTE).
# Les NF_NON_IMPUTABLE sont exclus des calculs d'indicateurs.

print("7. Filtrage grades valides avant agrégation...")

if INCLURE_ZEROS:
    base_gv = base.filter(pl.col("GRADE").is_in(GRADES_VALIDES))
else:
    base_gv = base.filter(
        pl.col("GRADE").is_in(GRADES_VALIDES) & (pl.col("SALAIRE_BRUT") > 0)
    )

n_gv = len(base_gv)
for source in ["OBSERVE", "IMPUTE"]:
    n = int((base_gv["GRADE_SOURCE"] == source).sum())
    print(f"   {source:<10} : {n:,}  ({100*n/n_gv:.1f}%)")
print(f"   Total retenu : {n_gv:,} ({100*n_gv/len(base):.1f}%)\n")

# ============================================================
# 7b. FUZZY JOIN EMPLOI → BARÈME
# ============================================================

print("7b. Fuzzy join EMPLOI → BARÈME (via table de correspondance)...")

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
        print("  ⚠️  Table barème absente — feuilles barème non produites")
    else:
        print(f"  ⚠️  Colonne emploi absente dans le panel — feuilles barème non produites")
        print(f"       Colonnes disponibles : {base_gv.columns}")
    BAREME_DISPONIBLE = False

print()

# ============================================================
# 7c. STATUT BARÈME — calcul anticipé pour filtrer sur "Conformes"
# ============================================================
# Le Statut est calculé ici (avant les indicateurs) pour que
# les indicateurs ne portent que sur les agents "Conformes au barème".
# Source : montant_brut_bareme du Gold (pré-agrégé en étape 06).
# Plus besoin de remonter au Silver.

print("7c. Calcul Statut barème (filtrage sur conformes avant indicateurs)...")
print()

# --- Chargement référentiel anstat_2025 ---
print("  7c-1. Chargement référentiel anstat_2025.txt.xlsx...")
df_anstat_ref   = charger_anstat_2025()
indic_cluster   = None
col_secteur_ref = None
col_grade_ref   = None

if df_anstat_ref is not None and "montant_brut_sans_indemnites" in df_anstat_ref.columns:
    print("  7c-2. Calcul indicateurs par cluster (secteur, grade)...")
    result_cluster = calculer_indicateurs_cluster(df_anstat_ref)
    if result_cluster is not None:
        indic_cluster, col_secteur_ref, col_grade_ref = result_cluster

# --- Création variable Statut directement sur base_gv ---
base_avec_statut = pl.DataFrame()

if indic_cluster is not None and "montant_brut_bareme" in base_gv.columns:
    print("  7c-3. Création variable Statut sur base_gv (montant_brut_bareme)...")

    # Mapping des noms de barème entre fichier de correspondance et anstat_2025
    # Clé = valeur dans base_gv (correspondance) | Valeur = nom dans anstat_2025
    MAPPING_BAREME_ANSTAT = {
        "Cadre-Sup-Santé Generaliste & Specialiste" : "Cadre-Sup-Santé",
        "police"                                     : "para militaire",
    }

    col_secteur_main = next(
        (c for c in ["bareme", "organisme", "CODE_ORGANISME"] if c in base_gv.columns),
        None,
    )
    col_grade_main = next(
        (c for c in ["GRADE", "CODE_GRADE", "grade"] if c in base_gv.columns),
        None,
    )

    # Appliquer le mapping sur la colonne secteur avant la jointure
    if col_secteur_main and col_secteur_main in base_gv.columns:
        n_avant_mapping = base_gv[col_secteur_main].is_not_null().sum()
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
                print(f"   Mapping barème : '{ancien}' → '{nouveau}' ({n_rem:,} agents)")

    cle_join_cluster = []
    if "secteur_ref" in indic_cluster.columns and col_secteur_main:
        cle_join_cluster.append(("secteur_ref", col_secteur_main))
    if "grade_ref" in indic_cluster.columns and col_grade_main:
        cle_join_cluster.append(("grade_ref", col_grade_main))

    if cle_join_cluster:
        left_on  = [pair[1] for pair in cle_join_cluster]
        right_on = [pair[0] for pair in cle_join_cluster]

        # ── DIAGNOSTIC jointure ───────────────────────────────────────────
        print(f"   Clés jointure — Gold : {left_on}  |  Référentiel : {right_on}")
        for l, r in zip(left_on, right_on):
            vals_gold = base_gv[l].drop_nulls().unique().sort().to_list()[:10]
            vals_ref  = indic_cluster[r].drop_nulls().unique().sort().to_list()
            print(f"   [{l}] Gold (10 ex.) : {vals_gold}")
            print(f"   [{r}] Référentiel   : {vals_ref}")
        # ─────────────────────────────────────────────────────────────────

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
            .when(pl.col("montant_brut_bareme") < pl.col("min_bareme"))
            .then(pl.lit("Inférieur au minimum du barème"))
            .when(pl.col("montant_brut_bareme") > pl.col("max_bareme"))
            .then(pl.lit("Supérieur au maximum du barème"))
            .otherwise(pl.lit("Conforme au barème"))
            .alias("Statut")
        )
        base_avec_statut = base_gv.clone()
    else:
        print("   ⚠️  Jointure cluster impossible — Statut non calculé")
        base_gv = base_gv.with_columns(pl.lit("Cluster non déterminé").alias("Statut"))
        base_avec_statut = base_gv.clone()

    n_statut  = base_gv["Statut"].value_counts().sort("Statut")
    n_total_s = n_statut["count"].sum()
    print(f"   Distribution Statut :")
    for row in n_statut.iter_rows(named=True):
        pct = 100 * row["count"] / n_total_s if n_total_s > 0 else 0.0
        print(f"     {row['Statut']:<40} : {row['count']:>10,}  ({pct:.1f}%)")

elif "montant_brut_bareme" not in base_gv.columns:
    print("   ⚠️  montant_brut_bareme absent du Gold — régénérer le Gold avec programme 06 modifié")
    base_gv = base_gv.with_columns(pl.lit("Cluster non déterminé").alias("Statut"))
    base_avec_statut = base_gv.clone()
else:
    base_gv = base_gv.with_columns(pl.lit("Cluster non déterminé").alias("Statut"))
    base_avec_statut = base_gv.clone()

# --- Filtrage "Conforme au barème" ---
if "Statut" in base_gv.columns:
    n_avant = len(base_gv)
    base_gv = base_gv.filter(pl.col("Statut") == "Conforme au barème")
    n_apres = len(base_gv)
    if n_avant > 0:
        print(f"\n   Filtrage Conformes : {n_apres:,} / {n_avant:,} "
              f"({100*n_apres/n_avant:.1f}% retenus pour les indicateurs)")
else:
    print("   ⚠️  Statut non disponible — indicateurs calculés sur tous les individus")

print()

# ============================================================
# 8. AGRÉGATION → REVENU TOTAL
# ============================================================

agg_col   = "cle_unique" if MODE_SALAIRE_BRUT else "matricule"
agg_label = agg_col
print(f"8. Agrégation par {agg_label} → REVENU_TOTAL...")

if agg_col not in base_gv.columns:
    agg_col = "matricule"

base_gv = base_gv.with_columns(
    pl.col("GRADE").map_elements(grade_order_value, return_dtype=pl.Int32)
    .alias("GRADE_ORDER")
)

cols_agg = ["ANNEE", "MOIS_NUM", "MOIS", agg_col]

base_agrege = (
    base_gv
    .group_by(cols_agg)
    .agg([
        # REVENU_TOTAL = montant_brut_bareme (Gold dédupliqué = 1 ligne/individu×mois)
        pl.col("SALAIRE_BRUT").first().alias("REVENU_TOTAL"),
        pl.col("montant_brut_total").first().alias("montant_brut_total")
          if "montant_brut_total" in base_gv.columns
          else pl.lit(None).cast(pl.Float64).alias("montant_brut_total"),
        pl.col("NB_POSTES").first().alias("NB_POSTES"),
        pl.col("GRADE").sort_by("GRADE_ORDER", descending=True).first().alias("GRADE_PRINCIPAL"),
        pl.col("GRADE_SOURCE").sort_by("GRADE_ORDER", descending=True).first().alias("GRADE_SOURCE_PRINCIPAL"),
        pl.col("SEXE_STD").first().alias("SEXE_STD"),
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
    base_agrege = base_agrege.filter(pl.col("REVENU_TOTAL") > 0)
base_agrege = base_agrege.filter(pl.col("REVENU_TOTAL").is_not_null())

print(f"   {len(base_agrege):,} agents agrégés\n")

# ============================================================
# 8b. WINSORISATION SUR MONTANT_BRUT_TOTAL (sauvegardé pour autres usages)
#     Les indicateurs seront calculés sur REVENU_TOTAL (= montant_brut_bareme)
# ============================================================

value_col_indic = "REVENU_TOTAL"

if SEUIL_WINS is not None and "montant_brut_total" in base_agrege.columns:
    wins_full     = [c for c in ["ANNEE", "MOIS_NUM", "GRADE_PRINCIPAL", "Code_CITP"]
                     if c in base_agrege.columns]
    wins_fallback = [c for c in ["ANNEE", "MOIS_NUM", "GRADE_PRINCIPAL"]
                     if c in base_agrege.columns]
    print(f"8b. Winsorisation MONTANT_BRUT_TOTAL "
          f"(P{int(SEUIL_WINS*100)}/P{int((1-SEUIL_WINS)*100)}) "
          f"— seuil groupe : {MIN_OBS_WINS} obs...")

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
    print(f"   Groupe mois × grade × CITP (≥{MIN_OBS_WINS} obs) : {n_obs_citp:,} obs")
    print(f"   Repli  mois × grade         (<{MIN_OBS_WINS} obs) : {n_obs_grade:,} obs")
    n_total_agrege = len(base_agrege)
    pct_modif = (100 * n_modif / n_total_agrege) if n_total_agrege > 0 else 0.0
    print(f"   Observations écrêtées : {n_modif:,}  ({pct_modif:.2f}%)")
    print(f"   → montant_brut_total_W sauvegardé pour autres usages")
    print(f"   → Indicateurs calculés sur REVENU_TOTAL (montant_brut_bareme)\n")
elif SEUIL_WINS is not None:
    print(f"8b. ⚠️  montant_brut_total absent — winsorisation ignorée\n")

# ============================================================
# 9. INDICATEURS SUR LE REVENU TOTAL
# ============================================================

print("9. Calcul indicateurs REVENU TOTAL...")

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
# 10. INDICATEURS SALAIRE BRUT (par ligne, pivot grade)
# ============================================================

print("10. Calcul indicateurs SALAIRE BRUT (pivot grade)...")

base_gv_brut = base_gv
if SEUIL_WINS is not None:
    wins_full_brut     = [c for c in ["ANNEE", "MOIS_NUM", "GRADE", "Code_CITP"]
                          if c in base_gv.columns]
    wins_fallback_brut = [c for c in ["ANNEE", "MOIS_NUM", "GRADE"]
                          if c in base_gv.columns]

    base_gv_brut = base_gv.with_columns([
        pl.len().over(wins_full_brut).alias("_n_citp"),
        pl.col("SALAIRE_BRUT")
          .quantile(SEUIL_WINS, interpolation="nearest")
          .over(wins_full_brut).alias("_pb_citp"),
        pl.col("SALAIRE_BRUT")
          .quantile(1 - SEUIL_WINS, interpolation="nearest")
          .over(wins_full_brut).alias("_ph_citp"),
        pl.col("SALAIRE_BRUT")
          .quantile(SEUIL_WINS, interpolation="nearest")
          .over(wins_fallback_brut).alias("_pb_grade"),
        pl.col("SALAIRE_BRUT")
          .quantile(1 - SEUIL_WINS, interpolation="nearest")
          .over(wins_fallback_brut).alias("_ph_grade"),
    ])
    base_gv_brut = base_gv_brut.with_columns(
        pl.when(pl.col("_n_citp") >= MIN_OBS_WINS)
        .then(pl.col("SALAIRE_BRUT").clip(pl.col("_pb_citp"), pl.col("_ph_citp")))
        .otherwise(pl.col("SALAIRE_BRUT").clip(pl.col("_pb_grade"), pl.col("_ph_grade")))
        .alias("SALAIRE_BRUT")
    ).drop(["_n_citp", "_pb_citp", "_ph_citp", "_pb_grade", "_ph_grade"])

    n_modif_brut = int((base_gv_brut["SALAIRE_BRUT"] != base_gv["SALAIRE_BRUT"]).sum())
    print(f"   Winsorisation SALAIRE_BRUT (mois × grade × CITP / repli grade) : "
          f"{n_modif_brut:,} obs écrêtées")

brut_moyen = (
    base_gv_brut
    .group_by(["ANNEE", "MOIS_NUM", "MOIS", "GRADE"])
    .agg(pl.col("SALAIRE_BRUT").mean().round(0).alias("Salaire_brut_moyen"))
    .sort(["ANNEE", "MOIS_NUM"])
)
brut_median = (
    base_gv_brut
    .group_by(["ANNEE", "MOIS_NUM", "MOIS", "GRADE"])
    .agg(pl.col("SALAIRE_BRUT").median().round(0).alias("Salaire_brut_median"))
    .sort(["ANNEE", "MOIS_NUM"])
)

brut_moyen_wide = brut_moyen.pivot(
    values="Salaire_brut_moyen",
    index=["ANNEE", "MOIS_NUM", "MOIS"],
    on="GRADE"
).sort(["ANNEE", "MOIS_NUM"])

brut_median_wide = brut_median.pivot(
    values="Salaire_brut_median",
    index=["ANNEE", "MOIS_NUM", "MOIS"],
    on="GRADE"
).sort(["ANNEE", "MOIS_NUM"])

print(f"   Brut moyen  : {len(brut_moyen_wide)} lignes × {len(brut_moyen_wide.columns)} cols")
print(f"   Brut médian : {len(brut_median_wide)} lignes × {len(brut_median_wide.columns)} cols\n")

# ============================================================
# 11. MULTI-POSTES
# ============================================================

print("11. Création feuilles multi-postes...")

multi_detail = (
    base_agrege
    .group_by(["ANNEE", "MOIS_NUM", "MOIS", "GRADE_PRINCIPAL", "NB_POSTES"])
    .agg([
        pl.len().alias("Effectif"),
        pl.col("REVENU_TOTAL").mean().round(0).alias("Revenu_moyen"),
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
        pl.col("REVENU_TOTAL").mean().round(0).alias("Revenu_moyen"),
    ])
    .sort("NB_POSTES")
)

print(f"   Multi-postes détail  : {len(multi_effectif)} lignes")
print(f"   Distribution globale : {len(distrib_postes)} lignes\n")

# ============================================================
# 12. PRÉ-CALCUL DONNÉES MODELE_ANSTAT PAR ANNÉE
# ============================================================
# On prépare un dict {annee: (global_brut, stats_bareme_detail)}
# pour éviter de refiltrer base_gv_b plusieurs fois.

anstat_data_par_annee: dict[int, tuple[dict, pl.DataFrame]] = {}

if BAREME_DISPONIBLE:
    print("12. Pré-calcul données MODELE_ANSTAT par année...")
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
# 13b. RAPPEL — variables Statut déjà calculées en section 7c
# ============================================================
# base_revenus, df_anstat_ref, indic_cluster, base_avec_statut
# sont déjà disponibles depuis la section 7c.
# Aucun recalcul nécessaire ici.

print("13b. Variables Statut/barème déjà calculées en section 7c — OK\n")

# ============================================================
# 13. EXPORT EXCEL
# ============================================================

print("13. Export Excel...")

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


wb = Workbook()
wb.remove(wb.active)

ecrire_feuille(wb, "REVENU_Grade_Detail",      indic_grade)
ecrire_feuille(wb, "REVENU_Grade_Sexe",         indic_grade_sexe)
if len(indic_citp) > 0:
    ecrire_feuille(wb, "REVENU_CITP_Detail",    indic_citp)
ecrire_feuille(wb, "SALAIRE_BRUT_Moyen",        brut_moyen_wide)
ecrire_feuille(wb, "SALAIRE_BRUT_Median",       brut_median_wide)
ecrire_feuille(wb, "MULTI_Par_Grade_NbPostes",  multi_effectif)
ecrire_feuille(wb, "MULTI_Distribution",        distrib_postes)

# --- Feuilles barème (si disponibles) ---
if BAREME_DISPONIBLE:
    ecrire_feuille(wb, "REVENU_Bareme_Detail",  indic_bareme)
    ecrire_feuille(wb, "REVENU_Bareme_Grade",   indic_bareme_grade)
    ecrire_feuille(wb, "REVENU_Bareme_Sexe",    indic_bareme_sexe)

    # --- Feuilles MODELE_ANSTAT_2024 et MODELE_ANSTAT_2025 ---
    # Une feuille par année, construite depuis les données filtrées
    # calculées à l'étape 12.
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

# ── Vérification : au moins une feuille doit contenir des données ────
feuilles_visibles = [ws for ws in wb.worksheets if ws.sheet_state == "visible"]
feuilles_avec_data = [ws for ws in feuilles_visibles if ws.max_row > 1]
if not feuilles_avec_data:
    raise ValueError(
        "ARRÊT — Le classeur Excel ne contient aucune feuille avec données. "
        "Vérifier que l'étape 7c (barème) a produit des agents conformes et "
        "que le fichier anstat_2025.txt.xlsx est bien disponible dans "
        f"staging/{KEY_ANSTAT_2025}."
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
print(f"   Feuilles : {len(wb.sheetnames)}\n")

# ============================================================
# 14. EXPORT EXCEL — INDICATEURS BARÈME SANS INDEMNITÉS + STATUT
# ============================================================

if indic_cluster is not None or len(base_avec_statut) > 0:
    print("14. Export Excel indicateurs barème sans indemnités...")

    wb2 = Workbook()
    wb2.remove(wb2.active)

    # Feuille 1 : Indicateurs barème par cluster (depuis référentiel)
    if indic_cluster is not None and len(indic_cluster) > 0:
        ecrire_feuille(wb2, "INDIC_Cluster_Secteur_Grade", indic_cluster)

    # Feuille 2 : Référentiel complet avec montant_brut_sans_indemnites
    if df_anstat_ref is not None and len(df_anstat_ref) > 0:
        # Limiter aux colonnes pertinentes pour ne pas surcharger
        cols_export_ref = [c for c in df_anstat_ref.columns
                           if c != "montant_brut_sans_indemnites"] + ["montant_brut_sans_indemnites"]
        ecrire_feuille(wb2, "REF_ANSTAT_2025_Complet",
                       df_anstat_ref.select(cols_export_ref))

    # Feuille 3 : Base principale avec Statut
    if len(base_avec_statut) > 0:
        ecrire_feuille(wb2, "BASE_AVEC_STATUT", base_avec_statut)

        # Feuille 4 : Résumé statistique de la variable Statut
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
            ecrire_feuille(wb2, "RESUME_STATUT", resume_statut)

    buf2 = io.BytesIO()
    wb2.save(buf2)
    buf2.seek(0)
    bytes2 = buf2.getvalue()

    s3.put_object(
        Bucket=BUCKET_STAGING,
        Key=KEY_INDIC_BAREME_SANS_INDEM,
        Body=bytes2,
        ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    taille2_kb = len(bytes2) / 1024
    print(f"\n✅ Fichier indicateurs barème : s3://{BUCKET_STAGING}/{KEY_INDIC_BAREME_SANS_INDEM}")
    print(f"   Taille   : {taille2_kb:.1f} KB")
    print(f"   Feuilles : {len(wb2.sheetnames)}\n")
else:
    print("14. ⚠️  Aucun indicateur barème sans indemnités à exporter\n")

print("=" * 70)
print("✓ CALCUL INDICATEURS TERMINÉ")
print("=" * 70)
print()
print("Prochaine étape : exécuter 08_creation_fichier_excel_avec_sommaire.py")