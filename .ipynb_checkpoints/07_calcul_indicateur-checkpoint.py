
# ============================================================
# PANEL ADMIN — ÉTAPE 7 : CALCUL DU STATUT BARÈME
# ============================================================
# Calcule le statut barème (Conforme / Inférieur / Supérieur /
# Cluster non déterminé) pour chaque agent fonctionnaire à partir
# des fichiers Gold panel et du référentiel anstat_2025.
#
# Prérequis : 06_compiler_panel.py doit avoir été exécuté.
# Les colonnes GRADE et GRADE_SOURCE sont lues directement depuis
# le Gold panel.
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
#
# Pipeline :
#   07  (ce script)   → Calcul Statut barème → base_avec_statut.parquet
#   07a               → Imputation hors-barème → base_avec_statut_corrigee.parquet
#   07b               → Indicateurs salaires (conformes + imputés)
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
PERIODE_RECENTE   = False

# True = inclure zéros | False = salaire strictement > 0
INCLURE_ZEROS     = False

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

# Composantes du salaire brut (codes F2, suffixe _F2 dans le Gold)
COMPOSANTES_SALAIRE_NOMS = {"124_F2", "228_F2", "271_F2", "421_F2", "425_F2"}

# Codes poste éligibles pour le calcul du revenu (filtrage individus)
CODES_POSTE_ELIGIBLES = {"124", "228", "271", "421", "425"}

# Code organisme à exclure de tous les traitements
CODE_ORGANISME_EXCLUS = "59"

# Fichier de référence barème 2025 (dans staging/panel_admin/references/)
KEY_ANSTAT_2025 = "panel_admin/references/anstat_2025.txt.xlsx"

# Clé de sortie base_avec_statut
KEY_BASE_AVEC_STATUT = f"{PREFIX_PANEL}/base_avec_statut.parquet"

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
print("STATUT BARÈME — Calcul du statut (Conforme/Inférieur/Supérieur)")
print(f"  Période : {'2024-2025' if PERIODE_RECENTE else '2015 → récent'}")
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
    texte = re.sub(r"[''\u2019]", " ", texte)
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

    # Renommage de la colonne allocation
    col_alloc = next((c for c in df.columns if "alloc" in c.lower()), None)
    if col_alloc and col_alloc != "allocation_familiale":
        df = df.rename({col_alloc: "allocation_familiale"})
        print(f"     Renommage : '{col_alloc}' → 'allocation_familiale'")

    # Mapping flexible des colonnes de composantes de salaire
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

# Exclusion défensive de l'organisme 59
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
# 4. VÉRIFICATION montant_brut_bareme
# ============================================================

print("4. Vérification montant_brut_bareme (pré-calculé en étape 06)...")

if "montant_brut_bareme" not in base.columns:
    raise RuntimeError(
        "Colonne montant_brut_bareme absente du Gold panel.\n"
        "Vérifiez que l'étape 06_compiler_panel.py a été exécutée avec les colonnes F2."
    )

# Cast en Float64 sur place (la colonne peut arriver en Int64 depuis le parquet)
base = base.with_columns(
    pl.col("montant_brut_bareme").cast(pl.Float64, strict=False)
)

n_total  = len(base)
n_pos    = int((base["montant_brut_bareme"] > 0).sum())
n_nul    = int(base["montant_brut_bareme"].is_null().sum())
n_zero   = int((base["montant_brut_bareme"] == 0).sum())
n_neg    = int((base["montant_brut_bareme"] < 0).sum())
print(f"   Source : montant_brut_bareme (postes 124/228/271/421/425, pré-agrégé en 06)")
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
    total_multi  = int((base["NB_POSTES"] > 1).sum())
    total_agents = len(base)
    print(f"   NB_POSTES issu du Gold (pré-calculé en étape 06)")
    print(f"   Multi-postes : {total_multi:,} / {total_agents:,} "
          f"({100*total_multi/total_agents:.1f}%)\n")
elif "matricule" in base.columns:
    print("   ⚠️  NB_POSTES absent du Gold — recalcul depuis les lignes")
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

print("7. Filtrage grades valides avant agrégation...")

if INCLURE_ZEROS:
    base_gv = base.filter(pl.col("GRADE").is_in(GRADES_VALIDES))
else:
    base_gv = base.filter(
        pl.col("GRADE").is_in(GRADES_VALIDES) & (pl.col("montant_brut_bareme") > 0)
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
    BAREME_DISPONIBLE = False

# --- Exclusion police (non-fonctionnaires — pas de statut barème) ---
if "bareme" in base_gv.columns:
    n_police = int((base_gv["bareme"] == "police").sum())
    base_gv = base_gv.filter(pl.col("bareme") != "police")
    print(f"   Exclusion police (NF) : {n_police:,} agents retirés")

# --- Exclusion montant_brut_bareme = 0 ou null ---
if "montant_brut_bareme" in base_gv.columns:
    n_zero = int(
        (base_gv["montant_brut_bareme"].is_null() | (base_gv["montant_brut_bareme"] == 0)).sum()
    )
    base_gv = base_gv.filter(
        pl.col("montant_brut_bareme").is_not_null() & (pl.col("montant_brut_bareme") != 0)
    )
    print(f"   Exclusion sans salaire (montant_brut_bareme ≤ 0) : {n_zero:,} agents retirés")

print(f"   Base après exclusions : {len(base_gv):,} agents")
print()

# ============================================================
# 7c. STATUT BARÈME
# ============================================================

print("7c. Calcul Statut barème...")
print()

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

base_avec_statut = pl.DataFrame()

if indic_cluster is not None and "montant_brut_bareme" in base_gv.columns:
    print("  7c-3. Création variable Statut sur base_gv (montant_brut_bareme)...")

    MAPPING_BAREME_ANSTAT = {
        "Cadre-Sup-Santé Generaliste & Specialiste" : "Cadre-Sup-Santé",
    }

    col_secteur_main = next(
        (c for c in ["bareme", "organisme", "CODE_ORGANISME"] if c in base_gv.columns),
        None,
    )
    col_grade_main = next(
        (c for c in ["GRADE", "CODE_GRADE", "grade"] if c in base_gv.columns),
        None,
    )

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
                print(f"   Mapping barème : '{ancien}' → '{nouveau}' ({n_rem:,} agents)")

    cle_join_cluster = []
    if "secteur_ref" in indic_cluster.columns and col_secteur_main:
        cle_join_cluster.append(("secteur_ref", col_secteur_main))
    if "grade_ref" in indic_cluster.columns and col_grade_main:
        cle_join_cluster.append(("grade_ref", col_grade_main))

    if cle_join_cluster:
        left_on  = [pair[1] for pair in cle_join_cluster]
        right_on = [pair[0] for pair in cle_join_cluster]

        print(f"   Clés jointure — Gold : {left_on}  |  Référentiel : {right_on}")

        for l, r in zip(left_on, right_on):
            set_gold = set(base_gv[l].drop_nulls().unique().to_list())
            set_ref  = set(indic_cluster[r].drop_nulls().unique().to_list())
            dans_ref_pas_gold = sorted(set_ref - set_gold)
            dans_gold_pas_ref = sorted(set_gold - set_ref)
            print(f"   [{l}] Gold ({len(set_gold)} modalités) : {sorted(set_gold)}")
            print(f"   [{r}] Référentiel ({len(set_ref)} modalités) : {sorted(set_ref)}")
            if dans_ref_pas_gold:
                print(f"     ⚠️  Dans référentiel mais absent du Gold : {dans_ref_pas_gold}")
            if dans_gold_pas_ref:
                print(f"     ⚠️  Dans Gold mais absent du référentiel : {dans_gold_pas_ref}")

        if len(left_on) > 1:
            combis_gold = set(
                base_gv.select(left_on).drop_nulls().unique().iter_rows()
            )
            combis_ref = set(
                indic_cluster.select(right_on).drop_nulls().unique().iter_rows()
            )
            dans_ref_pas_gold_c = sorted(combis_ref - combis_gold)
            dans_gold_pas_ref_c = sorted(combis_gold - combis_ref)
            print(f"   Combinaisons ({' × '.join(left_on)}) :")
            print(f"     Gold         : {len(combis_gold):,} combinaisons")
            print(f"     Référentiel  : {len(combis_ref):,} combinaisons")
            if dans_ref_pas_gold_c:
                print(f"     ⚠️  Dans référentiel mais absent du Gold : {dans_ref_pas_gold_c}")
            if dans_gold_pas_ref_c:
                print(f"     ⚠️  Dans Gold mais absent du référentiel : {dans_gold_pas_ref_c[:20]}"
                      f"{'...' if len(dans_gold_pas_ref_c) > 20 else ''}")
            if not dans_ref_pas_gold_c and not dans_gold_pas_ref_c:
                print(f"     ✓ Combinaisons identiques des deux côtés")

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

    # --- Vérification para-militaires ---
    if col_secteur_main and col_secteur_main in base_gv.columns:
        n_para = int((base_gv[col_secteur_main] == "para militaire").sum())
        if n_para == 0:
            print(f"   ✓ Vérification para-militaires : aucun agent para-militaire dans la base")
        else:
            print(f"   ⚠️  ALERTE : {n_para:,} agents avec barème 'para militaire' détectés")

    n_statut  = base_gv["Statut"].value_counts().sort("Statut")
    n_total_s = n_statut["count"].sum()
    print(f"   Distribution Statut (avant récupération) :")
    for row in n_statut.iter_rows(named=True):
        pct = 100 * row["count"] / n_total_s if n_total_s > 0 else 0.0
        print(f"     {row['Statut']:<40} : {row['count']:>10,}  ({pct:.1f}%)")

    # --- Récupération agents hors-barème à moins de 5% des bornes ---
    cols_ok = {"montant_brut_bareme", "min_bareme", "max_bareme"}.issubset(set(base_gv.columns))
    if cols_ok:
        n_rec_inf = int(
            base_gv.filter(
                (pl.col("Statut") == "Inférieur au minimum du barème") &
                ((pl.col("min_bareme") - pl.col("montant_brut_bareme")) / pl.col("min_bareme") * 100 < 5)
            ).shape[0]
        )
        n_rec_sup = int(
            base_gv.filter(
                (pl.col("Statut") == "Supérieur au maximum du barème") &
                ((pl.col("montant_brut_bareme") - pl.col("max_bareme")) / pl.col("max_bareme") * 100 < 5)
            ).shape[0]
        )
        base_gv = base_gv.with_columns(
            pl.when(
                (pl.col("Statut") == "Inférieur au minimum du barème") &
                ((pl.col("min_bareme") - pl.col("montant_brut_bareme")) / pl.col("min_bareme") * 100 < 5)
            ).then(pl.lit("Conforme au barème"))
            .when(
                (pl.col("Statut") == "Supérieur au maximum du barème") &
                ((pl.col("montant_brut_bareme") - pl.col("max_bareme")) / pl.col("max_bareme") * 100 < 5)
            ).then(pl.lit("Conforme au barème"))
            .otherwise(pl.col("Statut"))
            .alias("Statut")
        )
        base_avec_statut = base_gv.clone()
        print(f"\n   Récupération (écart < 5%) :")
        print(f"     Inférieur récupérés : {n_rec_inf:,}")
        print(f"     Supérieur récupérés : {n_rec_sup:,}")

        n_statut2  = base_gv["Statut"].value_counts().sort("Statut")
        n_total_s2 = n_statut2["count"].sum()
        print(f"\n   Distribution Statut (après récupération) :")
        for row in n_statut2.iter_rows(named=True):
            pct = 100 * row["count"] / n_total_s2 if n_total_s2 > 0 else 0.0
            print(f"     {row['Statut']:<40} : {row['count']:>10,}  ({pct:.1f}%)")

elif "montant_brut_bareme" not in base_gv.columns:
    print("   ⚠️  montant_brut_bareme absent du Gold — régénérer le Gold avec programme 06")
    base_gv = base_gv.with_columns(pl.lit("Cluster non déterminé").alias("Statut"))
    base_avec_statut = base_gv.clone()
else:
    base_gv = base_gv.with_columns(pl.lit("Cluster non déterminé").alias("Statut"))
    base_avec_statut = base_gv.clone()

print()

# ============================================================
# 8. STATS HORS-BARÈME PAR TRANCHES (console)
# ============================================================

STATUTS_HORS_BAREME = ["Inférieur au minimum du barème", "Supérieur au maximum du barème"]

if (len(base_avec_statut) > 0 and "Statut" in base_avec_statut.columns
        and {"montant_brut_bareme", "min_bareme", "max_bareme"}.issubset(
            set(base_avec_statut.columns))):
    hors_bareme = base_avec_statut.filter(pl.col("Statut").is_in(STATUTS_HORS_BAREME))
    total_hors  = len(hors_bareme)
    if total_hors > 0:
        ORDRE_TRANCHES = ["< 5%", "5% – 10%", "10% – 25%", "25% – 50%", "≥ 50%"]
        for statut_label, borne_col in [
            ("Inférieur au minimum du barème", "min_bareme"),
            ("Supérieur au maximum du barème", "max_bareme"),
        ]:
            sous = hors_bareme.filter(pl.col("Statut") == statut_label)
            n_sous = len(sous)
            if n_sous == 0:
                continue
            if statut_label == "Inférieur au minimum du barème":
                ecart_expr = (
                    (pl.col(borne_col) - pl.col("montant_brut_bareme"))
                    / pl.col(borne_col) * 100
                )
            else:
                ecart_expr = (
                    (pl.col("montant_brut_bareme") - pl.col(borne_col))
                    / pl.col(borne_col) * 100
                )
            sous = sous.with_columns(ecart_expr.alias("ecart_pct")).with_columns(
                pl.when(pl.col("ecart_pct") < 5)  .then(pl.lit("< 5%"))
                .when(pl.col("ecart_pct") < 10)   .then(pl.lit("5% – 10%"))
                .when(pl.col("ecart_pct") < 25)   .then(pl.lit("10% – 25%"))
                .when(pl.col("ecart_pct") < 50)   .then(pl.lit("25% – 50%"))
                .otherwise(pl.lit("≥ 50%"))
                .alias("tranche")
            )
            counts = {
                row["tranche"]: row["count"]
                for row in sous["tranche"].value_counts().iter_rows(named=True)
            }
            print(f"   Écarts au barème — {statut_label} ({n_sous:,}) :")
            for t in ORDRE_TRANCHES:
                n_t = counts.get(t, 0)
                pct = 100 * n_t / n_sous if n_sous > 0 else 0.0
                print(f"     {t:<12} : {n_t:>10,}  ({pct:.1f}%)")
            print()
    else:
        print("   ℹ️  Aucun agent hors-barème détecté")

# ============================================================
# 9. SAUVEGARDE base_avec_statut.parquet → Gold
# ============================================================

print("9. Sauvegarde base_avec_statut.parquet...")

if len(base_avec_statut) > 0:
    buf_pq = io.BytesIO()
    base_avec_statut.write_parquet(buf_pq, compression="snappy")
    buf_pq.seek(0)
    s3.put_object(
        Bucket=BUCKET_GOLD,
        Key=KEY_BASE_AVEC_STATUT,
        Body=buf_pq.getvalue(),
    )
    print(f"   ✅ s3://{BUCKET_GOLD}/{KEY_BASE_AVEC_STATUT}")
    print(f"   {len(base_avec_statut):,} lignes × {len(base_avec_statut.columns)} colonnes")

    # Résumé final Statut
    if "Statut" in base_avec_statut.columns:
        total = len(base_avec_statut)
        print(f"\n   Résumé final Statut :")
        for row in (
            base_avec_statut["Statut"].value_counts().sort("Statut").iter_rows(named=True)
        ):
            pct = 100 * row["count"] / total if total > 0 else 0.0
            print(f"     {row['Statut']:<40} : {row['count']:>10,}  ({pct:.1f}%)")
else:
    print("   ⚠️  base_avec_statut vide — fichier non sauvegardé")

print()
print("=" * 70)
print("✓ CALCUL STATUT BARÈME TERMINÉ")
print("=" * 70)
print()
print("Prochaine étape : exécuter 07a_imputer_hors_bareme.py")
