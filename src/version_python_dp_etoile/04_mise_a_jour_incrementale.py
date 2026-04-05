# ============================================================
# PANEL ADMIN — MISE À JOUR INCRÉMENTALE
# ============================================================
# Détecte les nouvelles périodes dans staging (non encore
# présentes en Bronze) et les traite à travers les couches :
#   Bronze → Silver → Gold Panel
#
# Seul le delta est traité :
#   - Nouvelles périodes uniquement : Staging → Bronze
#   - Années affectées uniquement  : Bronze → Silver
#   - Années affectées uniquement  : Silver → Gold panel_YYYY
#   - Toutes années                : rebuild panel_complet
#
# NOTA : Ce script NE ré-exécute PAS les imputations (03b grades,
# 03c salaires). Après mise à jour incrémentale, si vous avez
# besoin des imputations à jour sur les nouvelles périodes,
# relancez manuellement :
#   python 03b_imputer_grades_silver.py
#   python 03c_imputer_salaires_silver.py
#   python 04_mise_a_jour_incrementale.py   ← (panel Gold uniquement)
#
# Dépendances :
#   pip install polars fastexcel boto3 python-dotenv openpyxl
# ============================================================

import gc
import io
import os
import re
import tempfile
import unicodedata
from datetime import datetime
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import warnings
import polars as pl

warnings.filterwarnings("ignore", message="Could not determine dtype")

load_dotenv(".env")

# ============================================================
# CONFIGURATION
# ============================================================

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "datalab-team")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio-datalabteam123")

BUCKET_STAGING  = os.getenv("BUCKET_STAGING", "staging")
BUCKET_BRONZE   = os.getenv("BUCKET_BRONZE",  "bronze")
BUCKET_SILVER   = os.getenv("BUCKET_SILVER",  "silver")
BUCKET_GOLD     = os.getenv("BUCKET_GOLD",    "gold")

PREFIX_MENSUELS = "panel_admin/fichiers_mensuels"
PREFIX_BRONZE   = "panel_admin"
PREFIX_SILVER   = "panel_admin"
PREFIX_PANEL    = "panel_admin"
PREFIX_REFS     = "panel_admin/references"
PREFIX_BACKUP   = "panel_admin/backups"

KEY_PANEL_COMPLET = f"{PREFIX_PANEL}/panel_complet.parquet"

CODES_A_SURVEILLER = ["CODE_ORGANISME", "CODE_GRADE", "CODE_EMPLOI"]
COMPOSANTES_F2     = ["124_f2", "228_f2", "271_f2", "421_f2", "425_f2"]

COLONNES_PANEL = [
    "cle_unique", "matricule", "nom", "sexe", "date_naissance",
    "situation_matrimoniale", "nombre_enfant",
    "situation_normalisee",
    "CODE_ORGANISME", "organisme",
    "CODE_EMPLOI",    "emploi",
    "Code_CITP",      "Metier_CITP",
    "CODE_GRADE",     "grade", "statut_fonctionnaire",
    "CODE_FONCTION",  "fonction",
    "CODE_SERVICE",   "service",
    "CODE_AFFECTATION", "lieu_affectation",
    "CODE_POSTE",
    "prise_service", "date_retraite", "age_retraite",
    "GRADE", "GRADE_SOURCE",
    "mois_annee", "annee", "mois",
    "montant_brut", "montant_net",
    "retenue_pension", "impot", "charge_patronale",
    "salaire_brut_f2",
    "montant_brut_SOURCE", "montant_net_SOURCE",
    "fichier_source",
]

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
# LOG
# ============================================================

_debut_run = datetime.now()

def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def separateur(char: str = "=", n: int = 70) -> None:
    print(char * n, flush=True)

# ============================================================
# UTILITAIRES S3
# ============================================================

def lister_objets(bucket: str, prefix: str, suffixe: str = "") -> list[str]:
    """Retourne les clés S3 sous prefix avec le suffixe donné."""
    pager = s3.get_paginator("list_objects_v2")
    return sorted([
        obj["Key"]
        for page in pager.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(suffixe) if suffixe else True
    ])


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
    log(f"  ✓ s3://{bucket}/{key}  ({len(df):,} lignes × {len(df.columns)} cols  {taille:.1f} MB)")


def objet_existe(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False

# ============================================================
# UTILITAIRES BRONZE  (mêmes fonctions que 02_staging_to_bronze)
# ============================================================

def normaliser_nom_colonne(nom: str) -> str:
    if not nom or isinstance(nom, float):
        return "colonne_vide"
    nom = str(nom)
    nom = re.sub(r"^[A-Z_]+[0-9]+\.", "", nom)
    nom = nom.upper()
    nom = re.sub(r"[-/_ ]", "_", nom)
    nom = re.sub(r"\.", "_", nom)
    nom = unicodedata.normalize("NFKD", nom)
    nom = "".join(c for c in nom if not unicodedata.combining(c))
    nom = re.sub(r"[^A-Z0-9_]", "", nom)
    nom = re.sub(r"_+", "_", nom)
    return nom.strip("_")


def deduplication_noms(noms: list[str]) -> list[str]:
    seen: dict = {}
    result = []
    for n in noms:
        if n in seen:
            seen[n] += 1
            result.append(f"{n}_{seen[n]}")
        else:
            seen[n] = 1
            result.append(n)
    return result


def detecter_entete(fichier: str, sheet_id: int) -> int:
    try:
        preview = pl.read_excel(
            fichier,
            sheet_id=sheet_id + 1,
            read_options={"header_row": None, "n_rows": 20},
        )
        rows = preview.rows()
        if len(rows) > 1:
            rows = rows[:-1]
        for i, row in enumerate(rows):
            if any("matricule" in str(v).lower() for v in row if v is not None):
                return i
        return 1 if sheet_id == 1 else 0
    except Exception:
        return 1 if sheet_id == 1 else 0


def traiter_xlsx_vers_bronze(chemin_objet: str, periode: str, tmp_path: str) -> pl.DataFrame | None:
    """Convertit un fichier XLSX staging en DataFrame Bronze (tout-String)."""
    nom_fichier = os.path.basename(chemin_objet)
    s3.download_file(BUCKET_STAGING, chemin_objet, tmp_path)

    skip_f1 = detecter_entete(tmp_path, sheet_id=0)
    feuille1 = pl.read_excel(
        tmp_path,
        sheet_id=1,
        read_options={"header_row": skip_f1},
    )

    nom_cle_f1 = next(
        (c for c in feuille1.columns
         if re.search(r"MATRICULE.*\|\|", str(c), re.IGNORECASE)),
        None,
    )
    if nom_cle_f1:
        feuille1 = feuille1.rename({nom_cle_f1: "CLE_UNIQUE"})

    noms_norm = [normaliser_nom_colonne(c) for c in feuille1.columns]
    feuille1  = feuille1.rename(dict(zip(feuille1.columns, deduplication_noms(noms_norm))))

    skip_f2 = detecter_entete(tmp_path, sheet_id=1)
    feuille2 = pl.read_excel(
        tmp_path,
        sheet_id=2,
        read_options={"header_row": skip_f2},
    )

    nom_cle_f2 = next(
        (c for c in feuille2.columns
         if re.search(r"MATRICULE.*\|\|", str(c), re.IGNORECASE)),
        None,
    )

    if nom_cle_f2:
        feuille2 = feuille2.rename({nom_cle_f2: "CLE_UNIQUE_F2"})
        rename_f2 = {
            c: normaliser_nom_colonne(c) + "_F2"
            for c in feuille2.columns if c != "CLE_UNIQUE_F2"
        }
        feuille2 = feuille2.rename(rename_f2)

        cols_num_f2       = [c for c in feuille2.columns if re.match(r"^\d+_F2$", c)]
        cols_obligatoires = {"124_F2", "421_F2"}

        if not cols_obligatoires.issubset(set(cols_num_f2)):
            log(f"      (F2 : colonnes 124_F2/421_F2 absentes, F2 ignoré)")
            feuille2 = None

        if feuille2 is not None and "CLE_UNIQUE" in feuille1.columns:
            feuille1 = feuille1.join(
                feuille2,
                left_on="CLE_UNIQUE",
                right_on="CLE_UNIQUE_F2",
                how="inner",
            )
            log(f"      (F1↔F2 : {len(feuille1):,} individus communs)")

    feuille1 = (
        feuille1
        .with_columns([
            pl.lit(periode).alias("PERIODE"),
            pl.lit(nom_fichier).alias("FICHIER_SOURCE"),
        ])
        .with_columns([pl.col(c).cast(pl.Utf8) for c in feuille1.columns])
    )
    return feuille1

# ============================================================
# UTILITAIRES SILVER  (mêmes fonctions que 03_bronze_to_silver)
# ============================================================

def normaliser_pour_matching(texte) -> str | None:
    if texte is None:
        return None
    texte = str(texte).strip()
    if not texte:
        return None
    texte = re.sub(r"[-]", " ", texte)
    texte = re.sub(r"[.,;:()\[\]/]", " ", texte)
    texte = re.sub("[''\u2019]", " ", texte)
    texte = texte.upper()
    texte = unicodedata.normalize("NFKD", texte)
    texte = "".join(c for c in texte if not unicodedata.combining(c))
    texte = re.sub(r"\s+", " ", texte).strip()
    return texte if texte else None


def mapper_colonnes(noms_colonnes: list[str]) -> dict[str, str]:
    mapping = {
        "cle_unique":             [r"MATRICULE.*CODE.*ORGANISME", r"MATRICULE.*\\|\\|.*CODE"],
        "matricule":              [r"^MATRICULE$", r"^MATRIC$"],
        "nom":                    [r"^NOM$", r"^NOM_PRENOM$", r"^NOM_ET_PRENOM$"],
        "date_naissance":         [r"DATE.*NAISSANCE", r"DATE_NAISS", r"NAISSANCE"],
        "sexe":                   [r"^SEXE$"],
        "situation_matrimoniale": [r"SITUATION.*MATRIMONIALE", r"STATUT.*MATRIMONIAL"],
        "nombre_enfant":          [r"NOMBRE.*ENFANT", r"NBR.*ENFANT"],
        "situation":              [r"^SITUATION$", r"SITUATION_ADMINISTRATIVE"],
        "date_debut_situation":   [r"DATE.*DEBUT.*SITUATION", r"DATE_SITUATION"],
        "montant_brut":           [r"MONTANT_BRUT", r"SALAIRE_BRUT", r"REMUNERATION_BRUTE", r"^BRUT$"],
        "montant_net":            [r"MONTANT_NET", r"SALAIRE_NET", r"REMUNERATION_NETTE", r"^NET$"],
        "retenue_pension":        [r"RETENUE.*PENSION", r"^PENSION$", r"COTISATION.*PENSION"],
        "impot":                  [r"^IMPOT$", r"^IGR$", r"IMPOT.*REVENU"],
        "charge_patronale":       [r"CHARGE.*PATRONALE", r"CHARGES.*PATRONALES"],
        "organisme":              [r"^ORGANISME$", r"MINISTERE", r"DIRECTION"],
        "lieu_affectation":       [r"LIEU.*AFFECTATION", r"^AFFECTATION$"],
        "service":                [r"^SERVICE$"],
        "emploi":                 [r"^EMPLOI$", r"^CORPS$", r"CORPS.*EMPLOI"],
        "fonction":               [r"^FONCTION$"],
        "grade":                  [r"CLASSE.*ECHELON", r"CLASSE_ECHELON"],
        "statut_fonctionnaire":   [r"^GRADE$"],
        "poste":                  [r"^POSTE$", r"LIBELLE.*POSTE"],
        "prise_service":          [r"PRISE.*SERVICE", r"DATE.*PRISE.*SERVICE"],
        "date_retraite":          [r"DATE.*RETRAITE"],
        "age_retraite":           [r"AGE.*RETRAITE"],
        "mois_annee":             [r"MOIS.*ANNEE", r"^PERIODE$"],
    }
    correspondance = {}
    for nom in noms_colonnes:
        nom_norm = normaliser_nom_colonne(nom)
        matched  = None
        for std_name, patterns in mapping.items():
            for pattern in patterns:
                if re.search(pattern, nom_norm, re.IGNORECASE):
                    matched = std_name
                    break
            if matched:
                break
        correspondance[nom] = matched if matched else nom_norm.lower()
    return correspondance


def normaliser_situation(situation) -> str:
    if not situation or str(situation).strip() == "":
        return "autre"
    sit = str(situation).strip().upper()
    sit = unicodedata.normalize("NFKD", sit)
    sit = "".join(c for c in sit if not unicodedata.combining(c))
    sit = re.sub(r"\s+", " ", sit).strip()
    situations_valides = {
        "en_activite":      ["EN ACTIVITE", "ACTIVITE"],
        "regul_indemnites": ["REGUL. INDEMNITES", "REGUL INDEMNITES", "REGULARISATION INDEMNITES"],
        "demi_solde":       ["DEMI-SOLDE", "DEMI SOLDE", "DEMISOLDE", "1/2 SOLDE"],
    }
    for categorie, valeurs in situations_valides.items():
        if sit in valeurs:
            return categorie
    return "autre"


def charger_table_codes(
    tmp_anstat: str,
    sheet_name: str,
    libelle_col_names: list[str],
    col_jointure: str,
    cols_extra: list[str] | None = None,
) -> dict:
    dt = pl.read_excel(tmp_anstat, sheet_name=sheet_name)
    libelle_col = next(
        (col for col in dt.columns
         for pattern in libelle_col_names
         if pattern.upper() in col.upper()),
        None,
    )
    if not libelle_col:
        raise ValueError(
            f"Colonne libellé non trouvée dans l'onglet '{sheet_name}'.\n"
            f"  Cherché  : {libelle_col_names}\n"
            f"  Colonnes : {dt.columns}"
        )
    dt = dt.rename({libelle_col: "libelle"})
    dt = dt.with_columns(
        pl.col("libelle")
        .map_elements(normaliser_pour_matching, return_dtype=pl.Utf8)
        .alias("libelle_norm")
    )
    col_code       = dt.columns[0]
    cols_a_garder  = [col_code, "libelle", "libelle_norm"]
    if cols_extra:
        cols_a_garder += [c for c in cols_extra if c in dt.columns]
    return {"table": dt.select(cols_a_garder), "col_code": col_code, "col_jointure_source": col_jointure}


def appliquer_silver(df: pl.DataFrame, periode: str, dictionnaires_codes: dict) -> pl.DataFrame:
    """Applique toute la logique Silver sur un DataFrame Bronze d'une période."""
    correspondance = mapper_colonnes(list(df.columns))
    noms_std_vals  = list(correspondance.values())
    seen: dict     = {}
    new_corr: dict = {}
    for src, std in correspondance.items():
        if noms_std_vals.count(std) > 1:
            seen[std] = seen.get(std, 0) + 1
            if seen[std] > 1:
                std = f"{std}_dup{seen[std]}"
        new_corr[src] = std
    df = df.rename(new_corr)

    df = df.with_columns(pl.lit(periode).alias("mois_annee"))

    if "situation" in df.columns:
        df = df.with_columns([
            pl.col("situation").alias("situation_brute"),
            pl.col("situation")
            .map_elements(normaliser_situation, return_dtype=pl.Utf8)
            .alias("situation_normalisee"),
        ])
        df = df.filter(
            pl.col("situation_normalisee").is_in(
                ["en_activite", "regul_indemnites", "demi_solde"]
            )
        )

    if len(df) == 0:
        return df

    for nom_code, cfg in dictionnaires_codes.items():
        col_jointure = cfg["col_jointure_source"]
        table_codes  = cfg["table"]
        col_code     = cfg["col_code"]

        if col_jointure not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(nom_code))
            continue

        df = df.with_columns(
            pl.col(col_jointure)
            .map_elements(normaliser_pour_matching, return_dtype=pl.Utf8)
            .alias("_col_norm_tmp")
        )
        table_match = (
            table_codes
            .filter(pl.col("libelle_norm").is_not_null())
            .select(["libelle_norm", col_code])
            .unique(subset=["libelle_norm"])
            .rename({col_code: nom_code})
        )
        cols_extra_src = [c for c in table_codes.columns
                          if c not in (col_code, "libelle", "libelle_norm")]
        if cols_extra_src:
            table_extra = (
                table_codes
                .filter(pl.col("libelle_norm").is_not_null())
                .select(["libelle_norm"] + cols_extra_src)
                .unique(subset=["libelle_norm"])
            )
            table_match = table_match.join(table_extra, on="libelle_norm", how="left")

        df = (
            df.join(table_match, left_on="_col_norm_tmp", right_on="libelle_norm", how="left")
            .drop("_col_norm_tmp")
        )

    cols_num = ["montant_brut", "montant_net", "retenue_pension", "impot", "charge_patronale"]
    cols_codes_num = [c for c in df.columns if re.match(r"^[0-9]", c)]
    exprs_num = (
        [pl.col(c).cast(pl.Float64, strict=False) for c in cols_num if c in df.columns]
        + [pl.col(c).cast(pl.Float64, strict=False) for c in cols_codes_num]
    )
    if exprs_num:
        df = df.with_columns(exprs_num)

    comps_presentes = [c for c in COMPOSANTES_F2 if c in df.columns]
    if comps_presentes:
        df = df.with_columns(
            pl.sum_horizontal([pl.col(c).fill_null(0) for c in comps_presentes])
            .alias("salaire_brut_f2")
        )
    else:
        df = df.with_columns(pl.lit(0.0).alias("salaire_brut_f2"))

    return df

# ============================================================
# UTILITAIRE GOLD PANEL  (même logique que 06_compiler_panel)
# ============================================================

def construire_panel_annee(df: pl.DataFrame) -> pl.DataFrame:
    if "mois_annee" in df.columns:
        df = df.with_columns([
            pl.col("mois_annee").str.replace_all(r"[_/\.]", "-").alias("mois_annee"),
        ])
        df = df.with_columns([
            pl.when(pl.col("mois_annee").str.len_chars() == 6)
              .then(pl.col("mois_annee").str.slice(2, 4).cast(pl.Int32, strict=False))
              .otherwise(pl.col("mois_annee").str.slice(0, 4).cast(pl.Int32, strict=False))
              .alias("annee"),
            pl.when(pl.col("mois_annee").str.len_chars() == 6)
              .then(pl.col("mois_annee").str.slice(0, 2).cast(pl.Int32, strict=False))
              .otherwise(pl.col("mois_annee").str.slice(5, 2).cast(pl.Int32, strict=False))
              .alias("mois"),
        ])
    else:
        df = df.with_columns([
            pl.lit(None).cast(pl.Int32).alias("annee"),
            pl.lit(None).cast(pl.Int32).alias("mois"),
        ])

    cols_presentes = [c for c in COLONNES_PANEL if c in df.columns]
    df = df.select(cols_presentes)

    cles_dedup = [c for c in ["matricule", "mois_annee"] if c in df.columns]
    if cles_dedup:
        avant = len(df)
        df    = df.unique(subset=cles_dedup, keep="first", maintain_order=True)
        apres = len(df)
        if avant != apres:
            log(f"    {avant - apres:,} doublons supprimés")

    tri_cols = [c for c in ["matricule", "annee", "mois"] if c in df.columns]
    return df.sort(tri_cols, nulls_last=True)

# ============================================================
# DÉBUT DU SCRIPT
# ============================================================

separateur()
print("  PANEL ADMIN — MISE À JOUR INCRÉMENTALE")
print(f"  Démarrage : {_debut_run.strftime('%Y-%m-%d %H:%M:%S')}")
separateur()
print()

# ============================================================
# ÉTAPE 1 : DÉTECTER LES NOUVELLES PÉRIODES
# ============================================================

separateur("-")
log("ÉTAPE 1 : Détection des nouvelles périodes")
separateur("-")

# Périodes disponibles dans staging
objets_staging = lister_objets(BUCKET_STAGING, PREFIX_MENSUELS, ".xlsx")
periodes_staging = {
    os.path.splitext(os.path.basename(k))[0]: k
    for k in objets_staging
}
log(f"  Périodes dans staging    : {len(periodes_staging)}")

# Périodes déjà présentes en Bronze
cles_bronze = lister_objets(BUCKET_BRONZE, PREFIX_BRONZE, ".parquet")
periodes_bronze = {
    os.path.splitext(os.path.basename(k))[0]
    for k in cles_bronze
}
log(f"  Périodes dans Bronze     : {len(periodes_bronze)}")

# Nouvelles périodes = dans staging mais pas encore en Bronze
nouvelles_periodes = sorted(
    p for p in periodes_staging if p not in periodes_bronze
)
log(f"  Nouvelles périodes       : {len(nouvelles_periodes)}")

if not nouvelles_periodes:
    log("")
    log("  ✓ Bronze déjà à jour — aucune action nécessaire.")
    separateur()
    raise SystemExit(0)

for p in nouvelles_periodes:
    log(f"    • {p}")

# Années affectées par les nouvelles périodes
annees_affectees = sorted({
    re.search(r"20\d{2}", p).group()
    for p in nouvelles_periodes
    if re.search(r"20\d{2}", p)
})
log(f"\n  Années Silver affectées  : {annees_affectees}")

# ============================================================
# ÉTAPE 2 : CHARGEMENT TABLES ANSTAT
# ============================================================

separateur("-")
log("ÉTAPE 2 : Chargement tables ANSTAT")
separateur("-")

objets_refs = lister_objets(BUCKET_STAGING, PREFIX_REFS)
fichier_anstat_key = next(
    (k for k in objets_refs if "ANSTAT_CODE" in k.upper()),
    None,
)
if fichier_anstat_key is None:
    raise FileNotFoundError(
        f"Fichier ANSTAT introuvable dans s3://{BUCKET_STAGING}/{PREFIX_REFS}/"
    )

with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
    tmp_anstat = tmp.name
s3.download_file(BUCKET_STAGING, fichier_anstat_key, tmp_anstat)
log(f"  ✓ ANSTAT téléchargé : {os.path.basename(fichier_anstat_key)}")

dictionnaires_codes = {
    "CODE_AFFECTATION":    charger_table_codes(
                             tmp_anstat, "lieu affectation",
                             ["LIBELLÉ_LIEU_AFFECTATION", "LIBELLE_LIEU_AFFECTATION"],
                             "lieu_affectation",
                             cols_extra=["CODE_RÉGION", "CODE_SITE", "CODE_DRB",
                                         "CODE_DÉPARTEMENT", "MONTANT TRANSPORT"],
                           ),
    "CODE_ORGANISME":      charger_table_codes(
                             tmp_anstat, "ORGANISME_OK",
                             ["LIBELLÉ_ORGANISME", "LIBELLE_ORGANISME"],
                             "organisme",
                             cols_extra=["CODE_ÉTABLISSEMENT", "CODE_ACCT"],
                           ),
    "CODE_POSITION_SOLDE": charger_table_codes(
                             tmp_anstat, "CODE_SITUATION_SOLDE",
                             ["LIBELLÉ_POSITION_SOLDE", "LIBELLE_SITUATION_SOLDE",
                              "LIBELLÉ_SITUATION_SOLDE"],
                             "situation",
                           ),
    "CODE_EMPLOI":         charger_table_codes(
                             tmp_anstat, "HISTORIQUE_ECHELLES_CORPS",
                             ["LIBELLÉ_EMPLOI", "LIBELLE_EMPLOI"],
                             "emploi",
                             cols_extra=["GRADE_ADMINISTRATIF_ASSOCIE",
                                         "Code_CITP", "Metier_CITP"],
                           ),
    "CODE_SERVICE":        charger_table_codes(
                             tmp_anstat, "SERVICE",
                             ["LIBELLÉ_SERVICE", "LIBELLE_SERVICE"],
                             "service",
                           ),
    "CODE_FONCTION":       charger_table_codes(
                             tmp_anstat, "FONCTION",
                             ["LIBELLÉ_FONCTION", "LIBELLE_FONCTION"],
                             "fonction",
                           ),
    "CODE_GRADE":          charger_table_codes(
                             tmp_anstat, "GRADE",
                             ["LIBELLÉ_GRADE", "LIBELLE_GRADE"],
                             "grade",
                           ),
    "CODE_POSTE":          charger_table_codes(
                             tmp_anstat, "LIBELLE POSTE",
                             ["LIBELLÉ_POSTE", "LIBELLE_POSTE"],
                             "poste",
                           ),
}

for nom, cfg in dictionnaires_codes.items():
    log(f"  {nom:<25} : {len(cfg['table']):>4} codes")

os.unlink(tmp_anstat)

# ============================================================
# ÉTAPE 3 : NOUVELLES PÉRIODES → BRONZE
# ============================================================

separateur("-")
log("ÉTAPE 3 : Traitement nouvelles périodes → Bronze")
separateur("-")

nb_bronze_ok  = 0
nb_bronze_err = 0

for periode in nouvelles_periodes:
    chemin_objet = periodes_staging[periode]
    log(f"  [{periode}] ... ", )

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name

        df_bronze = traiter_xlsx_vers_bronze(chemin_objet, periode, tmp_path)

        if df_bronze is not None:
            key_bronze = f"{PREFIX_BRONZE}/{periode}.parquet"
            ecrire_parquet_s3(df_bronze, BUCKET_BRONZE, key_bronze)
            nb_bronze_ok += 1

    except Exception as e:
        log(f"    ✗ ERREUR : {e}")
        nb_bronze_err += 1
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

    gc.collect()

log(f"\n  Bronze : {nb_bronze_ok} ok  |  {nb_bronze_err} erreurs")

if nb_bronze_ok == 0:
    log("  ✗ Aucune période ingérée en Bronze — arrêt.")
    raise SystemExit(1)

# ============================================================
# ÉTAPE 4 : SILVER — RECONSTRUCTION DES ANNÉES AFFECTÉES
# ============================================================

separateur("-")
log("ÉTAPE 4 : Reconstruction Silver pour les années affectées")
separateur("-")

nb_silver_ok  = 0
nb_silver_err = 0

for annee in annees_affectees:
    log(f"\n  Année {annee} :")

    # Tous les fichiers Bronze de cette année (anciens + nouveaux)
    cles_bronze_annee = [
        k for k in lister_objets(BUCKET_BRONZE, PREFIX_BRONZE, ".parquet")
        if annee in os.path.basename(k)
    ]
    log(f"    {len(cles_bronze_annee)} fichiers Bronze disponibles pour {annee}")

    frames: list[pl.DataFrame] = []

    for cle in sorted(cles_bronze_annee):
        periode = os.path.splitext(os.path.basename(cle))[0]
        try:
            df_b = lire_parquet_s3(BUCKET_BRONZE, cle)
            df_s = appliquer_silver(df_b, periode, dictionnaires_codes)

            if len(df_s) > 0:
                taux_org = (
                    100 * df_s["CODE_ORGANISME"].is_not_null().sum() / len(df_s)
                    if "CODE_ORGANISME" in df_s.columns else 0.0
                )
                log(f"    [{periode}] {len(df_s):,} lignes  |  CODE_ORGANISME : {taux_org:.1f}%")
                frames.append(df_s)

        except Exception as e:
            log(f"    [{periode}] ✗ ERREUR : {e}")
        gc.collect()

    if not frames:
        log(f"    ✗ Aucun mois Silver produit pour {annee}")
        nb_silver_err += 1
        continue

    try:
        df_annee = pl.concat(frames, how="diagonal")
        key_silver = f"{PREFIX_SILVER}/{annee}.parquet"
        ecrire_parquet_s3(df_annee, BUCKET_SILVER, key_silver)
        nb_silver_ok += 1
    except Exception as e:
        log(f"    ✗ ERREUR écriture Silver {annee} : {e}")
        nb_silver_err += 1

    del frames, df_annee
    gc.collect()

log(f"\n  Silver : {nb_silver_ok} années ok  |  {nb_silver_err} erreurs")

# ============================================================
# ÉTAPE 5 : BACKUP panel_complet + RECONSTRUCTION GOLD PANEL
# ============================================================

separateur("-")
log("ÉTAPE 5 : Backup panel_complet + Reconstruction Gold Panel")
separateur("-")

# --- Backup ---
if objet_existe(BUCKET_GOLD, KEY_PANEL_COMPLET):
    ts         = datetime.now().strftime("%Y%m%d_%H%M%S")
    key_backup = f"{PREFIX_BACKUP}/panel_complet_backup_{ts}.parquet"
    s3.copy_object(
        CopySource={"Bucket": BUCKET_GOLD, "Key": KEY_PANEL_COMPLET},
        Bucket=BUCKET_GOLD,
        Key=key_backup,
    )
    log(f"  ✓ Backup créé : s3://{BUCKET_GOLD}/{key_backup}")
else:
    log("  ℹ️  panel_complet absent — pas de backup")

# --- Reconstruction des panels Gold pour les années affectées ---
nb_panel_ok  = 0
nb_panel_err = 0

for annee in annees_affectees:
    key_silver = f"{PREFIX_SILVER}/{annee}.parquet"
    if not objet_existe(BUCKET_SILVER, key_silver):
        log(f"  [{annee}] ✗ Silver introuvable, panel ignoré")
        nb_panel_err += 1
        continue

    try:
        log(f"  [{annee}] Lecture Silver...", )
        df_s = lire_parquet_s3(BUCKET_SILVER, key_silver)
        df_p = construire_panel_annee(df_s)
        del df_s
        gc.collect()

        key_panel = f"{PREFIX_PANEL}/panel_{annee}.parquet"
        ecrire_parquet_s3(df_p, BUCKET_GOLD, key_panel)
        nb_panel_ok += 1
    except Exception as e:
        log(f"  [{annee}] ✗ ERREUR : {e}")
        nb_panel_err += 1
    gc.collect()

log(f"\n  Panel Gold : {nb_panel_ok} années ok  |  {nb_panel_err} erreurs")

# ============================================================
# ÉTAPE 6 : REBUILD panel_complet (streaming, toutes années)
# ============================================================

separateur("-")
log("ÉTAPE 6 : Rebuild panel_complet.parquet (toutes années)")
separateur("-")

cles_panel_annuels = sorted([
    k for k in lister_objets(BUCKET_GOLD, PREFIX_PANEL, ".parquet")
    if re.search(r"panel_20\d{2}\.parquet$", k)
])
log(f"  Fichiers panel annuels trouvés : {len(cles_panel_annuels)}")

with tempfile.TemporaryDirectory() as tmpdir:
    chemins_locaux = []
    for cle in cles_panel_annuels:
        dest = os.path.join(tmpdir, os.path.basename(cle))
        s3.download_file(BUCKET_GOLD, cle, dest)
        chemins_locaux.append(dest)

    chemin_complet = os.path.join(tmpdir, "panel_complet.parquet")
    log("  Fusion en streaming...")
    (
        pl.scan_parquet(chemins_locaux)
        .sort(["matricule", "annee", "mois"], nulls_last=True)
        .sink_parquet(chemin_complet, compression="snappy")
    )

    taille = os.path.getsize(chemin_complet) / 1024**2
    log(f"  Upload panel_complet ({taille:.1f} MB)...")
    s3.upload_file(chemin_complet, BUCKET_GOLD, KEY_PANEL_COMPLET)
    log(f"  ✓ s3://{BUCKET_GOLD}/{KEY_PANEL_COMPLET}")

# ============================================================
# RAPPORT FINAL
# ============================================================

duree = (datetime.now() - _debut_run).total_seconds()

separateur()
print("  RAPPORT FINAL")
separateur()
print(f"  Nouvelles périodes détectées  : {len(nouvelles_periodes)}")
print(f"  Périodes ingérées en Bronze   : {nb_bronze_ok}  ({nb_bronze_err} erreurs)")
print(f"  Années Silver reconstruites   : {nb_silver_ok}  ({nb_silver_err} erreurs)")
print(f"  Panels Gold reconstruits      : {nb_panel_ok}   ({nb_panel_err} erreurs)")
print(f"  panel_complet                 : ✓ mis à jour")
print()
print(f"  Durée totale : {int(duree // 60)}m {int(duree % 60)}s")
separateur()
print()
print("  NOTA : Les imputations grades (03b) et salaires (03c)")
print("  n'ont PAS été relancées sur les nouvelles périodes.")
print("  Si nécessaire, exécutez :")
print("    python 03b_imputer_grades_silver.py")
print("    python 03c_imputer_salaires_silver.py")
print("    python 04_mise_a_jour_incrementale.py")
separateur()
