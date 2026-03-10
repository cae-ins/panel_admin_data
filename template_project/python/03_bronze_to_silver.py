# ============================================================
# PANEL ADMIN — ÉTAPE 3 : BRONZE → SILVER ICEBERG
# ============================================================
# Lit la table Bronze, applique toute la logique métier :
#   - Mapping des colonnes brutes → noms standardisés (mapper_colonnes)
#   - Normalisation et filtrage des situations administratives
#   - Enrichissement avec les codes ANSTAT (organisme, grade, emploi...)
#   - Cast des montants en numérique
#
# Table source   : nessie.bronze.panel_admin_solde_mensuel
# Table produite : nessie.silver.panel_admin_solde_mensuel
#
# Architecture :
#   pandas+openpyxl → lecture Excel (fiable sur toutes les versions)
#   Polars          → toutes les transformations métier
#   PySpark         → lecture/écriture Iceberg (catalogue Nessie)
#
# Dépendances :
#   pip install polars pandas openpyxl boto3 python-dotenv pyspark
# ============================================================

import os
import re
import tempfile
import unicodedata
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import pandas as pd
import polars as pl
from pyspark.sql import SparkSession

load_dotenv(".env")

# --- CONFIGURATION ---

# LORSQU'ON TRAVAILLE DEPUIS SA MACHINE LOCAL
MINIO_ENDPOINT   = "http://192.168.1.230:30137"
MINIO_ACCESS_KEY = "datalab-team"
MINIO_SECRET_KEY = "minio-datalabteam123"
NESSIE_URI       = "http://192.168.1.230:30604/api/v1"

# LORSQU'ON TRAVAILLE SUR JHUB
# MINIO_ENDPOINT   = "http://minio.mon-namespace.svc.cluster.local:80"
# MINIO_ACCESS_KEY = "datalab-team"
# MINIO_SECRET_KEY = "minio-datalabteam123"
# NESSIE_URI       = "http://nessie.trino.svc.cluster.local:19120/api/v1"

BUCKET        = "staging"
PREFIX_REFS   = "panel_admin/references"
TABLE_BRONZE  = "nessie.bronze.panel_admin_solde_mensuel"
TABLE_SILVER  = "nessie.silver.panel_admin_solde_mensuel"

# --- CLIENT S3 (MinIO) ---
s3 = boto3.client(
    "s3",
    endpoint_url          = MINIO_ENDPOINT,
    aws_access_key_id     = MINIO_ACCESS_KEY,
    aws_secret_access_key = MINIO_SECRET_KEY,
    config                = Config(signature_version="s3v4"),
    region_name           = "us-east-1",
    verify                = False,
)

# ============================================================
# FONCTIONS MÉTIER
# ============================================================

def normaliser_pour_matching(texte):
    """Normalise un texte pour la jointure : ASCII majuscule, espaces simples."""
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


def normaliser_nom_colonne(nom):
    """Normalise un nom de colonne : ASCII majuscule, sans caractères spéciaux."""
    if not nom or (isinstance(nom, float)):
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


def mapper_colonnes(noms_colonnes):
    """
    Mappe les noms bruts vers les noms standardisés.
    Retourne un dict {nom_source: nom_standard}.
    """
    mapping = {
        "cle_unique":             [r"MATRICULE.*CODE.*ORGANISME", r"MATRICULE.*\|\|.*CODE"],
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
        matched = None
        for std_name, patterns in mapping.items():
            for pattern in patterns:
                if re.search(pattern, nom_norm, re.IGNORECASE):
                    matched = std_name
                    break
            if matched:
                break
        correspondance[nom] = matched if matched else nom_norm.lower()

    return correspondance


def normaliser_situation(situation):
    """Normalise et catégorise les situations administratives."""
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


# ============================================================
# CHARGEMENT DES TABLES DE CODES ANSTAT (depuis staging)
# ============================================================

print("=" * 70)
print("CHARGEMENT : Tables de codes ANSTAT (depuis staging)")
print("=" * 70)
print()

paginator = s3.get_paginator("list_objects_v2")
pages = paginator.paginate(Bucket=BUCKET, Prefix=PREFIX_REFS)
fichier_anstat_key = next(
    (obj["Key"]
     for page in pages
     for obj in page.get("Contents", [])
     if "ANSTAT_CODE" in obj["Key"].upper()),
    None,
)

with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
    tmp_anstat = tmp.name
s3.download_file(BUCKET, fichier_anstat_key, tmp_anstat)
print(f"✓ Fichier ANSTAT téléchargé : {os.path.basename(fichier_anstat_key)}\n")


def charger_table_codes(sheet_name, libelle_col_names, col_jointure):
    dt = pl.from_pandas(pd.read_excel(tmp_anstat, sheet_name=sheet_name, dtype=str))
    libelle_col = next((c for c in libelle_col_names if c in dt.columns), None)
    if not libelle_col:
        raise ValueError(f"Colonne libellé non trouvée dans {sheet_name}")
    dt = dt.rename({libelle_col: "libelle"})
    dt = dt.with_columns(
        pl.col("libelle")
        .map_elements(normaliser_pour_matching, return_dtype=pl.Utf8)
        .alias("libelle_norm")
    )
    col_code = dt.columns[0]
    return {"table": dt, "col_code": col_code, "col_jointure_source": col_jointure}


dictionnaires_codes = {
    "CODE_AFFECTATION":    charger_table_codes("lieu affectation",
                             ["LIBELLÉ_LIEU_AFFECTATION", "LIBELLE_LIEU_AFFECTATION"],
                             "lieu_affectation"),
    "CODE_ORGANISME":      charger_table_codes("ORGANISME_OK",
                             ["LIBELLÉ_ORGANISME", "LIBELLE_ORGANISME"],
                             "organisme"),
    "CODE_POSITION_SOLDE": charger_table_codes("CODE_SITUATION_SOLDE",
                             ["LIBELLÉ_POSITION_SOLDE", "LIBELLE_SITUATION_SOLDE",
                              "LIBELLÉ_SITUATION_SOLDE"],
                             "situation"),
    "CODE_EMPLOI":         charger_table_codes("HISTORIQUE_ECHELLES_CORPS",
                             ["LIBELLÉ_EMPLOI", "LIBELLE_EMPLOI"],
                             "emploi"),
    "CODE_SERVICE":        charger_table_codes("SERVICE",
                             ["LIBELLÉ_SERVICE", "LIBELLE_SERVICE"],
                             "service"),
    "CODE_FONCTION":       charger_table_codes("FONCTION",
                             ["LIBELLÉ_FONCTION", "LIBELLE_FONCTION"],
                             "fonction"),
    "CODE_GRADE":          charger_table_codes("GRADE",
                             ["LIBELLÉ_GRADE", "LIBELLE_GRADE"],
                             "grade"),
    "CODE_POSTE":          charger_table_codes("LIBELLE POSTE",
                             ["LIBELLÉ_POSTE", "LIBELLE_POSTE"],
                             "poste"),
}

for nom_code, cfg in dictionnaires_codes.items():
    print(f"  {nom_code:<25} : {len(cfg['table'])} codes")

os.unlink(tmp_anstat)

# ============================================================
# CONNEXION SPARK (lecture Bronze + écriture Silver)
# ============================================================

spark = (
    SparkSession.builder
    .appName("panel_admin_bronze_to_silver")
    .config("spark.driver.memory", "16g")
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
    .config("spark.sql.catalog.nessie",              "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.nessie.uri",          NESSIE_URI)
    .config("spark.sql.catalog.nessie.ref",          "main")
    .config("spark.sql.catalog.nessie.warehouse",    "s3a://bronze/")
    .config("spark.hadoop.fs.s3a.endpoint",                   MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",                 MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",                 MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access",          "true")
    .config("spark.hadoop.fs.s3a.impl",                       "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)

# ============================================================
# LECTURE BRONZE → CONVERSION POLARS
# ============================================================

print()
print("=" * 70)
print("TRANSFORMATION : Bronze → Silver")
print("=" * 70)
print()

print("Lecture de la table Bronze...")
df = pl.from_pandas(spark.table(TABLE_BRONZE).toPandas())
print(f"✓ Bronze : {len(df):,} lignes × {len(df.columns)} colonnes\n")

# --- MAPPING DES COLONNES (Polars) ---
print("Application de mapper_colonnes()...")
correspondance = mapper_colonnes(list(df.columns))

# Gérer les doublons dans les noms standards
noms_std_vals = list(correspondance.values())
seen = {}
new_correspondance = {}
for src, std in correspondance.items():
    if noms_std_vals.count(std) > 1:
        seen[std] = seen.get(std, 0) + 1
        if seen[std] > 1:
            std = f"{std}_dup{seen[std]}"
    new_correspondance[src] = std
correspondance = new_correspondance

df = df.rename(correspondance)
print(f"✓ {len(correspondance)} colonnes mappées\n")

# --- NORMALISATION DES SITUATIONS (Polars) ---
print("Normalisation des situations administratives...")

if "situation" in df.columns:
    df = df.with_columns([
        pl.col("situation").alias("situation_brute"),
        pl.col("situation")
        .map_elements(normaliser_situation, return_dtype=pl.Utf8)
        .alias("situation_normalisee"),
    ])

    avant = len(df)
    df = df.filter(
        pl.col("situation_normalisee").is_in(["en_activite", "regul_indemnites", "demi_solde"])
    )
    apres = len(df)
    pct = 100 * apres / avant if avant > 0 else 0
    print(f"✓ Filtrage : {avant:,} → {apres:,} lignes ({pct:.1f}% conservé)\n")

# --- ENRICHISSEMENT PAR CODES ANSTAT (Polars) ---
print("Enrichissement avec les codes ANSTAT...")

for nom_code, cfg in dictionnaires_codes.items():
    col_jointure = cfg["col_jointure_source"]
    table_codes  = cfg["table"]
    col_code     = cfg["col_code"]

    if col_jointure not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(nom_code))
        print(f"  {nom_code:<25} : colonne source absente")
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

    df = (
        df.join(table_match, left_on="_col_norm_tmp", right_on="libelle_norm", how="left")
        .drop("_col_norm_tmp")
    )

    taux = 100 * df[nom_code].is_not_null().sum() / len(df)
    print(f"  {nom_code:<25} : {taux:.1f}% matchés")

print()

# --- CAST DES TYPES NUMÉRIQUES (Polars) ---
print("Cast des montants en numérique...")

cols_numeriques = ["montant_brut", "montant_net", "retenue_pension", "impot", "charge_patronale"]
exprs_num = [
    pl.col(c).cast(pl.Float64, strict=False)
    for c in cols_numeriques
    if c in df.columns
]

cols_codes_num = [c for c in df.columns if re.match(r"^[0-9]", c)]
exprs_num += [pl.col(c).cast(pl.Float64, strict=False) for c in cols_codes_num]

if exprs_num:
    df = df.with_columns(exprs_num)

nb_num = len([c for c in cols_numeriques if c in df.columns]) + len(cols_codes_num)
print(f"✓ {nb_num} colonnes numériques castées\n")

# ============================================================
# ÉCRITURE EN SILVER (via Spark)
# ============================================================

print("=" * 70)
print(f"ÉCRITURE : {TABLE_SILVER}")
print("=" * 70)
print()

print(f"Silver : {len(df):,} lignes × {len(df.columns)} colonnes")

df_silver_spark = spark.createDataFrame(df.to_pandas())
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
df_silver_spark.writeTo(TABLE_SILVER).using("iceberg").mode("overwrite").createOrReplace()

print(f"\n✓ TABLE SILVER ÉCRITE : {TABLE_SILVER}")
print("Prochaine étape : exécuter 04_validation_silver.py")

spark.stop()
