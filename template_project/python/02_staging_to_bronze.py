# ============================================================
# PANEL ADMIN — ÉTAPE 2 : STAGING → BRONZE ICEBERG
# ============================================================
# Lit chaque fichier Excel mensuel depuis MinIO staging,
# applique le mapping minimal de colonnes (normalisation des
# noms uniquement, sans transformation métier),
# et ingère vers la table Bronze Iceberg en mode batch.
#
# Bronze = données brutes, tout en String, toutes périodes
# empilées. Les types et règles métier vont en Silver (étape 3).
#
# Table produite : nessie.bronze.panel_admin_solde_mensuel
#
# Architecture :
#   pandas+calamine → lecture Excel (calamine = moteur Rust, ~5× plus rapide qu'openpyxl)
#                     fallback openpyxl si calamine non installé
#   Polars          → transformations, concat, cast
#   PySpark + Arrow → écriture Iceberg (Arrow évite la sérialisation Row-by-Row)
#
# Parallélisme :
#   ThreadPoolExecutor → téléchargements et lectures Excel en parallèle (I/O bound)
#
# Dépendances :
#   pip install polars pandas python-calamine openpyxl boto3 python-dotenv pyspark
# ============================================================

import os
import re
import tempfile
import unicodedata
from math import ceil
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import pandas as pd
import polars as pl
from pyspark.sql import SparkSession

# Moteur Excel : calamine (Rust, rapide) avec fallback openpyxl
try:
    import python_calamine  # noqa: F401
    EXCEL_ENGINE = "calamine"
except ImportError:
    EXCEL_ENGINE = "openpyxl"

load_dotenv(".env")

# --- CONFIGURATION ---

# Endpoint selon environnement : configurer dans .env (local ou JHub)
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
NESSIE_URI       = os.getenv("NESSIE_URI")

BUCKET          = "staging"
PREFIX_MENSUELS = "panel_admin/fichiers_mensuels"
TABLE_BRONZE    = "nessie.bronze.panel_admin_solde_mensuel"
TAILLE_LOT      = 12   # 12 mois traités puis écrits en batch
NB_WORKERS      = 4    # téléchargements/lectures Excel en parallèle

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
# FONCTIONS UTILITAIRES
# ============================================================

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


def detecter_entete(fichier, sheet_id):
    """Détecte la ligne d'en-tête contenant 'matricule' (0-indexed)."""
    try:
        preview = pd.read_excel(
            fichier, sheet_name=sheet_id, header=None,
            nrows=20, engine=EXCEL_ENGINE,
        )
        if len(preview) > 1:
            preview = preview.iloc[:-1]
        for i, row in preview.iterrows():
            if any("matricule" in str(v).lower() for v in row):
                return i
        return 1 if sheet_id == 1 else 0
    except Exception:
        return 1 if sheet_id == 1 else 0


def lire_feuille(fichier, sheet_id, skiprows):
    """Lit une feuille Excel → Polars DataFrame (tout en string)."""
    return pl.from_pandas(
        pd.read_excel(
            fichier, sheet_name=sheet_id,
            skiprows=skiprows, dtype=str,
            engine=EXCEL_ENGINE,
        )
    )


# ============================================================
# CONNEXION SPARK (pour l'écriture Iceberg uniquement)
# ============================================================

spark = (
    SparkSession.builder
    .appName("panel_admin_staging_to_bronze")
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
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .getOrCreate()
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

# ============================================================
# LISTER LES FICHIERS DANS STAGING
# ============================================================

print("=" * 70)
print("STAGING → BRONZE : Panel Administratif")
print("=" * 70)
print()

paginator = s3.get_paginator("list_objects_v2")
pages = paginator.paginate(Bucket=BUCKET, Prefix=PREFIX_MENSUELS)

fichiers_minio = sorted([
    obj["Key"]
    for page in pages
    for obj in page.get("Contents", [])
    if obj["Key"].lower().endswith(".xlsx")
])

nb_lots = ceil(len(fichiers_minio) / TAILLE_LOT)
lots = [fichiers_minio[i:i + TAILLE_LOT] for i in range(0, len(fichiers_minio), TAILLE_LOT)]

print(f"Fichiers dans staging : {len(fichiers_minio)}")
print(f"Taille des lots       : {TAILLE_LOT} fichiers")
print(f"Nombre de lots        : {nb_lots}\n")

# ============================================================
# TRAITEMENT PAR LOTS  (Polars + parallélisme)
# ============================================================

def traiter_fichier(chemin_objet):
    """
    Télécharge et traite un fichier Excel.
    Retourne (periode, DataFrame) ou (periode, None) en cas d'erreur.
    Conçu pour être appelé en parallèle via ThreadPoolExecutor.
    """
    periode     = os.path.splitext(os.path.basename(chemin_objet))[0]
    nom_fichier = os.path.basename(chemin_objet)
    tmp_path    = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name
        s3.download_file(BUCKET, chemin_objet, tmp_path)

        # --- LECTURE FEUILLE 1 (données agents) ---
        skip_f1  = detecter_entete(tmp_path, sheet_id=0)
        feuille1 = lire_feuille(tmp_path, sheet_id=0, skiprows=skip_f1)

        # Normalisation des noms de colonnes
        noms = [normaliser_nom_colonne(c) for c in feuille1.columns]
        seen = {}
        noms_uniques = []
        for n in noms:
            if n in seen:
                seen[n] += 1
                noms_uniques.append(f"{n}_{seen[n]}")
            else:
                seen[n] = 1
                noms_uniques.append(n)
        feuille1 = feuille1.rename(dict(zip(feuille1.columns, noms_uniques)))

        # --- LECTURE FEUILLE 2 (codes numériques) ---
        skip_f2  = detecter_entete(tmp_path, sheet_id=1)
        feuille2 = lire_feuille(tmp_path, sheet_id=1, skiprows=skip_f2)

        # Trouver la clé de jointure F2
        nom_cle_f2 = next(
            (c for c in feuille2.columns
             if re.search(r"MATRICULE.*\|\|", str(c), re.IGNORECASE)),
            None,
        )

        if nom_cle_f2:
            feuille2 = feuille2.rename({nom_cle_f2: "CLE_UNIQUE_F2"})
            autres_cols_f2 = [c for c in feuille2.columns if c != "CLE_UNIQUE_F2"]
            rename_f2 = {c: normaliser_nom_colonne(c) + "_F2" for c in autres_cols_f2}
            feuille2 = feuille2.rename(rename_f2)

            cle_f1 = "CLE_UNIQUE" if "CLE_UNIQUE" in feuille1.columns else "MATRICULE"
            if cle_f1 in feuille1.columns:
                feuille1 = feuille1.join(
                    feuille2, left_on=cle_f1, right_on="CLE_UNIQUE_F2", how="left",
                )

        # Métadonnées
        feuille1 = feuille1.with_columns([
            pl.lit(periode).alias("PERIODE"),
            pl.lit(nom_fichier).alias("FICHIER_SOURCE"),
        ])

        return periode, feuille1

    except Exception as e:
        return periode, e
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


premier_lot = True

for i_lot, fichiers_lot in enumerate(lots, start=1):
    print(f"[LOT {i_lot}/{nb_lots}] {len(fichiers_lot)} fichiers  "
          f"[moteur Excel : {EXCEL_ENGINE}]")
    print("-" * 70)

    resultats_lot = []

    # Téléchargements et lectures en parallèle
    with ThreadPoolExecutor(max_workers=NB_WORKERS) as executor:
        futures = {executor.submit(traiter_fichier, f): f for f in fichiers_lot}
        for future in as_completed(futures):
            periode, result = future.result()
            if isinstance(result, Exception):
                print(f"  [{periode}] ✗ ERREUR : {result}")
            else:
                resultats_lot.append(result)
                print(f"  [{periode}] ✓ {len(result):,} lignes × {len(result.columns)} cols")

    # --- CONSOLIDATION DU LOT (Polars) ---
    if not resultats_lot:
        continue

    print(f"\n  Consolidation lot {i_lot}...")

    # diagonal : aligne par nom de colonne, remplit les colonnes manquantes avec null
    base_lot = pl.concat(resultats_lot, how="diagonal")

    # Tout en String pour Bronze (les null restent null)
    base_lot = base_lot.with_columns([
        pl.col(c).cast(pl.Utf8) for c in base_lot.columns
    ])

    print(f"  ✓ Lot {i_lot} : {len(base_lot):,} lignes × {len(base_lot.columns)} colonnes")

    # --- ÉCRITURE VERS BRONZE ICEBERG (via Spark) ---
    print(f"  Écriture vers {TABLE_BRONZE}... ", end="", flush=True)

    df_spark = spark.createDataFrame(base_lot.to_pandas())
    mode = "overwrite" if premier_lot else "append"
    df_spark.writeTo(TABLE_BRONZE).using("iceberg").mode(mode).createOrReplace()

    print("✓\n")
    premier_lot = False

# ============================================================
# RÉSUMÉ FINAL
# ============================================================

print("=" * 70)
print("RÉSUMÉ")
print("=" * 70)
print()

nb_total = spark.sql(f"SELECT COUNT(*) AS n FROM {TABLE_BRONZE}").collect()[0]["n"]
print(f"Table Bronze : {nb_total:,} lignes")
print(f"Table        : {TABLE_BRONZE}")
print("\n✓ INGESTION BRONZE TERMINÉE")
print("Prochaine étape : exécuter 03_bronze_to_silver.py")

spark.stop()
