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
# Dépendances :
#   pip install pyspark boto3 openpyxl pandas python-dotenv unicodedata2
# ============================================================

import os
import re
import tempfile
import unicodedata
from math import ceil
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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

BUCKET          = "staging"
PREFIX_MENSUELS = "panel_admin/fichiers_mensuels"
TABLE_BRONZE    = "nessie.bronze.panel_admin_solde_mensuel"
TAILLE_LOT      = 12   # 12 mois traités puis écrits en batch

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


def detecter_entete(fichier, sheet):
    """Détecte la ligne d'en-tête contenant 'matricule'."""
    try:
        preview = pd.read_excel(fichier, sheet_name=sheet, header=None, nrows=20)
        if len(preview) > 1:
            preview = preview.iloc[:-1]
        for i, row in preview.iterrows():
            ligne_lower = [str(v).lower().strip() for v in row]
            if any("matricule" in v for v in ligne_lower):
                return i  # 0-indexed
        return 1 if sheet == 1 else 0
    except Exception:
        return 1 if sheet == 1 else 0


# ============================================================
# CONNEXION SPARK
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
# TRAITEMENT PAR LOTS
# ============================================================

premier_lot = True

for i_lot, fichiers_lot in enumerate(lots, start=1):
    print(f"[LOT {i_lot}/{nb_lots}] {len(fichiers_lot)} fichiers")
    print("-" * 70)

    resultats_lot = {}

    for chemin_objet in fichiers_lot:
        periode     = os.path.splitext(os.path.basename(chemin_objet))[0]
        nom_fichier = os.path.basename(chemin_objet)
        print(f"  [{periode}] Téléchargement... ", end="", flush=True)

        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
                tmp_path = tmp.name
            s3.download_file(BUCKET, chemin_objet, tmp_path)

            # --- LECTURE FEUILLE 1 (données agents) ---
            skip_f1  = detecter_entete(tmp_path, sheet=0)
            feuille1 = pd.read_excel(tmp_path, sheet_name=0,
                                     skiprows=skip_f1, dtype=str)

            # Normalisation des noms de colonnes
            noms = [normaliser_nom_colonne(c) for c in feuille1.columns]
            # Gérer les doublons
            seen = {}
            for idx, n in enumerate(noms):
                if n in seen:
                    seen[n] += 1
                    noms[idx] = f"{n}_{seen[n]}"
                else:
                    seen[n] = 1
            feuille1.columns = noms

            # --- LECTURE FEUILLE 2 (codes numériques) ---
            skip_f2  = detecter_entete(tmp_path, sheet=1)
            feuille2 = pd.read_excel(tmp_path, sheet_name=1,
                                     skiprows=skip_f2, dtype=str)

            # Trouver la clé de jointure F2
            nom_cle_f2 = next(
                (c for c in feuille2.columns
                 if re.search(r"MATRICULE.*\|\|", str(c), re.IGNORECASE)),
                None
            )

            if nom_cle_f2:
                feuille2 = feuille2.rename(columns={nom_cle_f2: "CLE_UNIQUE_F2"})
                autres_cols_f2 = [c for c in feuille2.columns if c != "CLE_UNIQUE_F2"]
                rename_f2 = {c: normaliser_nom_colonne(c) + "_F2" for c in autres_cols_f2}
                feuille2 = feuille2.rename(columns=rename_f2)

                cle_f1 = "CLE_UNIQUE" if "CLE_UNIQUE" in feuille1.columns else "MATRICULE"
                if cle_f1 in feuille1.columns:
                    feuille1 = feuille1.merge(
                        feuille2, left_on=cle_f1, right_on="CLE_UNIQUE_F2", how="left"
                    )

            # Métadonnées
            feuille1["PERIODE"]        = periode
            feuille1["FICHIER_SOURCE"] = nom_fichier

            resultats_lot[periode] = feuille1
            os.unlink(tmp_path)

            print(f"✓ {len(feuille1):,} lignes × {len(feuille1.columns)} cols")

        except Exception as e:
            print(f"✗ ERREUR : {e}")
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)

    # --- CONSOLIDATION DU LOT ---
    if not resultats_lot:
        continue

    print(f"\n  Consolidation lot {i_lot}...")

    base_lot = pd.concat(resultats_lot.values(), axis=0, ignore_index=True)

    # Tout en string pour Bronze
    base_lot = base_lot.astype(str).replace("nan", None)

    print(f"  ✓ Lot {i_lot} : {len(base_lot):,} lignes × {len(base_lot.columns)} colonnes")

    # --- ÉCRITURE VERS BRONZE ICEBERG ---
    print(f"  Écriture vers {TABLE_BRONZE}... ", end="", flush=True)

    df_spark = spark.createDataFrame(base_lot)
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
