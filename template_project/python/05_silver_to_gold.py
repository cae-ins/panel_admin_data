# ============================================================
# PANEL ADMIN — ÉTAPE 5 : SILVER → GOLD ICEBERG
# ============================================================
# Produit les tables Gold prêtes pour les tableaux de bord
# et les exports finaux à partir de la table Silver.
#
# Tables produites :
#   nessie.gold.panel_admin_masse_salariale
#     → Masse salariale brute et nette par période × organisme
#
#   nessie.gold.panel_admin_effectifs
#     → Effectifs, agents uniques, distribution par période
#
# Architecture :
#   PySpark → lecture/écriture Iceberg (catalogue Nessie)
#   Polars  → toutes les agrégations
#
# Dépendances :
#   pip install polars boto3 python-dotenv pyspark
# ============================================================

import io
from dotenv import load_dotenv
import boto3
from botocore.client import Config
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

TABLE_SILVER         = "nessie.silver.panel_admin_solde_mensuel"
TABLE_GOLD_SALAIRES  = "nessie.gold.panel_admin_masse_salariale"
TABLE_GOLD_EFFECTIFS = "nessie.gold.panel_admin_effectifs"
BUCKET               = "staging"
PREFIX_EXPORTS       = "panel_admin/exports_gold"

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
# CONNEXION SPARK (lecture Silver + écriture Gold)
# ============================================================

spark = (
    SparkSession.builder
    .appName("panel_admin_silver_to_gold")
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

spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

# ============================================================
# LECTURE SILVER → CONVERSION POLARS
# ============================================================

print("Lecture de la table Silver...")
df_silver = pl.from_pandas(spark.table(TABLE_SILVER).toPandas())
nb_silver = len(df_silver)
print(f"Silver lu : {nb_silver:,} lignes\n")


def ecrire_gold_spark(df_polars, table_name):
    """Convertit un DataFrame Polars en Spark et écrit dans Iceberg."""
    df_sp = spark.createDataFrame(df_polars.to_pandas())
    df_sp.writeTo(table_name).using("iceberg").mode("overwrite").createOrReplace()


def exporter_vers_staging(df, nom_fichier):
    """Exporte un DataFrame Polars en CSV vers MinIO staging."""
    csv_bytes = df.write_csv().encode("utf-8")
    s3.put_object(
        Bucket=BUCKET,
        Key=f"{PREFIX_EXPORTS}/{nom_fichier}",
        Body=csv_bytes,
    )
    print(f"  ✓ {nom_fichier} ({len(df):,} lignes)")


# ============================================================
# TABLE GOLD 1 : MASSE SALARIALE PAR PÉRIODE × ORGANISME
# ============================================================

print("=" * 70)
print("GOLD 1 : Masse salariale par période × organisme")
print("=" * 70)
print()

df_masse_salariale = (
    df_silver
    .filter(
        pl.col("montant_brut").is_not_null()
        & pl.col("montant_net").is_not_null()
    )
    .with_columns([
        pl.col("periode").str.slice(0, 4).alias("annee"),
        pl.col("periode").str.slice(5, 2).alias("mois"),
        pl.col("CODE_ORGANISME").fill_null("NON_CODE").alias("code_organisme"),
        pl.col("organisme").fill_null("NON_IDENTIFIE").alias("organisme"),
    ])
    .group_by([
        "periode", "annee", "mois",
        "code_organisme", "organisme",
        "situation_normalisee",
    ])
    .agg([
        pl.len().alias("nb_lignes"),
        pl.col("matricule").n_unique().alias("nb_agents_uniques"),
        pl.col("montant_brut").sum().round(0).alias("masse_brute"),
        pl.col("montant_net").sum().round(0).alias("masse_nette"),
        pl.col("montant_brut").mean().round(0).alias("salaire_brut_moyen"),
        pl.col("montant_net").mean().round(0).alias("salaire_net_moyen"),
        pl.col("montant_brut").quantile(0.5, interpolation="nearest").round(0).alias("salaire_brut_mediane"),
        pl.col("montant_net").quantile(0.5, interpolation="nearest").round(0).alias("salaire_net_mediane"),
    ])
    .sort(["periode", "organisme"])
)

nb_ms = len(df_masse_salariale)
print(f"✓ {nb_ms:,} lignes produites")

ecrire_gold_spark(df_masse_salariale, TABLE_GOLD_SALAIRES)
print(f"✓ Table écrite : {TABLE_GOLD_SALAIRES}\n")

# ============================================================
# TABLE GOLD 2 : EFFECTIFS PAR PÉRIODE
# ============================================================

print("=" * 70)
print("GOLD 2 : Effectifs par période")
print("=" * 70)
print()

df_effectifs = (
    df_silver
    .with_columns([
        pl.col("periode").str.slice(0, 4).alias("annee"),
        pl.col("periode").str.slice(5, 2).alias("mois"),
        pl.col("CODE_ORGANISME").fill_null("NON_CODE").alias("code_organisme"),
    ])
    .group_by([
        "periode", "annee", "mois",
        "situation_normalisee", "code_organisme",
    ])
    .agg([
        pl.len().alias("nb_lignes"),
        pl.col("matricule").n_unique().alias("nb_agents_uniques"),
        (pl.col("sexe") == "1").sum().alias("nb_hommes"),
        (pl.col("sexe") == "2").sum().alias("nb_femmes"),
    ])
    .sort(["periode", "situation_normalisee"])
)

nb_eff = len(df_effectifs)
print(f"✓ {nb_eff:,} lignes produites")

ecrire_gold_spark(df_effectifs, TABLE_GOLD_EFFECTIFS)
print(f"✓ Table écrite : {TABLE_GOLD_EFFECTIFS}\n")

# ============================================================
# EXPORT CSV VERS STAGING (pour consultation externe)
# ============================================================

print("=" * 70)
print("EXPORT CSV vers staging")
print("=" * 70)
print()

exporter_vers_staging(df_masse_salariale, "masse_salariale_par_periode_organisme.csv")
exporter_vers_staging(df_effectifs,       "effectifs_par_periode.csv")

# ============================================================
# RÉSUMÉ
# ============================================================

print()
print("=" * 70)
print("RÉSUMÉ GOLD")
print("=" * 70)
print()

print(f"{TABLE_GOLD_SALAIRES:<45} : {nb_ms:,} lignes")
print(f"{TABLE_GOLD_EFFECTIFS:<45} : {nb_eff:,} lignes")
print(f"\nExports CSV : s3://{BUCKET}/{PREFIX_EXPORTS}/")
print("\n✓ PIPELINE PANEL ADMIN COMPLET")
print("=" * 70)
print("\nTables disponibles pour requêtage :")
print(f"  - {TABLE_SILVER}")
print(f"  - {TABLE_GOLD_SALAIRES}")
print(f"  - {TABLE_GOLD_EFFECTIFS}")
print("\nOuvrir exploration.py pour interroger ces tables.")

spark.stop()
