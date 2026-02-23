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
# Dépendances :
#   pip install pyspark boto3 pandas python-dotenv
# ============================================================

import io
import os
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import pandas as pd
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
# CONNEXION SPARK
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

df_silver = spark.table(TABLE_SILVER)
nb_silver = df_silver.count()
print(f"Silver lu : {nb_silver:,} lignes\n")

# ============================================================
# TABLE GOLD 1 : MASSE SALARIALE PAR PÉRIODE × ORGANISME
# ============================================================

print("=" * 70)
print("GOLD 1 : Masse salariale par période × organisme")
print("=" * 70)
print()

df_masse_salariale = spark.sql(f"""
    SELECT
        periode,
        SUBSTR(periode, 1, 4)                              AS annee,
        SUBSTR(periode, 6, 2)                              AS mois,
        COALESCE(CODE_ORGANISME, 'NON_CODE')               AS code_organisme,
        COALESCE(organisme, 'NON_IDENTIFIE')               AS organisme,
        situation_normalisee,
        COUNT(*)                                           AS nb_lignes,
        COUNT(DISTINCT matricule)                          AS nb_agents_uniques,
        ROUND(SUM(montant_brut), 0)                        AS masse_brute,
        ROUND(SUM(montant_net), 0)                         AS masse_nette,
        ROUND(AVG(montant_brut), 0)                        AS salaire_brut_moyen,
        ROUND(AVG(montant_net), 0)                         AS salaire_net_moyen,
        ROUND(PERCENTILE_APPROX(montant_brut, 0.5), 0)     AS salaire_brut_mediane,
        ROUND(PERCENTILE_APPROX(montant_net, 0.5), 0)      AS salaire_net_mediane
    FROM {TABLE_SILVER}
    WHERE montant_brut IS NOT NULL
      AND montant_net IS NOT NULL
    GROUP BY
        periode, SUBSTR(periode, 1, 4), SUBSTR(periode, 6, 2),
        COALESCE(CODE_ORGANISME, 'NON_CODE'),
        COALESCE(organisme, 'NON_IDENTIFIE'),
        situation_normalisee
    ORDER BY periode, organisme
""")

nb_ms = df_masse_salariale.count()
print(f"✓ {nb_ms:,} lignes produites")

df_masse_salariale.writeTo(TABLE_GOLD_SALAIRES).using("iceberg").mode("overwrite").createOrReplace()
print(f"✓ Table écrite : {TABLE_GOLD_SALAIRES}\n")

# ============================================================
# TABLE GOLD 2 : EFFECTIFS PAR PÉRIODE
# ============================================================

print("=" * 70)
print("GOLD 2 : Effectifs par période")
print("=" * 70)
print()

df_effectifs = spark.sql(f"""
    SELECT
        periode,
        SUBSTR(periode, 1, 4)                        AS annee,
        SUBSTR(periode, 6, 2)                        AS mois,
        situation_normalisee,
        COALESCE(CODE_ORGANISME, 'NON_CODE')         AS code_organisme,
        COUNT(*)                                     AS nb_lignes,
        COUNT(DISTINCT matricule)                    AS nb_agents_uniques,
        SUM(CASE WHEN sexe = '1' THEN 1 ELSE 0 END) AS nb_hommes,
        SUM(CASE WHEN sexe = '2' THEN 1 ELSE 0 END) AS nb_femmes
    FROM {TABLE_SILVER}
    GROUP BY
        periode, SUBSTR(periode, 1, 4), SUBSTR(periode, 6, 2),
        situation_normalisee,
        COALESCE(CODE_ORGANISME, 'NON_CODE')
    ORDER BY periode, situation_normalisee
""")

nb_eff = df_effectifs.count()
print(f"✓ {nb_eff:,} lignes produites")

df_effectifs.writeTo(TABLE_GOLD_EFFECTIFS).using("iceberg").mode("overwrite").createOrReplace()
print(f"✓ Table écrite : {TABLE_GOLD_EFFECTIFS}\n")

# ============================================================
# EXPORT CSV VERS STAGING (pour consultation externe)
# ============================================================

print("=" * 70)
print("EXPORT CSV vers staging")
print("=" * 70)
print()


def exporter_vers_staging(df_spark, nom_fichier):
    df_local = df_spark.toPandas()
    buf = io.StringIO()
    df_local.to_csv(buf, index=False)
    buf.seek(0)
    s3.put_object(
        Bucket = BUCKET,
        Key    = f"{PREFIX_EXPORTS}/{nom_fichier}",
        Body   = buf.getvalue().encode("utf-8"),
    )
    print(f"  ✓ {nom_fichier} ({len(df_local):,} lignes)")
    return df_local


df_ms_local  = exporter_vers_staging(df_masse_salariale, "masse_salariale_par_periode_organisme.csv")
df_eff_local = exporter_vers_staging(df_effectifs,       "effectifs_par_periode.csv")

# ============================================================
# RÉSUMÉ
# ============================================================

print()
print("=" * 70)
print("RÉSUMÉ GOLD")
print("=" * 70)
print()

print(f"{TABLE_GOLD_SALAIRES:<40} : {nb_ms:,} lignes")
print(f"{TABLE_GOLD_EFFECTIFS:<40} : {nb_eff:,} lignes")
print(f"\nExports CSV : s3://{BUCKET}/{PREFIX_EXPORTS}/")
print("\n✓ PIPELINE PANEL ADMIN COMPLET")
print("=" * 70)
print("\nTables disponibles pour requêtage :")
print(f"  - {TABLE_SILVER}")
print(f"  - {TABLE_GOLD_SALAIRES}")
print(f"  - {TABLE_GOLD_EFFECTIFS}")
print("\nOuvrir exploration.py pour interroger ces tables.")

spark.stop()
