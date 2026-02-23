# ============================================================
# PANEL ADMIN — ÉTAPE 4 : VALIDATION DE LA TABLE SILVER
# ============================================================
# Effectue les mêmes contrôles qualité que le script original,
# directement sur la table Silver Iceberg via PySpark/SQL.
#
# Contrôles :
#   1. Complétude des colonnes clés
#   2. Colonnes avec variations NA suspectes entre années
#   3. Doublons matricule × période
#   4. Cohérence des montants (net > brut)
#   5. Distribution des situations administratives
#
# Les rapports sont sauvegardés dans staging/panel_admin/validation/
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

TABLE_SILVER = "nessie.silver.panel_admin_solde_mensuel"
BUCKET       = "staging"
PREFIX_VALID = "panel_admin/validation"

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


def sauvegarder_rapport(df, nom_fichier):
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    s3.put_object(
        Bucket = BUCKET,
        Key    = f"{PREFIX_VALID}/{nom_fichier}",
        Body   = buf.getvalue().encode("utf-8"),
    )
    print(f"  ✓ Rapport : {nom_fichier}")


# ============================================================
# CONNEXION SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("panel_admin_validation_silver")
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

print("=" * 70)
print("POST-VALIDATION : CONTRÔLE QUALITÉ TABLE SILVER")
print("=" * 70)
print()

df       = spark.table(TABLE_SILVER)
nb_total = df.count()
nb_cols  = len(df.columns)
cols_all = df.columns

print(f"Table : {nb_total:,} lignes × {nb_cols} colonnes\n")

# ============================================================
# CONTRÔLE 1 : COMPLÉTUDE DES COLONNES CLÉS
# ============================================================

print("=" * 70)
print("CONTRÔLE 1 : COMPLÉTUDE DES COLONNES CLÉS")
print("=" * 70)
print()

colonnes_cles = [c for c in ["matricule", "nom", "montant_brut", "montant_net",
                               "CODE_ORGANISME", "CODE_GRADE", "CODE_EMPLOI",
                               "situation_normalisee", "periode"]
                 if c in cols_all]

select_parts = ", ".join(
    f"ROUND(100.0 * COUNT(`{c}`) / COUNT(*), 1) AS `{c}_pct`"
    for c in colonnes_cles
)
completude = spark.sql(f"SELECT {select_parts} FROM {TABLE_SILVER}").toPandas()

for col in colonnes_cles:
    pct  = completude[f"{col}_pct"].iloc[0]
    flag = "⚠️ " if pct < 95 else "✓ "
    print(f"  {flag}{col:<30} : {pct:.1f}% complet")
print()

# ============================================================
# CONTRÔLE 2 : VARIATIONS NA PAR ANNÉE (colonnes suspectes)
# ============================================================

print("=" * 70)
print("CONTRÔLE 2 : COLONNES SUSPECTES (NA variable selon l'année)")
print("=" * 70)
print()

cols_a_surveiller = [c for c in ["matricule", "nom", "montant_brut", "montant_net",
                                   "organisme", "grade", "emploi", "lieu_affectation", "service"]
                     if c in cols_all]

na_parts = ", ".join(
    f"ROUND(100.0 * SUM(CASE WHEN `{c}` IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS `{c}_pct_na`"
    for c in cols_a_surveiller
)
na_par_annee = spark.sql(f"""
    SELECT
        SUBSTR(periode, 1, 4) AS annee,
        COUNT(*) AS n,
        {na_parts}
    FROM {TABLE_SILVER}
    GROUP BY SUBSTR(periode, 1, 4)
    ORDER BY annee
""").toPandas()

colonnes_suspectes = []
for col in cols_a_surveiller:
    col_na = f"{col}_pct_na"
    if col_na in na_par_annee.columns:
        pcts = na_par_annee[col_na].dropna()
        if pcts.max() > 95 and pcts.min() < 5:
            colonnes_suspectes.append(col)
            print(f"⚠️  {col} : NA varie de {pcts.min():.1f}% à {pcts.max():.1f}% selon l'année")

if not colonnes_suspectes:
    print("✓ Aucune colonne suspecte détectée")
else:
    sauvegarder_rapport(na_par_annee, "na_par_annee_colonnes_surveillees.csv")
print()

# ============================================================
# CONTRÔLE 3 : DOUBLONS MATRICULE × PÉRIODE
# ============================================================

print("=" * 70)
print("CONTRÔLE 3 : DOUBLONS MATRICULE × PÉRIODE")
print("=" * 70)
print()

doublons = spark.sql(f"""
    SELECT matricule, periode, COUNT(*) AS n
    FROM {TABLE_SILVER}
    WHERE matricule IS NOT NULL
    GROUP BY matricule, periode
    HAVING COUNT(*) > 1
    ORDER BY n DESC
""").toPandas()

if len(doublons) > 0:
    print(f"⚠️  {len(doublons):,} combinaisons matricule × période en doublon")
    print("  Top 5 doublons :")
    for _, row in doublons.head(5).iterrows():
        print(f"    {row['matricule']} | {row['periode']} | {row['n']} occurrences")
    sauvegarder_rapport(doublons, "doublons_matricule_periode.csv")
else:
    print("✓ Aucun doublon matricule × période")
print()

# ============================================================
# CONTRÔLE 4 : COHÉRENCE MONTANTS (net > brut)
# ============================================================

print("=" * 70)
print("CONTRÔLE 4 : COHÉRENCE MONTANTS (net > brut)")
print("=" * 70)
print()

incoherences = spark.sql(f"""
    SELECT COUNT(*) AS nb,
           ROUND(100.0 * COUNT(*) / {nb_total}, 2) AS pct
    FROM {TABLE_SILVER}
    WHERE montant_net IS NOT NULL
      AND montant_brut IS NOT NULL
      AND montant_net > montant_brut
""").toPandas()

nb_inc = incoherences["nb"].iloc[0]
if nb_inc > 0:
    print(f"⚠️  Net > Brut : {nb_inc:,} lignes ({incoherences['pct'].iloc[0]:.2f}%)")
else:
    print("✓ Tous les montants sont cohérents (net ≤ brut)")
print()

# ============================================================
# CONTRÔLE 5 : DISTRIBUTION DES SITUATIONS
# ============================================================

print("=" * 70)
print("CONTRÔLE 5 : DISTRIBUTION DES SITUATIONS")
print("=" * 70)
print()

dist_sit = spark.sql(f"""
    SELECT situation_normalisee,
           COUNT(*) AS nb,
           ROUND(100.0 * COUNT(*) / {nb_total}, 1) AS pct
    FROM {TABLE_SILVER}
    GROUP BY situation_normalisee
    ORDER BY nb DESC
""").toPandas()

for _, row in dist_sit.iterrows():
    print(f"  {row['situation_normalisee']:<25} : {row['nb']:,} ({row['pct']:.1f}%)")
print()

# ============================================================
# RÉSUMÉ FINAL
# ============================================================

print("=" * 70)
print("RÉSUMÉ VALIDATION")
print("=" * 70)
print()

periodes_stats = spark.sql(f"""
    SELECT MIN(periode) AS premiere, MAX(periode) AS derniere,
           COUNT(DISTINCT periode) AS nb_periodes,
           COUNT(DISTINCT matricule) AS agents_uniques
    FROM {TABLE_SILVER}
""").toPandas()

row = periodes_stats.iloc[0]
print(f"Lignes totales  : {nb_total:,}")
print(f"Colonnes        : {nb_cols}")
print(f"Périodes        : {row['premiere']} à {row['derniere']} ({row['nb_periodes']} périodes)")
print(f"Agents uniques  : {row['agents_uniques']:,}")
print()
print("=" * 70)
print("✓ VALIDATION TERMINÉE")
print("=" * 70)
print("Prochaine étape : exécuter 05_silver_to_gold.py")

spark.stop()
