# ============================================================
# PANEL ADMIN — ÉTAPE 4 : VALIDATION DE LA TABLE SILVER
# ============================================================
# Effectue les contrôles qualité sur la table Silver Iceberg.
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
# Architecture :
#   PySpark → lecture Iceberg (catalogue Nessie)
#   Polars  → tous les contrôles qualité
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
    csv_bytes = df.write_csv().encode("utf-8")
    s3.put_object(
        Bucket=BUCKET,
        Key=f"{PREFIX_VALID}/{nom_fichier}",
        Body=csv_bytes,
    )
    print(f"  ✓ Rapport : {nom_fichier}")


# ============================================================
# CONNEXION SPARK (lecture Iceberg uniquement)
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

# ============================================================
# LECTURE SILVER → CONVERSION POLARS
# ============================================================

print("=" * 70)
print("POST-VALIDATION : CONTRÔLE QUALITÉ TABLE SILVER")
print("=" * 70)
print()

df = pl.from_pandas(spark.table(TABLE_SILVER).toPandas())
spark.stop()  # Spark n'est plus nécessaire après la lecture

nb_total = len(df)
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

colonnes_cles = [
    c for c in ["matricule", "nom", "montant_brut", "montant_net",
                "CODE_ORGANISME", "CODE_GRADE", "CODE_EMPLOI",
                "situation_normalisee", "periode"]
    if c in cols_all
]

for col in colonnes_cles:
    pct  = round(100.0 * df[col].is_not_null().sum() / nb_total, 1)
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

cols_a_surveiller = [
    c for c in ["matricule", "nom", "montant_brut", "montant_net",
                "organisme", "grade", "emploi", "lieu_affectation", "service"]
    if c in cols_all
]

if "periode" in cols_all:
    df_avec_annee = df.with_columns(
        pl.col("periode").str.slice(0, 4).alias("annee")
    )

    na_par_annee_rows = []
    for (annee,), groupe in df_avec_annee.group_by("annee"):
        row = {"annee": annee, "n": len(groupe)}
        for col in cols_a_surveiller:
            row[f"{col}_pct_na"] = round(100.0 * groupe[col].is_null().sum() / len(groupe), 1)
        na_par_annee_rows.append(row)

    na_par_annee = pl.DataFrame(na_par_annee_rows).sort("annee")

    colonnes_suspectes = []
    for col in cols_a_surveiller:
        col_na = f"{col}_pct_na"
        if col_na in na_par_annee.columns:
            pcts = na_par_annee[col_na].drop_nulls()
            if pcts.max() > 95 and pcts.min() < 5:
                colonnes_suspectes.append(col)
                print(f"⚠️  {col} : NA varie de {pcts.min():.1f}% à {pcts.max():.1f}% selon l'année")

    if not colonnes_suspectes:
        print("✓ Aucune colonne suspecte détectée")
    else:
        sauvegarder_rapport(na_par_annee, "na_par_annee_colonnes_surveillees.csv")
else:
    print("  (colonne 'periode' absente — contrôle ignoré)")
print()

# ============================================================
# CONTRÔLE 3 : DOUBLONS MATRICULE × PÉRIODE
# ============================================================

print("=" * 70)
print("CONTRÔLE 3 : DOUBLONS MATRICULE × PÉRIODE")
print("=" * 70)
print()

if "matricule" in cols_all and "periode" in cols_all:
    doublons = (
        df.filter(pl.col("matricule").is_not_null())
        .group_by(["matricule", "periode"])
        .agg(pl.len().alias("n"))
        .filter(pl.col("n") > 1)
        .sort("n", descending=True)
    )

    if len(doublons) > 0:
        print(f"⚠️  {len(doublons):,} combinaisons matricule × période en doublon")
        print("  Top 5 doublons :")
        for row in doublons.head(5).iter_rows(named=True):
            print(f"    {row['matricule']} | {row['periode']} | {row['n']} occurrences")
        sauvegarder_rapport(doublons, "doublons_matricule_periode.csv")
    else:
        print("✓ Aucun doublon matricule × période")
else:
    print("  (colonnes 'matricule' ou 'periode' absentes — contrôle ignoré)")
print()

# ============================================================
# CONTRÔLE 4 : COHÉRENCE MONTANTS (net > brut)
# ============================================================

print("=" * 70)
print("CONTRÔLE 4 : COHÉRENCE MONTANTS (net > brut)")
print("=" * 70)
print()

if "montant_net" in cols_all and "montant_brut" in cols_all:
    nb_inc = (
        df.filter(
            pl.col("montant_net").is_not_null()
            & pl.col("montant_brut").is_not_null()
            & (pl.col("montant_net") > pl.col("montant_brut"))
        )
        .select(pl.len())
        .item()
    )
    pct_inc = round(100.0 * nb_inc / nb_total, 2)
    if nb_inc > 0:
        print(f"⚠️  Net > Brut : {nb_inc:,} lignes ({pct_inc:.2f}%)")
    else:
        print("✓ Tous les montants sont cohérents (net ≤ brut)")
else:
    print("  (colonnes 'montant_net' ou 'montant_brut' absentes — contrôle ignoré)")
print()

# ============================================================
# CONTRÔLE 5 : DISTRIBUTION DES SITUATIONS
# ============================================================

print("=" * 70)
print("CONTRÔLE 5 : DISTRIBUTION DES SITUATIONS")
print("=" * 70)
print()

if "situation_normalisee" in cols_all:
    dist_sit = (
        df.group_by("situation_normalisee")
        .agg(pl.len().alias("nb"))
        .with_columns(
            (pl.col("nb") * 100.0 / nb_total).round(1).alias("pct")
        )
        .sort("nb", descending=True)
    )
    for row in dist_sit.iter_rows(named=True):
        print(f"  {row['situation_normalisee']:<25} : {row['nb']:,} ({row['pct']:.1f}%)")
else:
    print("  (colonne 'situation_normalisee' absente — contrôle ignoré)")
print()

# ============================================================
# RÉSUMÉ FINAL
# ============================================================

print("=" * 70)
print("RÉSUMÉ VALIDATION")
print("=" * 70)
print()

print(f"Lignes totales  : {nb_total:,}")
print(f"Colonnes        : {nb_cols}")

if "periode" in cols_all:
    print(f"Périodes        : {df['periode'].min()} à {df['periode'].max()} "
          f"({df['periode'].n_unique()} périodes)")

if "matricule" in cols_all:
    print(f"Agents uniques  : {df['matricule'].n_unique():,}")

print()
print("=" * 70)
print("✓ VALIDATION TERMINÉE")
print("=" * 70)
print("Prochaine étape : exécuter 05_silver_to_gold.py")
