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
#   install.packages(c("sparklyr", "dplyr", "arrow", "aws.s3", "dotenv", "httr"))
# ============================================================

library(sparklyr)
library(dplyr)
library(arrow)
library(aws.s3)
library(dotenv)
library(httr)

httr::set_config(httr::config(ssl_verifypeer = FALSE, connecttimeout = 60, timeout = 600))

load_dot_env(file = ".env")

# --- CONFIGURATION ---

#LORSQU'ON TRAVAILLE DEPUIS SA MACHINE LOCAL
MINIO_ENDPOINT   <- "http://192.168.1.230:30137"
MINIO_ACCESS_KEY <- "datalab-team"
MINIO_SECRET_KEY <- "minio-datalabteam123"
NESSIE_URI       <- "http://192.168.1.230:30604/api/v1"

#LORSQU'ON TRAVAILLE SUR JHUB
# MINIO_ENDPOINT   <- "http://minio.mon-namespace.svc.cluster.local:80"
# MINIO_ACCESS_KEY <- "datalab-team"
# MINIO_SECRET_KEY <- "minio-datalabteam123"
# NESSIE_URI       <- "http://nessie.trino.svc.cluster.local:19120/api/v1"

#---------------------------------------------------------------------------------

TABLE_SILVER        <- "nessie.silver.panel_admin_solde_mensuel"
TABLE_GOLD_SALAIRES <- "nessie.gold.panel_admin_masse_salariale"
TABLE_GOLD_EFFECTIFS <- "nessie.gold.panel_admin_effectifs"
BUCKET              <- "staging"
PREFIX_EXPORTS      <- "panel_admin/exports_gold"

endpoint_propre <- sub("^https?://", "", MINIO_ENDPOINT)

Sys.setenv(
  AWS_ACCESS_KEY_ID     = MINIO_ACCESS_KEY,
  AWS_SECRET_ACCESS_KEY = MINIO_SECRET_KEY,
  AWS_DEFAULT_REGION    = "us-east-1"
)

# ============================================================
# CONNEXION SPARK
# ============================================================

config <- spark_config()
config$spark.driver.memory <- "16g"
config$spark.jars.packages <- paste(
  "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
  "org.apache.hadoop:hadoop-aws:3.3.4",
  "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1",
  sep = ","
)
config$spark.sql.extensions <- paste(
  "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
  "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
  sep = ","
)
config$spark.sql.catalog.nessie                  <- "org.apache.iceberg.spark.SparkCatalog"
config$`spark.sql.catalog.nessie.catalog-impl`   <- "org.apache.iceberg.nessie.NessieCatalog"
config$spark.sql.catalog.nessie.uri              <- NESSIE_URI
config$spark.sql.catalog.nessie.ref              <- "main"
config$spark.sql.catalog.nessie.warehouse        <- "s3a://bronze/"
config$spark.hadoop.fs.s3a.endpoint                    <- MINIO_ENDPOINT
config$spark.hadoop.fs.s3a.access.key                  <- MINIO_ACCESS_KEY
config$spark.hadoop.fs.s3a.secret.key                  <- MINIO_SECRET_KEY
config$spark.hadoop.fs.s3a.path.style.access           <- "true"
config$spark.hadoop.fs.s3a.impl                        <- "org.apache.hadoop.fs.s3a.S3AFileSystem"
config$`spark.hadoop.fs.s3a.aws.credentials.provider`  <- "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"

sc <- spark_connect(master = "local", config = config)
DBI::dbExecute(sc, "CREATE NAMESPACE IF NOT EXISTS nessie.gold")

df_silver <- spark_read_table(sc, TABLE_SILVER)
cat(sprintf("Silver lu : %s lignes\n\n", format(sdf_nrow(df_silver), big.mark = ",")))

# ============================================================
# TABLE GOLD 1 : MASSE SALARIALE PAR PÉRIODE × ORGANISME
# ============================================================

cat(paste(rep("=", 70), collapse = ""), "\n")
cat("GOLD 1 : Masse salariale par période × organisme\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

df_masse_salariale <- DBI::dbGetQuery(sc, sprintf("
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
  FROM %s
  WHERE montant_brut IS NOT NULL
    AND montant_net IS NOT NULL
  GROUP BY
    periode, SUBSTR(periode, 1, 4), SUBSTR(periode, 6, 2),
    COALESCE(CODE_ORGANISME, 'NON_CODE'),
    COALESCE(organisme, 'NON_IDENTIFIE'),
    situation_normalisee
  ORDER BY periode, organisme
", TABLE_SILVER))

cat(sprintf("✓ %s lignes produites\n", format(nrow(df_masse_salariale), big.mark = ",")))

# Écriture en Gold
df_spark_ms <- copy_to(sc, df_masse_salariale, overwrite = TRUE)
spark_write_table(df_spark_ms, TABLE_GOLD_SALAIRES,
                  mode = "overwrite", format = "iceberg")
cat(sprintf("✓ Table écrite : %s\n\n", TABLE_GOLD_SALAIRES))

# ============================================================
# TABLE GOLD 2 : EFFECTIFS PAR PÉRIODE
# ============================================================

cat(paste(rep("=", 70), collapse = ""), "\n")
cat("GOLD 2 : Effectifs par période\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

df_effectifs <- DBI::dbGetQuery(sc, sprintf("
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
  FROM %s
  GROUP BY
    periode, SUBSTR(periode, 1, 4), SUBSTR(periode, 6, 2),
    situation_normalisee,
    COALESCE(CODE_ORGANISME, 'NON_CODE')
  ORDER BY periode, situation_normalisee
", TABLE_SILVER))

cat(sprintf("✓ %s lignes produites\n", format(nrow(df_effectifs), big.mark = ",")))

df_spark_eff <- copy_to(sc, df_effectifs, overwrite = TRUE)
spark_write_table(df_spark_eff, TABLE_GOLD_EFFECTIFS,
                  mode = "overwrite", format = "iceberg")
cat(sprintf("✓ Table écrite : %s\n\n", TABLE_GOLD_EFFECTIFS))

# ============================================================
# EXPORT CSV VERS STAGING (pour consultation externe)
# ============================================================

cat(paste(rep("=", 70), collapse = ""), "\n")
cat("EXPORT CSV vers staging\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

exporter_vers_staging <- function(df_local, nom_fichier) {
  tmp <- tempfile(fileext = ".csv")
  write.csv(df_local, tmp, row.names = FALSE)
  aws.s3::put_object(
    file      = tmp,
    object    = file.path(PREFIX_EXPORTS, nom_fichier),
    bucket    = BUCKET,
    base_url  = endpoint_propre,
    region    = "",
    use_https = FALSE
  )
  unlink(tmp)
  cat(sprintf("  ✓ %s (%s lignes)\n", nom_fichier, format(nrow(df_local), big.mark = ",")))
}

exporter_vers_staging(df_masse_salariale, "masse_salariale_par_periode_organisme.csv")
exporter_vers_staging(df_effectifs,       "effectifs_par_periode.csv")

# ============================================================
# RÉSUMÉ
# ============================================================

cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("RÉSUMÉ GOLD\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

cat(sprintf("%-40s : %s lignes\n", TABLE_GOLD_SALAIRES,
            format(nrow(df_masse_salariale), big.mark = ",")))
cat(sprintf("%-40s : %s lignes\n", TABLE_GOLD_EFFECTIFS,
            format(nrow(df_effectifs), big.mark = ",")))

cat(sprintf("\nExports CSV : s3://%s/%s/\n", BUCKET, PREFIX_EXPORTS))
cat("\n✓ PIPELINE PANEL ADMIN COMPLET\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("\nTables disponibles pour requêtage :\n")
cat(sprintf("  - %s\n", TABLE_SILVER))
cat(sprintf("  - %s\n", TABLE_GOLD_SALAIRES))
cat(sprintf("  - %s\n", TABLE_GOLD_EFFECTIFS))
cat("\nOuvrir exploration.R pour interroger ces tables.\n")

spark_disconnect(sc)
