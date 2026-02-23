# ============================================================
# PANEL ADMIN — ÉTAPE 4 : VALIDATION DE LA TABLE SILVER
# ============================================================
# Adapté de : 03_post_validation_base_finale.R
#
# Effectue les mêmes contrôles qualité que le script original,
# mais directement sur la table Silver Iceberg via sparklyr/SQL
# (sans télécharger toute la table localement).
#
# Contrôles :
#   1. Colonnes avec variations NA suspectes entre années
#   2. Complétude des colonnes clés
#   3. Doublons matricule × période
#   4. Cohérence des montants (net > brut)
#   5. Distribution des situations administratives
#
# Les rapports sont sauvegardés dans staging/panel_admin/validation/
#
# Dépendances :
#   install.packages(c("sparklyr", "dplyr", "data.table", "aws.s3", "dotenv", "httr"))
# ============================================================

library(sparklyr)
library(dplyr)
library(data.table)
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

TABLE_SILVER   <- "nessie.silver.panel_admin_solde_mensuel"
BUCKET         <- "staging"
PREFIX_VALID   <- "panel_admin/validation"
endpoint_propre <- sub("^https?://", "", MINIO_ENDPOINT)

Sys.setenv(
  AWS_ACCESS_KEY_ID     = MINIO_ACCESS_KEY,
  AWS_SECRET_ACCESS_KEY = MINIO_SECRET_KEY,
  AWS_DEFAULT_REGION    = "us-east-1"
)

sauvegarder_rapport <- function(df, nom_fichier) {
  tmp <- tempfile(fileext = ".csv")
  write.csv(df, tmp, row.names = FALSE)
  aws.s3::put_object(file = tmp, object = file.path(PREFIX_VALID, nom_fichier),
                     bucket = BUCKET, base_url = endpoint_propre, region = "",
                     use_https = FALSE)
  unlink(tmp)
  cat(sprintf("  ✓ Rapport : %s\n", nom_fichier))
}

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

cat(paste(rep("=", 70), collapse = ""), "\n")
cat("POST-VALIDATION : CONTRÔLE QUALITÉ TABLE SILVER\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

df <- spark_read_table(sc, TABLE_SILVER)
nb_total <- sdf_nrow(df)
nb_cols  <- ncol(df)

cat(sprintf("Table : %s lignes × %d colonnes\n\n",
            format(nb_total, big.mark = ","), nb_cols))

# ============================================================
# CONTRÔLE 1 : COMPLÉTUDE DES COLONNES CLÉS
# ============================================================

cat(paste(rep("=", 70), collapse = ""), "\n")
cat("CONTRÔLE 1 : COMPLÉTUDE DES COLONNES CLÉS\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

colonnes_cles <- c("matricule", "nom", "montant_brut", "montant_net",
                   "CODE_ORGANISME", "CODE_GRADE", "CODE_EMPLOI",
                   "situation_normalisee", "periode")
colonnes_cles <- intersect(colonnes_cles, colnames(df))

completude <- DBI::dbGetQuery(sc, sprintf("
  SELECT
    %s
  FROM %s
",
  paste(sprintf("ROUND(100.0 * COUNT(%s) / COUNT(*), 1) AS %s_pct",
                colonnes_cles, colonnes_cles), collapse = ",\n    "),
  TABLE_SILVER
))

for (col in colonnes_cles) {
  pct <- completude[[paste0(col, "_pct")]]
  flag <- if (pct < 95) "⚠️ " else "✓ "
  cat(sprintf("  %s%-30s : %.1f%% complet\n", flag, col, pct))
}
cat("\n")

# ============================================================
# CONTRÔLE 2 : VARIATIONS NA PAR ANNÉE (colonnes suspectes)
# ============================================================

cat(paste(rep("=", 70), collapse = ""), "\n")
cat("CONTRÔLE 2 : COLONNES SUSPECTES (NA variable selon l'année)\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

# Calculer % NA par année pour les colonnes clés via SQL
cols_a_surveiller <- intersect(
  c("matricule", "nom", "montant_brut", "montant_net", "organisme",
    "grade", "emploi", "lieu_affectation", "service"),
  colnames(df)
)

na_par_annee <- DBI::dbGetQuery(sc, sprintf("
  SELECT
    SUBSTR(periode, 1, 4) AS annee,
    COUNT(*) AS n,
    %s
  FROM %s
  GROUP BY SUBSTR(periode, 1, 4)
  ORDER BY annee
",
  paste(sprintf("ROUND(100.0 * SUM(CASE WHEN %s IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS %s_pct_na",
                cols_a_surveiller, cols_a_surveiller), collapse = ",\n    "),
  TABLE_SILVER
))

# Identifier les colonnes avec forte variation
colonnes_suspectes <- character(0)
for (col in cols_a_surveiller) {
  col_na <- paste0(col, "_pct_na")
  if (col_na %in% names(na_par_annee)) {
    pcts <- na_par_annee[[col_na]]
    if (max(pcts, na.rm = TRUE) > 95 && min(pcts, na.rm = TRUE) < 5) {
      colonnes_suspectes <- c(colonnes_suspectes, col)
      cat(sprintf("⚠️  %s : NA varie de %.1f%% à %.1f%% selon l'année\n",
                  col, min(pcts, na.rm = TRUE), max(pcts, na.rm = TRUE)))
    }
  }
}

if (length(colonnes_suspectes) == 0) {
  cat("✓ Aucune colonne suspecte détectée\n")
} else {
  sauvegarder_rapport(na_par_annee, "na_par_annee_colonnes_surveillees.csv")
}
cat("\n")

# ============================================================
# CONTRÔLE 3 : DOUBLONS MATRICULE × PÉRIODE
# ============================================================

cat(paste(rep("=", 70), collapse = ""), "\n")
cat("CONTRÔLE 3 : DOUBLONS MATRICULE × PÉRIODE\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

doublons <- DBI::dbGetQuery(sc, sprintf("
  SELECT matricule, periode, COUNT(*) AS n
  FROM %s
  WHERE matricule IS NOT NULL
  GROUP BY matricule, periode
  HAVING COUNT(*) > 1
  ORDER BY n DESC
", TABLE_SILVER))

if (nrow(doublons) > 0) {
  cat(sprintf("⚠️  %s combinaisons matricule × période en doublon\n",
              format(nrow(doublons), big.mark = ",")))
  cat("  Top 5 doublons :\n")
  for (i in 1:min(5, nrow(doublons))) {
    cat(sprintf("    %s | %s | %d occurrences\n",
                doublons$matricule[i], doublons$periode[i], doublons$n[i]))
  }
  sauvegarder_rapport(doublons, "doublons_matricule_periode.csv")
} else {
  cat("✓ Aucun doublon matricule × période\n")
}
cat("\n")

# ============================================================
# CONTRÔLE 4 : COHÉRENCE MONTANTS (net > brut)
# ============================================================

cat(paste(rep("=", 70), collapse = ""), "\n")
cat("CONTRÔLE 4 : COHÉRENCE MONTANTS (net > brut)\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

incoherences <- DBI::dbGetQuery(sc, sprintf("
  SELECT COUNT(*) AS nb,
         ROUND(100.0 * COUNT(*) / %d, 2) AS pct
  FROM %s
  WHERE montant_net IS NOT NULL
    AND montant_brut IS NOT NULL
    AND montant_net > montant_brut
", nb_total, TABLE_SILVER))

if (incoherences$nb > 0) {
  cat(sprintf("⚠️  Net > Brut : %s lignes (%.2f%%)\n",
              format(incoherences$nb, big.mark = ","), incoherences$pct))
} else {
  cat("✓ Tous les montants sont cohérents (net ≤ brut)\n")
}
cat("\n")

# ============================================================
# CONTRÔLE 5 : DISTRIBUTION DES SITUATIONS
# ============================================================

cat(paste(rep("=", 70), collapse = ""), "\n")
cat("CONTRÔLE 5 : DISTRIBUTION DES SITUATIONS\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

dist_sit <- DBI::dbGetQuery(sc, sprintf("
  SELECT situation_normalisee,
         COUNT(*) AS nb,
         ROUND(100.0 * COUNT(*) / %d, 1) AS pct
  FROM %s
  GROUP BY situation_normalisee
  ORDER BY nb DESC
", nb_total, TABLE_SILVER))

for (i in 1:nrow(dist_sit)) {
  cat(sprintf("  %-25s : %s (%.1f%%)\n",
              dist_sit$situation_normalisee[i],
              format(dist_sit$nb[i], big.mark = ","),
              dist_sit$pct[i]))
}
cat("\n")

# ============================================================
# RÉSUMÉ FINAL
# ============================================================

cat(paste(rep("=", 70), collapse = ""), "\n")
cat("RÉSUMÉ VALIDATION\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

periodes_stats <- DBI::dbGetQuery(sc, sprintf("
  SELECT MIN(periode) AS premiere, MAX(periode) AS derniere,
         COUNT(DISTINCT periode) AS nb_periodes,
         COUNT(DISTINCT matricule) AS agents_uniques
  FROM %s
", TABLE_SILVER))

cat(sprintf("Lignes totales  : %s\n", format(nb_total, big.mark = ",")))
cat(sprintf("Colonnes        : %d\n", nb_cols))
cat(sprintf("Périodes        : %s à %s (%d périodes)\n",
            periodes_stats$premiere, periodes_stats$derniere, periodes_stats$nb_periodes))
cat(sprintf("Agents uniques  : %s\n", format(periodes_stats$agents_uniques, big.mark = ",")))
cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("✓ VALIDATION TERMINÉE\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("Prochaine étape : exécuter 05_silver_to_gold.R\n")

spark_disconnect(sc)
