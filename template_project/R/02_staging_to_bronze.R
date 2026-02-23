# ============================================================
# PANEL ADMIN — ÉTAPE 2 : STAGING → BRONZE ICEBERG
# ============================================================
# Adapté de : 02_consolidation_complete_2015_2025.R
#
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
#   install.packages(c("sparklyr", "dplyr", "readxl", "data.table",
#                      "stringi", "aws.s3", "dotenv", "httr"))
# ============================================================

library(sparklyr)
library(dplyr)
library(readxl)
library(data.table)
library(stringi)
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

BUCKET          <- "staging"
PREFIX_MENSUELS <- "panel_admin/fichiers_mensuels"
TABLE_BRONZE    <- "nessie.bronze.panel_admin_solde_mensuel"
TAILLE_LOT      <- 12   # 12 mois traités puis écrits en batch

endpoint_propre <- sub("^https?://", "", MINIO_ENDPOINT)

Sys.setenv(
  AWS_ACCESS_KEY_ID     = MINIO_ACCESS_KEY,
  AWS_SECRET_ACCESS_KEY = MINIO_SECRET_KEY,
  AWS_DEFAULT_REGION    = "us-east-1"
)

# ============================================================
# FONCTIONS UTILITAIRES (extraites du script original)
# ============================================================

normaliser_nom_colonne <- function(nom) {
  if (is.na(nom) || nom == "") return("colonne_vide")
  nom <- gsub("^[A-Z_]+[0-9]+\\.", "", nom)
  nom <- toupper(nom)
  nom <- gsub("[-/_ ]", "_", nom)
  nom <- gsub("\\.", "_", nom)
  nom <- stri_trans_general(nom, "Latin-ASCII")
  nom <- gsub("[^A-Z0-9_]", "", nom)
  nom <- gsub("_+", "_", nom)
  return(trimws(nom, whitespace = "_"))
}

detecter_entete <- function(fichier, sheet) {
  tryCatch({
    preview <- read_excel(fichier, sheet = sheet, col_names = FALSE, n_max = 20)
    if (nrow(preview) > 1) preview <- preview[-nrow(preview), ]
    for (i in 1:nrow(preview)) {
      ligne_lower <- tolower(trimws(as.character(preview[i, ])))
      if (any(grepl("matricule", ligne_lower, fixed = TRUE))) return(i)
    }
    return(ifelse(sheet == 2, 2, 1))
  }, error = function(e) {
    return(ifelse(sheet == 2, 2, 1))
  })
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

DBI::dbExecute(sc, "CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

# ============================================================
# LISTER LES FICHIERS DANS STAGING
# ============================================================

cat(paste(rep("=", 70), collapse = ""), "\n")
cat("STAGING → BRONZE : Panel Administratif\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

objets <- aws.s3::get_bucket_df(
  bucket    = BUCKET,
  prefix    = PREFIX_MENSUELS,
  base_url  = endpoint_propre,
  region    = "",
  use_https = FALSE
)
fichiers_minio <- sort(objets$Key[grepl("\\.xlsx$", objets$Key, ignore.case = TRUE)])

cat(sprintf("Fichiers dans staging : %d\n", length(fichiers_minio)))
cat(sprintf("Taille des lots       : %d fichiers\n", TAILLE_LOT))

nb_lots <- ceiling(length(fichiers_minio) / TAILLE_LOT)
lots    <- split(fichiers_minio, ceiling(seq_along(fichiers_minio) / TAILLE_LOT))
cat(sprintf("Nombre de lots        : %d\n\n", nb_lots))

# ============================================================
# TRAITEMENT PAR LOTS
# ============================================================

premier_lot <- TRUE  # Contrôle du mode overwrite/append

for (i_lot in seq_along(lots)) {
  fichiers_lot <- lots[[i_lot]]

  cat(sprintf("[LOT %d/%d] %d fichiers\n", i_lot, nb_lots, length(fichiers_lot)))
  cat(paste(rep("-", 70), collapse = ""), "\n")

  resultats_lot <- list()

  for (chemin_objet in fichiers_lot) {
    periode    <- gsub("\\.xlsx$", "", basename(chemin_objet))
    nom_fichier <- basename(chemin_objet)
    cat(sprintf("  [%s] Téléchargement... ", periode))

    tmp <- tempfile(fileext = ".xlsx")

    tryCatch({
      # Téléchargement depuis staging
      aws.s3::save_object(
        object    = chemin_objet,
        bucket    = BUCKET,
        file      = tmp,
        base_url  = endpoint_propre,
        region    = "",
        use_https = FALSE
      )

      # --- LECTURE FEUILLE 1 (données agents) ---
      ligne_entete_f1 <- detecter_entete(tmp, sheet = 1)
      feuille1 <- as.data.table(read_excel(tmp, sheet = 1,
                                           skip        = ligne_entete_f1 - 1,
                                           col_types   = "text"))

      # Normalisation minimale des noms de colonnes (Bronze = noms propres, pas de mapping métier)
      noms_normalises <- sapply(names(feuille1), normaliser_nom_colonne)
      # Gérer les doublons éventuels
      doublons <- noms_normalises[duplicated(noms_normalises)]
      if (length(doublons) > 0) {
        for (nd in unique(doublons)) {
          idx <- which(noms_normalises == nd)
          for (k in seq_along(idx)) {
            if (k > 1) noms_normalises[idx[k]] <- paste0(nd, "_", k)
          }
        }
      }
      setnames(feuille1, old = names(feuille1), new = noms_normalises)

      # --- LECTURE FEUILLE 2 (codes numériques) ---
      ligne_entete_f2 <- detecter_entete(tmp, sheet = 2)
      feuille2 <- as.data.table(read_excel(tmp, sheet = 2,
                                           skip      = ligne_entete_f2 - 1,
                                           col_types = "text"))

      # Trouver la clé de jointure F2 (colonne MATRICULE||CODE_ORGANISME)
      nom_cle_f2 <- names(feuille2)[grepl("MATRICULE.*\\|\\|", names(feuille2), ignore.case = TRUE)][1]

      if (!is.na(nom_cle_f2)) {
        setnames(feuille2, nom_cle_f2, "CLE_UNIQUE_F2")

        # Normaliser les autres colonnes de F2
        autres_cols_f2 <- setdiff(names(feuille2), "CLE_UNIQUE_F2")
        noms_f2_norm   <- sapply(autres_cols_f2, normaliser_nom_colonne)

        # Éviter les conflits avec F1 : préfixer les colonnes F2
        noms_f2_norm <- paste0(noms_f2_norm, "_F2")
        setnames(feuille2, autres_cols_f2, noms_f2_norm)

        # Clé de jointure côté F1
        cle_f1 <- if ("CLE_UNIQUE" %in% names(feuille1)) "CLE_UNIQUE" else "MATRICULE"

        if (cle_f1 %in% names(feuille1)) {
          feuille1 <- merge(feuille1, feuille2,
                            by.x = cle_f1, by.y = "CLE_UNIQUE_F2",
                            all.x = TRUE)
        }
      }

      # Métadonnées
      feuille1[, PERIODE          := periode]
      feuille1[, FICHIER_SOURCE   := nom_fichier]

      resultats_lot[[periode]] <- feuille1
      unlink(tmp)
      gc(verbose = FALSE)

      cat(sprintf("✓ %s lignes × %d cols\n",
                  format(nrow(feuille1), big.mark = ","), ncol(feuille1)))

    }, error = function(e) {
      cat(sprintf("✗ ERREUR : %s\n", e$message))
      unlink(tmp)
      gc(verbose = FALSE)
    })
  }

  # --- CONSOLIDATION DU LOT ---
  if (length(resultats_lot) == 0) next

  cat(sprintf("\n  Consolidation lot %d...\n", i_lot))

  toutes_colonnes <- unique(unlist(lapply(resultats_lot, names)))

  for (periode in names(resultats_lot)) {
    dt  <- resultats_lot[[periode]]
    manquantes <- setdiff(toutes_colonnes, names(dt))
    if (length(manquantes) > 0) {
      for (col in manquantes) dt[, (col) := NA_character_]
    }
    setcolorder(dt, toutes_colonnes)
    resultats_lot[[periode]] <- dt
  }

  base_lot <- rbindlist(resultats_lot, use.names = TRUE, fill = TRUE)

  # Tout en character pour Bronze
  for (col in names(base_lot)) {
    if (!is.character(base_lot[[col]])) {
      base_lot[, (col) := as.character(get(col))]
    }
  }

  cat(sprintf("  ✓ Lot %d : %s lignes × %d colonnes\n",
              i_lot, format(nrow(base_lot), big.mark = ","), ncol(base_lot)))

  # --- ÉCRITURE VERS BRONZE ICEBERG ---
  cat(sprintf("  Écriture vers %s... ", TABLE_BRONZE))

  df_spark <- copy_to(sc, base_lot, overwrite = TRUE)

  mode_ecriture <- if (premier_lot) "overwrite" else "append"
  spark_write_table(df_spark, TABLE_BRONZE,
                    mode   = mode_ecriture,
                    format = "iceberg")

  cat("✓\n\n")
  premier_lot <- FALSE

  rm(resultats_lot, base_lot, df_spark)
  gc()
}

# ============================================================
# RÉSUMÉ FINAL
# ============================================================

cat(paste(rep("=", 70), collapse = ""), "\n")
cat("RÉSUMÉ\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

nb_total <- DBI::dbGetQuery(sc, sprintf("SELECT COUNT(*) AS n FROM %s", TABLE_BRONZE))$n
cat(sprintf("Table Bronze : %s lignes\n", format(nb_total, big.mark = ",")))
cat(sprintf("Table        : %s\n", TABLE_BRONZE))
cat("\n✓ INGESTION BRONZE TERMINÉE\n")
cat("Prochaine étape : exécuter 03_bronze_to_silver.R\n")

spark_disconnect(sc)
