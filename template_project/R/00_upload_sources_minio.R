# ============================================================
# PANEL ADMIN — ÉTAPE 0 : UPLOAD DES SOURCES VERS MINIO STAGING
# ============================================================
# À exécuter UNE SEULE FOIS pour déposer les fichiers sources
# sur MinIO. Ensuite, toute l'équipe travaille depuis staging.
#
# Dépose :
#   - Les ~120 fichiers Excel mensuels (2015-2025)
#     vers : staging/panel_admin/fichiers_mensuels/
#   - Le fichier de référence ANSTAT_CODE
#     vers : staging/panel_admin/references/
#
# Dépendances :
#   install.packages(c("aws.s3", "dotenv", "httr"))
# ============================================================

library(aws.s3)
library(dotenv)
library(httr)

httr::set_config(httr::config(ssl_verifypeer = FALSE, connecttimeout = 60, timeout = 600))

load_dot_env(file = "template_project/.env")

# --- CONFIGURATION ---

#LORSQU'ON TRAVAILLE DEPUIS SA MACHINE LOCAL
MINIO_ENDPOINT   <- "http://192.168.1.230:30137"
MINIO_ACCESS_KEY <- "datalab-team"
MINIO_SECRET_KEY <- "minio-datalabteam123"

#---------------------------------------------------------------------------------

#LORSQU'ON TRAVAILLE SUR JHUB
# MINIO_ENDPOINT   <- "http://minio.mon-namespace.svc.cluster.local:80"
# MINIO_ACCESS_KEY <- "datalab-team"
# MINIO_SECRET_KEY <- "minio-datalabteam123"

#---------------------------------------------------------------------------------

endpoint_propre <- sub("^http?://", "", MINIO_ENDPOINT)

Sys.setenv(
  AWS_ACCESS_KEY_ID     = MINIO_ACCESS_KEY,
  AWS_SECRET_ACCESS_KEY = MINIO_SECRET_KEY,
  AWS_DEFAULT_REGION    = "us-east-1"
)

# ============================================================
# <<< À MODIFIER : chemins locaux vers vos fichiers sources >>>
DOSSIER_EXCEL   <- "consolidation_solde_2015_2025/01_data_sources/fichiers_solde_mensuels"
FICHIER_ANSTAT  <- "consolidation_solde_2015_2025/01_data_sources/fichiers_anstat_codes/FICHIER_ANSTAT_CODE_2025.xlsx"
# <<<----------->>>
# ============================================================

BUCKET          <- "staging"
PREFIX_MENSUELS <- "panel_admin/fichiers_mensuels"
PREFIX_REFS     <- "panel_admin/references"

# --- UPLOAD DES FICHIERS MENSUELS ---

cat(paste(rep("=", 60), collapse = ""), "\n")
cat("UPLOAD : Fichiers mensuels Excel\n")
cat(paste(rep("=", 60), collapse = ""), "\n\n")

fichiers_excel <- list.files(
  path.expand(DOSSIER_EXCEL),
  pattern     = "\\.xlsx$",
  full.names  = TRUE,
  recursive   = TRUE
)
fichiers_excel <- sort(fichiers_excel)

cat(sprintf("Fichiers trouvés : %d\n\n", length(fichiers_excel)))

compteur_ok  <- 0
compteur_err <- 0

for (fichier in fichiers_excel) {
  nom_fichier <- basename(fichier)
  objet_dest  <- file.path(PREFIX_MENSUELS, nom_fichier)

  cat(sprintf("  Upload : %s ... ", nom_fichier))

  tryCatch({
    aws.s3::put_object(
      file      = fichier,
      object    = objet_dest,
      bucket    = BUCKET,
      base_url  = endpoint_propre,
      region    = "",
      use_https = FALSE,
      multipart = TRUE
    )
    cat("✓\n")
    compteur_ok <- compteur_ok + 1
  }, error = function(e) {
    cat(sprintf("✗ ERREUR : %s\n", e$message))
    compteur_err <<- compteur_err + 1
  })
}

cat(sprintf("\n✓ %d fichiers uploadés, %d erreurs\n\n", compteur_ok, compteur_err))

# --- UPLOAD DU FICHIER DE RÉFÉRENCE ANSTAT ---

cat(paste(rep("=", 60), collapse = ""), "\n")
cat("UPLOAD : Fichier de référence ANSTAT\n")
cat(paste(rep("=", 60), collapse = ""), "\n\n")

fichier_anstat <- path.expand(FICHIER_ANSTAT)

if (file.exists(fichier_anstat)) {
  objet_ref <- file.path(PREFIX_REFS, basename(fichier_anstat))

  tryCatch({
    aws.s3::put_object(
      file      = fichier_anstat,
      object    = objet_ref,
      bucket    = BUCKET,
      base_url  = endpoint_propre,
      region    = "",
      use_https = FALSE,
      multipart = TRUE
    )
    cat(sprintf("✓ %s → s3://%s/%s\n", basename(fichier_anstat), BUCKET, objet_ref))
  }, error = function(e) {
    cat(sprintf("✗ ERREUR : %s\n", e$message))
  })
} else {
  cat(sprintf("✗ Fichier non trouvé : %s\n", fichier_anstat))
}

cat("\n")
cat(paste(rep("=", 60), collapse = ""), "\n")
cat("✓ UPLOAD TERMINÉ\n")
cat(paste(rep("=", 60), collapse = ""), "\n")
cat(sprintf("\nVos fichiers sont disponibles sur :\n"))
cat(sprintf("  s3://%s/%s/\n", BUCKET, PREFIX_MENSUELS))
cat(sprintf("  s3://%s/%s/\n", BUCKET, PREFIX_REFS))
cat("\nProchaine étape : exécuter 01_pre_analyse.R\n")

