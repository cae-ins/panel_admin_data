# ============================================================
# PANEL ADMIN — ÉTAPE 1 : PRÉ-ANALYSE DES COLONNES
# ============================================================
# Adapté de : 01_pre_analyse_colonnes_2015_2025.R
#
# Lit les en-têtes de tous les fichiers Excel depuis MinIO staging
# (sans télécharger les données, juste les noms de colonnes)
# et produit un rapport des changements de structure année par année.
#
# Utile avant l'ingestion pour anticiper les colonnes à mapper.
# Les rapports sont déposés dans staging/panel_admin/pre_analyse/
#
# Dépendances :
#   install.packages(c("readxl", "data.table", "aws.s3", "dotenv", "httr"))
# ============================================================

library(readxl)
library(data.table)
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

BUCKET           <- "staging"
PREFIX_MENSUELS  <- "panel_admin/fichiers_mensuels"
PREFIX_SORTIE    <- "panel_admin/pre_analyse"

cat(paste(rep("=", 70), collapse = ""), "\n")
cat("PRÉ-ANALYSE : COLONNES 2015-2025 (depuis MinIO staging)\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

# --- LISTER LES FICHIERS DANS STAGING ---

objets <- aws.s3::get_bucket_df(
  bucket    = BUCKET,
  prefix    = PREFIX_MENSUELS,
  base_url  = endpoint_propre,
  region    = "",
  use_https = FALSE
)

fichiers_minio <- sort(objets$Key[grepl("\\.xlsx$", objets$Key, ignore.case = TRUE)])
cat(sprintf("✓ %d fichiers trouvés dans staging\n\n", length(fichiers_minio)))

# --- EXTRACTION DES NOMS DE COLONNES ---
# On télécharge chaque fichier temporairement juste pour lire les en-têtes (n_max = 0)

cat("Extraction des noms de colonnes...\n\n")

colonnes_par_periode <- list()
erreurs <- c()

for (chemin_objet in fichiers_minio) {
  periode <- gsub("\\.xlsx$", "", basename(chemin_objet))
  cat(sprintf("  %s... ", periode))

  tmp <- tempfile(fileext = ".xlsx")

  tryCatch({
    aws.s3::save_object(
      object    = chemin_objet,
      bucket    = BUCKET,
      file      = tmp,
      base_url  = endpoint_propre,
      region    = "",
      use_https = FALSE
    )

    f1 <- read_excel(tmp, sheet = 1, n_max = 0)
    f2 <- read_excel(tmp, sheet = 2, n_max = 0)

    colonnes_par_periode[[periode]] <- list(
      f1    = names(f1),
      f2    = names(f2),
      nb_f1 = length(names(f1)),
      nb_f2 = length(names(f2))
    )

    cat(sprintf("✓ F1:%d F2:%d\n", length(names(f1)), length(names(f2))))
    unlink(tmp)
    gc(verbose = FALSE)

  }, error = function(e) {
    cat(sprintf("✗ ERREUR: %s\n", e$message))
    erreurs <<- c(erreurs, periode)
    unlink(tmp)
    gc(verbose = FALSE)
  })
}

cat(sprintf("\n✓ %d fichiers analysés", length(colonnes_par_periode)))
if (length(erreurs) > 0) cat(sprintf(" | ⚠️  %d erreurs", length(erreurs)))
cat("\n")

# --- ANALYSE CHANGEMENTS FEUILLE 1 ---

cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("CHANGEMENTS FEUILLE 1 (DONNÉES AGENTS)\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

periodes       <- names(colonnes_par_periode)
changements_f1 <- list()

for (i in 1:(length(periodes) - 1)) {
  p1    <- periodes[i]
  p2    <- periodes[i + 1]
  cols1 <- colonnes_par_periode[[p1]]$f1
  cols2 <- colonnes_par_periode[[p2]]$f1

  disparues <- setdiff(cols1, cols2)
  apparues  <- setdiff(cols2, cols1)

  if (length(disparues) > 0 || length(apparues) > 0) {
    cat(sprintf("%s → %s :\n", p1, p2))
    if (length(disparues) > 0) {
      cat(sprintf("  ❌ DISPARUES (%d) :\n", length(disparues)))
      for (col in head(disparues, 10)) cat(sprintf("     • %s\n", col))
      if (length(disparues) > 10) cat(sprintf("     ... et %d autres\n", length(disparues) - 10))
    }
    if (length(apparues) > 0) {
      cat(sprintf("  ✅ APPARUES (%d) :\n", length(apparues)))
      for (col in head(apparues, 10)) cat(sprintf("     • %s\n", col))
      if (length(apparues) > 10) cat(sprintf("     ... et %d autres\n", length(apparues) - 10))
    }
    cat("\n")
    changements_f1[[paste(p1, p2, sep = "_")]] <- list(
      periode_avant = p1, periode_apres = p2,
      disparues = disparues, apparues = apparues
    )
  }
}

if (length(changements_f1) == 0) cat("✓ Aucun changement détecté\n\n")

# --- STATISTIQUES GLOBALES ---

cat(paste(rep("=", 70), collapse = ""), "\n")
cat("STATISTIQUES GLOBALES\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

toutes_cols_f1 <- unique(unlist(lapply(colonnes_par_periode, function(x) x$f1)))
toutes_cols_f2 <- unique(unlist(lapply(colonnes_par_periode, function(x) x$f2)))
tous_codes_f2  <- toutes_cols_f2[grepl("^[0-9]+$", toutes_cols_f2)]

nb_cols_f1 <- sapply(colonnes_par_periode, function(x) x$nb_f1)
nb_cols_f2 <- sapply(colonnes_par_periode, function(x) x$nb_f2)

cat(sprintf("Colonnes F1 uniques (toutes périodes) : %d\n", length(toutes_cols_f1)))
cat(sprintf("Colonnes F2 uniques (toutes périodes) : %d\n", length(toutes_cols_f2)))
cat(sprintf("Codes numériques F2 uniques : %d\n", length(tous_codes_f2)))
cat(sprintf("\nF1 — colonnes / fichier : min=%d, max=%d, moy=%.1f\n",
            min(nb_cols_f1), max(nb_cols_f1), mean(nb_cols_f1)))
cat(sprintf("F2 — colonnes / fichier : min=%d, max=%d, moy=%.1f\n",
            min(nb_cols_f2), max(nb_cols_f2), mean(nb_cols_f2)))

# --- SAUVEGARDE DES RAPPORTS VERS STAGING ---

cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("SAUVEGARDE DES RAPPORTS\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

sauvegarder_rapport <- function(df, nom_fichier) {
  tmp <- tempfile(fileext = ".csv")
  write.csv(df, tmp, row.names = FALSE)
  aws.s3::put_object(
    file      = tmp,
    object    = file.path(PREFIX_SORTIE, nom_fichier),
    bucket    = BUCKET,
    base_url  = endpoint_propre,
    region    = "",
    use_https = FALSE
  )
  unlink(tmp)
  cat(sprintf("✓ %s\n", nom_fichier))
}

# Rapport 1 : colonnes F1 uniques
sauvegarder_rapport(
  data.frame(colonne = toutes_cols_f1),
  "colonnes_f1_uniques.csv"
)

# Rapport 2 : stats par période
sauvegarder_rapport(
  data.frame(periode = periodes, nb_cols_f1 = nb_cols_f1, nb_cols_f2 = nb_cols_f2),
  "stats_par_periode.csv"
)

# Rapport 3 : changements F1
if (length(changements_f1) > 0) {
  lignes <- list()
  for (chg in changements_f1) {
    for (col in chg$disparues)
      lignes[[length(lignes) + 1]] <- list(p_avant = chg$periode_avant, p_apres = chg$periode_apres,
                                           type = "DISPARUE", colonne = col)
    for (col in chg$apparues)
      lignes[[length(lignes) + 1]] <- list(p_avant = chg$periode_avant, p_apres = chg$periode_apres,
                                           type = "APPARUE", colonne = col)
  }
  sauvegarder_rapport(rbindlist(lignes), "changements_f1.csv")
}

cat("\n✓ PRÉ-ANALYSE TERMINÉE\n")
cat(sprintf("Rapports disponibles dans : s3://%s/%s/\n\n", BUCKET, PREFIX_SORTIE))
cat("Prochaine étape : exécuter 02_staging_to_bronze.R\n")
