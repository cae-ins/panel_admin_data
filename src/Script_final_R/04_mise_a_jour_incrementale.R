# ============================================================
# MISE À JOUR INCRÉMENTALE BASE CONSOLIDÉE
# Ajoute les nouveaux mois à la base existante
# ============================================================

rm(list = ls())
gc()

# --- CHARGEMENT PACKAGES ---
cat("Chargement des packages...\n")

packages_requis <- c("data.table", "readxl", "stringi", "openxlsx", "arrow")

for (pkg in packages_requis) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    install.packages(pkg, repos = "https://cloud.r-project.org")
    library(pkg, character.only = TRUE)
  }
}

suppressPackageStartupMessages({
  library(data.table)
  library(readxl)
  library(stringi)
  library(openxlsx)
  library(arrow)
})

# --- CONFIGURATION ---
PROJECT_ROOT <- path.expand("~/consolidation_solde_2015_2025")
DATA_SOURCES <- file.path(PROJECT_ROOT, "01_data_sources/fichiers_solde_mensuels")
ANSTAT_FILE <- file.path(PROJECT_ROOT, "01_data_sources/fichiers_anstat_codes/FICHIER_ANSTAT_CODE_2025.xlsx")
OUTPUT_DIR <- file.path(PROJECT_ROOT, "03_data_output/base_finale")
LOGS_DIR <- file.path(PROJECT_ROOT, "06_logs/execution")
BACKUP_DIR <- file.path(PROJECT_ROOT, "03_data_output/base_finale/backups")

dir.create(BACKUP_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(LOGS_DIR, showWarnings = FALSE, recursive = TRUE)

# --- INITIALISATION LOG ---
log_file <- file.path(LOGS_DIR, paste0("mise_a_jour_incrementale_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".log"))

log_message <- function(msg) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  message <- paste0("[", timestamp, "] ", msg)
  cat(message, "\n")
  cat(message, "\n", file = log_file, append = TRUE)
}

log_message(paste(rep("=", 70), collapse = ""))
log_message("MISE À JOUR INCRÉMENTALE")
log_message(paste(rep("=", 70), collapse = ""))

# ============================================================
# CHARGER LES FONCTIONS UTILITAIRES (MÊME QUE CONSOLIDATION)
# ============================================================

source(file.path(PROJECT_ROOT, "04_scripts/R/fonctions_utilitaires.R"))
# OU copier-coller les fonctions ici

normaliser_pour_matching <- function(texte) {
  if (is.null(texte) || length(texte) == 0) return(NA_character_)
  texte <- as.character(texte)
  texte[is.na(texte)] <- ""
  texte <- trimws(texte)
  texte <- gsub("-", " ", texte, fixed = TRUE)
  texte <- gsub("\\.", " ", texte)
  texte <- gsub("'", "'", texte, fixed = TRUE)
  texte <- gsub("'", "'", texte, fixed = TRUE)
  texte <- gsub("'", " ", texte, fixed = TRUE)
  texte <- gsub(",", " ", texte, fixed = TRUE)
  texte <- gsub(";", " ", texte, fixed = TRUE)
  texte <- gsub(":", " ", texte, fixed = TRUE)
  texte <- gsub("/", " ", texte, fixed = TRUE)
  texte <- gsub("\\(", " ", texte)
  texte <- gsub("\\)", " ", texte)
  texte <- gsub("\\[", " ", texte)
  texte <- gsub("\\]", " ", texte)
  texte <- toupper(texte)
  texte <- stri_trans_general(texte, "Latin-ASCII")
  texte <- gsub("\\s+", " ", texte)
  texte <- trimws(texte)
  texte[texte == ""] <- NA_character_
  return(texte)
}

# [Copier toutes les autres fonctions : detecter_entete, normaliser_nom_colonne, 
#  mapper_colonnes, normaliser_situation, enrichir_par_codes, traiter_fichier]

# ============================================================
# CHARGEMENT TABLES ANSTAT
# ============================================================

log_message("\n[CHARGEMENT] Tables de codes ANSTAT")

charger_table_codes <- function(sheet_name, libelle_col_names, code_name, jointure_col) {
  dt <- as.data.table(read_excel(ANSTAT_FILE, sheet = sheet_name))
  for (lcn in libelle_col_names) {
    if (lcn %in% names(dt)) {
      setnames(dt, lcn, "libelle")
      break
    }
  }
  if (!"libelle" %in% names(dt)) stop(sprintf("Colonne libellé non trouvée dans %s", sheet_name))
  dt[, libelle_norm := normaliser_pour_matching(libelle)]
  list(table = dt, col_code = names(dt)[1], col_jointure_source = jointure_col)
}

dictionnaires_codes <- list()

dictionnaires_codes[["CODE_AFFECTATION"]] <- charger_table_codes(
  "lieu affectation", c("LIBELLÉ_LIEU_AFFECTATION", "LIBELLE_LIEU_AFFECTATION"),
  "CODE_AFFECTATION", "lieu_affectation")

dictionnaires_codes[["CODE_ORGANISME"]] <- charger_table_codes(
  "ORGANISME_OK", c("LIBELLÉ_ORGANISME", "LIBELLE_ORGANISME"),
  "CODE_ORGANISME", "organisme")

dictionnaires_codes[["CODE_POSITION_SOLDE"]] <- charger_table_codes(
  "CODE_SITUATION_SOLDE", c("LIBELLÉ_POSITION_SOLDE", "LIBELLE_SITUATION_SOLDE", "LIBELLÉ_SITUATION_SOLDE"),
  "CODE_POSITION_SOLDE", "situation_brute")

config_emploi <- charger_table_codes(
  "HISTORIQUE_ECHELLES_CORPS", c("LIBELLÉ_EMPLOI", "LIBELLE_EMPLOI"),
  "CODE_EMPLOI", "emploi")
config_emploi$cols_supplementaires <- c("Code_CITP", "Metier_CITP")
dictionnaires_codes[["CODE_EMPLOI"]] <- config_emploi

dictionnaires_codes[["CODE_SERVICE"]] <- charger_table_codes(
  "SERVICE", c("LIBELLÉ_SERVICE", "LIBELLE_SERVICE"),
  "CODE_SERVICE", "service")

dictionnaires_codes[["CODE_FONCTION"]] <- charger_table_codes(
  "FONCTION", c("LIBELLÉ_FONCTION", "LIBELLE_FONCTION"),
  "CODE_FONCTION", "fonction")

dictionnaires_codes[["CODE_GRADE"]] <- charger_table_codes(
  "GRADE", c("LIBELLÉ_GRADE", "LIBELLE_GRADE"),
  "CODE_GRADE", "grade")

dictionnaires_codes[["CODE_POSTE"]] <- charger_table_codes(
  "LIBELLE POSTE", c("LIBELLÉ_POSTE", "LIBELLE_POSTE"),
  "CODE_POSTE", "poste")

log_message("  ✓ Toutes les tables chargées")

# ============================================================
# ÉTAPE 1 : CHARGER LA BASE EXISTANTE
# ============================================================

log_message("\n" %s+% paste(rep("=", 70), collapse = ""))
log_message("ÉTAPE 1 : CHARGEMENT BASE EXISTANTE")
log_message(paste(rep("=", 70), collapse = ""))

fichier_base_existante <- file.path(OUTPUT_DIR, "base_consolidee_2015_2025.parquet")

if (!file.exists(fichier_base_existante)) {
  stop("❌ Base existante non trouvée. Lancez d'abord la consolidation complète.")
}

log_message("  Lecture base existante...")
debut <- Sys.time()
base_existante <- as.data.table(read_parquet(fichier_base_existante))
temps <- as.numeric(difftime(Sys.time(), debut, units = "secs"))

log_message(sprintf("  ✓ Base chargée : %s lignes × %d colonnes (%.1f sec)", 
                   format(nrow(base_existante), big.mark = ","), 
                   ncol(base_existante), 
                   temps))

# Identifier la dernière période
derniere_periode <- max(base_existante$periode)
log_message(sprintf("  Dernière période dans la base : %s", derniere_periode))

# ============================================================
# ÉTAPE 2 : IDENTIFIER LES NOUVEAUX FICHIERS
# ============================================================

log_message("\n" %s+% paste(rep("=", 70), collapse = ""))
log_message("ÉTAPE 2 : IDENTIFICATION NOUVEAUX FICHIERS")
log_message(paste(rep("=", 70), collapse = ""))

# Lister tous les fichiers disponibles
tous_fichiers <- list.files(DATA_SOURCES, pattern = "\\.xlsx$", full.names = TRUE, recursive = TRUE)
tous_fichiers <- sort(tous_fichiers)

# Extraire les périodes
periodes_disponibles <- gsub("\\.xlsx$", "", basename(tous_fichiers))

# Identifier les nouveaux (postérieurs à la dernière période)
nouveaux_fichiers <- tous_fichiers[periodes_disponibles > derniere_periode]

if (length(nouveaux_fichiers) == 0) {
  log_message("  ℹ️  Aucun nouveau fichier détecté")
  log_message("\n✓ Base déjà à jour")
  quit(save = "no")
}

log_message(sprintf("  ✓ %d nouveaux fichiers détectés", length(nouveaux_fichiers)))

for (fichier in nouveaux_fichiers) {
  periode <- gsub("\\.xlsx$", "", basename(fichier))
  log_message(sprintf("    • %s", periode))
}

# ============================================================
# ÉTAPE 3 : TRAITER LES NOUVEAUX FICHIERS
# ============================================================

log_message("\n" %s+% paste(rep("=", 70), collapse = ""))
log_message("ÉTAPE 3 : TRAITEMENT NOUVEAUX FICHIERS")
log_message(paste(rep("=", 70), collapse = ""))

nouveaux_resultats <- list()
stats_nouveaux <- list()

for (fichier_path in nouveaux_fichiers) {
  periode <- gsub("\\.xlsx$", "", basename(fichier_path))
  
  log_message(sprintf("\n  [%s]", periode))
  
  resultat <- traiter_fichier(fichier_path, periode)
  
  if (!is.null(resultat)) {
    nouveaux_resultats[[periode]] <- resultat$feuille1
    stats_nouveaux[[periode]] <- resultat$stats
    
    log_message(sprintf("    ✓ %s lignes × %d colonnes", 
                       format(nrow(resultat$feuille1), big.mark = ","), 
                       ncol(resultat$feuille1)))
    
    for (code in c("CODE_ORGANISME", "CODE_GRADE", "CODE_EMPLOI")) {
      if (!is.null(resultat$stats$stats_codes[[code]])) {
        taux <- resultat$stats$stats_codes[[code]]$taux
        log_message(sprintf("      %s : %.1f%%", code, taux))
      }
    }
  } else {
    log_message(sprintf("    ✗ Échec traitement"))
  }
}

if (length(nouveaux_resultats) == 0) {
  log_message("\n✗ Aucun nouveau fichier traité avec succès")
  quit(save = "no")
}

# ============================================================
# ÉTAPE 4 : HARMONISATION COLONNES
# ============================================================

log_message("\n" %s+% paste(rep("=", 70), collapse = ""))
log_message("ÉTAPE 4 : HARMONISATION COLONNES")
log_message(paste(rep("=", 70), collapse = ""))

# Colonnes de la base existante
colonnes_existantes <- names(base_existante)
log_message(sprintf("  Colonnes base existante : %d", length(colonnes_existantes)))

# Colonnes des nouveaux fichiers
colonnes_nouvelles <- unique(unlist(lapply(nouveaux_resultats, names)))
log_message(sprintf("  Colonnes nouveaux fichiers : %d", length(colonnes_nouvelles)))

# Colonnes totales (union)
toutes_colonnes <- unique(c(colonnes_existantes, colonnes_nouvelles))
log_message(sprintf("  Colonnes totales après fusion : %d", length(toutes_colonnes)))

# Colonnes ajoutées
colonnes_ajoutees <- setdiff(colonnes_nouvelles, colonnes_existantes)
if (length(colonnes_ajoutees) > 0) {
  log_message(sprintf("  ⚠️  %d nouvelles colonnes détectées :", length(colonnes_ajoutees)))
  for (col in head(colonnes_ajoutees, 10)) {
    log_message(sprintf("    • %s", col))
  }
  if (length(colonnes_ajoutees) > 10) {
    log_message(sprintf("    ... et %d autres", length(colonnes_ajoutees) - 10))
  }
}

# Ajouter colonnes manquantes à la base existante
colonnes_manquantes_base <- setdiff(toutes_colonnes, colonnes_existantes)
if (length(colonnes_manquantes_base) > 0) {
  log_message(sprintf("  Ajout %d colonnes à la base existante...", length(colonnes_manquantes_base)))
  for (col in colonnes_manquantes_base) {
    base_existante[, (col) := NA]
  }
}

# Ajouter colonnes manquantes aux nouveaux fichiers
for (periode in names(nouveaux_resultats)) {
  dt <- nouveaux_resultats[[periode]]
  colonnes_manquantes_nouveau <- setdiff(toutes_colonnes, names(dt))
  
  if (length(colonnes_manquantes_nouveau) > 0) {
    for (col in colonnes_manquantes_nouveau) {
      dt[, (col) := NA]
    }
  }
  
  # Réordonner colonnes
  setcolorder(dt, toutes_colonnes)
  nouveaux_resultats[[periode]] <- dt
}

# Réordonner colonnes base existante
setcolorder(base_existante, toutes_colonnes)

log_message("  ✓ Harmonisation terminée")

# ============================================================
# ÉTAPE 5 : CONSOLIDATION
# ============================================================

log_message("\n" %s+% paste(rep("=", 70), collapse = ""))
log_message("ÉTAPE 5 : CONSOLIDATION")
log_message(paste(rep("=", 70), collapse = ""))

# Empiler nouveaux fichiers
log_message("  Consolidation nouveaux fichiers...")
nouveaux_empiles <- rbindlist(nouveaux_resultats, use.names = TRUE, fill = TRUE)
log_message(sprintf("  ✓ Nouveaux : %s lignes × %d colonnes", 
                   format(nrow(nouveaux_empiles), big.mark = ","), 
                   ncol(nouveaux_empiles)))

# Fusionner avec base existante
log_message("  Fusion avec base existante...")
base_mise_a_jour <- rbindlist(list(base_existante, nouveaux_empiles), use.names = TRUE, fill = TRUE)
log_message(sprintf("  ✓ Base mise à jour : %s lignes × %d colonnes", 
                   format(nrow(base_mise_a_jour), big.mark = ","), 
                   ncol(base_mise_a_jour)))

# Statistiques
log_message("\n  Résumé :")
log_message(sprintf("    Lignes anciennes : %s", format(nrow(base_existante), big.mark = ",")))
log_message(sprintf("    Lignes nouvelles : %s", format(nrow(nouveaux_empiles), big.mark = ",")))
log_message(sprintf("    Total : %s", format(nrow(base_mise_a_jour), big.mark = ",")))
log_message(sprintf("    Périodes : %s à %s", min(base_mise_a_jour$periode), max(base_mise_a_jour$periode)))

# ============================================================
# ÉTAPE 6 : BACKUP DE L'ANCIENNE BASE
# ============================================================

log_message("\n" %s+% paste(rep("=", 70), collapse = ""))
log_message("ÉTAPE 6 : BACKUP ANCIENNE BASE")
log_message(paste(rep("=", 70), collapse = ""))

timestamp_backup <- format(Sys.time(), "%Y%m%d_%H%M%S")
fichier_backup <- file.path(BACKUP_DIR, paste0("base_consolidee_backup_", timestamp_backup, ".parquet"))

log_message(sprintf("  Sauvegarde backup : %s", basename(fichier_backup)))
write_parquet(base_existante, fichier_backup, compression = "snappy")
taille_backup <- file.info(fichier_backup)$size / 1024^2
log_message(sprintf("  ✓ Backup sauvegardé (%.0f MB)", taille_backup))

# ============================================================
# ÉTAPE 7 : SAUVEGARDE BASE MISE À JOUR
# ============================================================

log_message("\n" %s+% paste(rep("=", 70), collapse = ""))
log_message("ÉTAPE 7 : SAUVEGARDE BASE MISE À JOUR")
log_message(paste(rep("=", 70), collapse = ""))

# Parquet
fichier_parquet <- file.path(OUTPUT_DIR, "base_consolidee_2015_2025.parquet")
log_message("  Sauvegarde Parquet...")
write_parquet(base_mise_a_jour, fichier_parquet, compression = "snappy")
taille_parquet <- file.info(fichier_parquet)$size / 1024^2
log_message(sprintf("  ✓ Parquet : %.0f MB", taille_parquet))

# CSV
fichier_csv <- file.path(OUTPUT_DIR, "base_consolidee_2015_2025.csv")
log_message("  Sauvegarde CSV...")
fwrite(base_mise_a_jour, fichier_csv)
taille_csv <- file.info(fichier_csv)$size / 1024^2
log_message(sprintf("  ✓ CSV : %.0f MB", taille_csv))

# Excel (échantillon)
if (nrow(base_mise_a_jour) > 100000) {
  echantillon <- head(base_mise_a_jour, 100000)
} else {
  echantillon <- base_mise_a_jour
}

fichier_sample <- file.path(OUTPUT_DIR, "echantillon_excel.xlsx")
log_message("  Sauvegarde Excel...")
write.xlsx(echantillon, fichier_sample)
taille_sample <- file.info(fichier_sample)$size / 1024^2
log_message(sprintf("  ✓ Excel : %.0f MB (%s lignes)", 
                   taille_sample, 
                   format(nrow(echantillon), big.mark = ",")))

# Métadonnées
fichier_metadata <- file.path(OUTPUT_DIR, "METADATA.txt")

metadata_content <- sprintf("
=================================================================
MÉTADONNÉES BASE CONSOLIDÉE 2015-2025
=================================================================

Date dernière mise à jour : %s
Script : 05_mise_a_jour_incrementale.R

--- DIMENSIONS ---
Lignes totales : %s
Colonnes : %d
Taille mémoire : %.2f GB

--- PÉRIODES ---
Première période : %s
Dernière période : %s
Nombre de périodes : %d

--- AGENTS ---
Agents uniques : %s

--- DERNIÈRE MISE À JOUR ---
Fichiers ajoutés : %d
Lignes ajoutées : %s
Périodes ajoutées : %s

--- FORMATS DISPONIBLES ---
- base_consolidee_2015_2025.parquet  (%.0f MB) - FORMAT PRINCIPAL
- base_consolidee_2015_2025.csv      (%.0f MB) - BACKUP
- echantillon_excel.xlsx             (%.0f MB) - CONSULTATION

--- BACKUPS ---
Dernier backup : %s

=================================================================
",
  format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
  format(nrow(base_mise_a_jour), big.mark = ","),
  ncol(base_mise_a_jour),
  object.size(base_mise_a_jour) / 1024^3,
  min(base_mise_a_jour$periode),
  max(base_mise_a_jour$periode),
  length(unique(base_mise_a_jour$periode)),
  if ("matricule" %in% names(base_mise_a_jour)) format(uniqueN(base_mise_a_jour$matricule), big.mark = ",") else "N/A",
  length(nouveaux_fichiers),
  format(nrow(nouveaux_empiles), big.mark = ","),
  paste(names(nouveaux_resultats), collapse = ", "),
  taille_parquet,
  taille_csv,
  taille_sample,
  basename(fichier_backup)
)


cat(metadata_content, file = fichier_metadata)
log_message("  ✓ Métadonnées mises à jour")

# ============================================================
# RAPPORT FINAL
# ============================================================

log_message("\n" %s+% paste(rep("=", 70), collapse = ""))
log_message("RAPPORT FINAL")
log_message(paste(rep("=", 70), collapse = ""))

log_message(sprintf("Nouveaux fichiers traités : %d", length(nouveaux_resultats)))
log_message(sprintf("Lignes ajoutées : %s", format(nrow(nouveaux_empiles), big.mark = ",")))
log_message(sprintf("Base finale : %s lignes × %d colonnes", 
                   format(nrow(base_mise_a_jour), big.mark = ","), 
                   ncol(base_mise_a_jour)))
log_message(sprintf("Périodes : %s à %s", 
                   min(base_mise_a_jour$periode), 
                   max(base_mise_a_jour$periode)))

log_message("\n✓✓✓ MISE À JOUR TERMINÉE AVEC SUCCÈS ✓✓✓")

cat("\n", paste(rep("=", 70), collapse = ""), "\n")
cat("✓ SCRIPT TERMINÉ\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")
cat(sprintf("📋 Log : %s\n", log_file))
cat(sprintf("📊 Base mise à jour : %s\n", OUTPUT_DIR))
cat(sprintf("💾 Backup : %s\n", fichier_backup))
cat("\n")