# ============================================================
# PRÉ-ANALYSE : ÉVOLUTION DES COLONNES 2015-2025
# Objectif : Identifier tous les changements de colonnes
# ============================================================

rm(list = ls())
gc()

library(readxl)
library(data.table)

# --- CONFIGURATION ---
PROJECT_ROOT <- path.expand("~/consolidation_solde_2015_2025")
DATA_SOURCES <- file.path(PROJECT_ROOT, "01_data_sources/fichiers_solde_mensuels")
OUTPUT_DIR <- file.path(PROJECT_ROOT, "03_data_output/base_finale/pre_analyse")

dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

cat(paste(rep("=", 70), collapse = ""), "\n")
cat("PRÉ-ANALYSE : COLONNES 2015-2025\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

# --- ÉTAPE 1 : LISTER TOUS LES FICHIERS ---
cat("Recherche des fichiers...\n")

fichiers <- list.files(DATA_SOURCES, pattern = "\\.xlsx$", full.names = TRUE, recursive = TRUE)
fichiers <- sort(fichiers)

cat(sprintf("✓ %d fichiers trouvés\n\n", length(fichiers)))

# --- ÉTAPE 2 : EXTRAIRE NOMS COLONNES ---
cat("Extraction des noms de colonnes...\n\n")

colonnes_par_periode <- list()
erreurs <- c()

for (fichier in fichiers) {
  periode <- gsub("\\.xlsx$", "", basename(fichier))
  
  cat(sprintf("  %s... ", periode))
  
  tryCatch({
    # Feuille 1
    f1 <- read_excel(fichier, sheet = 1, n_max = 0)
    
    # Feuille 2
    f2 <- read_excel(fichier, sheet = 2, n_max = 0)
    
    colonnes_par_periode[[periode]] <- list(
      f1 = names(f1),
      f2 = names(f2),
      nb_f1 = length(names(f1)),
      nb_f2 = length(names(f2))
    )
    
    cat(sprintf("✓ F1:%d F2:%d\n", length(names(f1)), length(names(f2))))
    
  }, error = function(e) {
    cat(sprintf("✗ ERREUR: %s\n", e$message))
    erreurs <- c(erreurs, periode)
  })
}

cat(sprintf("\n✓ %d fichiers analysés\n", length(colonnes_par_periode)))
if (length(erreurs) > 0) {
  cat(sprintf("⚠️  %d erreurs\n", length(erreurs)))
}

# --- ÉTAPE 3 : ANALYSE CHANGEMENTS FEUILLE 1 ---
cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("CHANGEMENTS FEUILLE 1 (ANNÉE PAR ANNÉE)\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

periodes <- names(colonnes_par_periode)
changements_f1 <- list()

for (i in 1:(length(periodes)-1)) {
  p1 <- periodes[i]
  p2 <- periodes[i+1]
  
  cols1 <- colonnes_par_periode[[p1]]$f1
  cols2 <- colonnes_par_periode[[p2]]$f1
  
  disparues <- setdiff(cols1, cols2)
  apparues <- setdiff(cols2, cols1)
  
  if (length(disparues) > 0 || length(apparues) > 0) {
    cat(sprintf("%s → %s :\n", p1, p2))
    
    if (length(disparues) > 0) {
      cat(sprintf("  ❌ DISPARUES (%d) :\n", length(disparues)))
      for (col in head(disparues, 10)) {
        cat(sprintf("     • %s\n", col))
      }
      if (length(disparues) > 10) cat(sprintf("     ... et %d autres\n", length(disparues) - 10))
    }
    
    if (length(apparues) > 0) {
      cat(sprintf("  ✅ APPARUES (%d) :\n", length(apparues)))
      for (col in head(apparues, 10)) {
        cat(sprintf("     • %s\n", col))
      }
      if (length(apparues) > 10) cat(sprintf("     ... et %d autres\n", length(apparues) - 10))
    }
    
    cat("\n")
    
    changements_f1[[paste(p1, p2, sep = "_")]] <- list(
      periode_avant = p1,
      periode_apres = p2,
      disparues = disparues,
      apparues = apparues
    )
  }
}

if (length(changements_f1) == 0) {
  cat("✓ Aucun changement détecté\n\n")
}

# --- ÉTAPE 4 : ANALYSE CHANGEMENTS FEUILLE 2 ---
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("CHANGEMENTS FEUILLE 2 (CODES NUMÉRIQUES)\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

changements_f2 <- list()

for (i in 1:(length(periodes)-1)) {
  p1 <- periodes[i]
  p2 <- periodes[i+1]
  
  cols1 <- colonnes_par_periode[[p1]]$f2
  cols2 <- colonnes_par_periode[[p2]]$f2
  
  # Extraire codes numériques
  codes1 <- cols1[grepl("^[0-9]+$", cols1)]
  codes2 <- cols2[grepl("^[0-9]+$", cols2)]
  
  disparues <- setdiff(codes1, codes2)
  apparues <- setdiff(codes2, codes1)
  
  if (length(disparues) > 0 || length(apparues) > 0) {
    cat(sprintf("%s → %s :\n", p1, p2))
    
    if (length(disparues) > 0) {
      cat(sprintf("  ❌ CODES DISPARUS (%d) : %s\n", 
                 length(disparues), 
                 paste(head(disparues, 20), collapse = ", ")))
    }
    
    if (length(apparues) > 0) {
      cat(sprintf("  ✅ CODES APPARUS (%d) : %s\n", 
                 length(apparues), 
                 paste(head(apparues, 20), collapse = ", ")))
    }
    
    cat("\n")
    
    changements_f2[[paste(p1, p2, sep = "_")]] <- list(
      periode_avant = p1,
      periode_apres = p2,
      disparues = disparues,
      apparues = apparues
    )
  }
}

if (length(changements_f2) == 0) {
  cat("✓ Aucun changement majeur détecté\n\n")
}

# --- ÉTAPE 5 : STATISTIQUES GLOBALES ---
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("STATISTIQUES GLOBALES\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

# Colonnes F1 uniques
toutes_cols_f1 <- unique(unlist(lapply(colonnes_par_periode, function(x) x$f1)))
cat(sprintf("Colonnes F1 uniques (toutes périodes) : %d\n", length(toutes_cols_f1)))

# Colonnes F2 uniques
toutes_cols_f2 <- unique(unlist(lapply(colonnes_par_periode, function(x) x$f2)))
cat(sprintf("Colonnes F2 uniques (toutes périodes) : %d\n", length(toutes_cols_f2)))

# Codes numériques F2
tous_codes_f2 <- toutes_cols_f2[grepl("^[0-9]+$", toutes_cols_f2)]
cat(sprintf("Codes numériques F2 uniques : %d\n", length(tous_codes_f2)))
cat(sprintf("  Min : %s\n", min(tous_codes_f2)))
cat(sprintf("  Max : %s\n", max(tous_codes_f2)))

# Évolution nombre colonnes
nb_cols_f1 <- sapply(colonnes_par_periode, function(x) x$nb_f1)
nb_cols_f2 <- sapply(colonnes_par_periode, function(x) x$nb_f2)

cat(sprintf("\nÉvolution nombre colonnes F1 :\n"))
cat(sprintf("  Min : %d\n", min(nb_cols_f1)))
cat(sprintf("  Max : %d\n", max(nb_cols_f1)))
cat(sprintf("  Moyenne : %.1f\n", mean(nb_cols_f1)))

cat(sprintf("\nÉvolution nombre colonnes F2 :\n"))
cat(sprintf("  Min : %d\n", min(nb_cols_f2)))
cat(sprintf("  Max : %d\n", max(nb_cols_f2)))
cat(sprintf("  Moyenne : %.1f\n", mean(nb_cols_f2)))

# --- ÉTAPE 6 : SAUVEGARDE RAPPORTS ---
cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("SAUVEGARDE RAPPORTS\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

# Rapport 1 : Liste complète colonnes F1
rapport_f1 <- data.frame(
  colonne = toutes_cols_f1,
  stringsAsFactors = FALSE
)
fichier_f1 <- file.path(OUTPUT_DIR, "colonnes_f1_uniques.csv")
write.csv(rapport_f1, fichier_f1, row.names = FALSE)
cat(sprintf("✓ %s\n", basename(fichier_f1)))

# Rapport 2 : Liste codes F2
rapport_f2 <- data.frame(
  code = tous_codes_f2,
  stringsAsFactors = FALSE
)
fichier_f2 <- file.path(OUTPUT_DIR, "codes_f2_uniques.csv")
write.csv(rapport_f2, fichier_f2, row.names = FALSE)
cat(sprintf("✓ %s\n", basename(fichier_f2)))

# Rapport 3 : Changements F1 par période
if (length(changements_f1) > 0) {
  lignes <- list()
  for (chg_name in names(changements_f1)) {
    chg <- changements_f1[[chg_name]]
    
    if (length(chg$disparues) > 0) {
      for (col in chg$disparues) {
        lignes[[length(lignes) + 1]] <- list(
          periode_avant = chg$periode_avant,
          periode_apres = chg$periode_apres,
          type = "DISPARUE",
          colonne = col
        )
      }
    }
    
    if (length(chg$apparues) > 0) {
      for (col in chg$apparues) {
        lignes[[length(lignes) + 1]] <- list(
          periode_avant = chg$periode_avant,
          periode_apres = chg$periode_apres,
          type = "APPARUE",
          colonne = col
        )
      }
    }
  }
  
  rapport_chg_f1 <- rbindlist(lignes)
  fichier_chg_f1 <- file.path(OUTPUT_DIR, "changements_f1.csv")
  write.csv(rapport_chg_f1, fichier_chg_f1, row.names = FALSE)
  cat(sprintf("✓ %s\n", basename(fichier_chg_f1)))
}

# Rapport 4 : Statistiques par période
stats_par_periode <- data.frame(
  periode = periodes,
  nb_cols_f1 = nb_cols_f1,
  nb_cols_f2 = nb_cols_f2,
  stringsAsFactors = FALSE
)
fichier_stats <- file.path(OUTPUT_DIR, "stats_par_periode.csv")
write.csv(stats_par_periode, fichier_stats, row.names = FALSE)
cat(sprintf("✓ %s\n", basename(fichier_stats)))

# --- FIN ---
cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("✓ PRÉ-ANALYSE TERMINÉE\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat(sprintf("\nRapports disponibles dans : %s\n", OUTPUT_DIR))