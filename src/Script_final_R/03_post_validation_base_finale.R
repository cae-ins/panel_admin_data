# ============================================================
# POST-VALIDATION : CONTRÔLE QUALITÉ BASE FINALE (VERSION OPTIMISÉE)
# ============================================================

rm(list = ls())
gc()

library(data.table)
library(stringi)
library(arrow)

# --- CONFIGURATION ---
PROJECT_ROOT <- path.expand("~/consolidation_solde_2015_2025")
BASE_FILE_PARQUET <- file.path(PROJECT_ROOT, "03_data_output/base_finale/base_consolidee_2015_2025.parquet")
BASE_FILE_CSV <- file.path(PROJECT_ROOT, "03_data_output/base_finale/base_consolidee_2015_2025.csv")
OUTPUT_DIR <- file.path(PROJECT_ROOT, "03_data_output/base_finale/validation")

dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

cat(paste(rep("=", 70), collapse = ""), "\n")
cat("POST-VALIDATION : CONTRÔLE QUALITÉ (VERSION OPTIMISÉE)\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

# --- CHARGEMENT BASE ---
cat("Chargement de la base...\n")

if (file.exists(BASE_FILE_PARQUET)) {
  cat("  Format : PARQUET\n")
  debut <- Sys.time()
  base <- as.data.table(read_parquet(BASE_FILE_PARQUET))
  temps <- as.numeric(difftime(Sys.time(), debut, units = "secs"))
  cat(sprintf("✓ Base chargée : %s lignes × %d colonnes (%.1f sec)\n\n", 
              format(nrow(base), big.mark = ","), ncol(base), temps))
  
} else if (file.exists(BASE_FILE_CSV)) {
  cat("  Format : CSV (fallback)\n")
  debut <- Sys.time()
  base <- fread(BASE_FILE_CSV)
  temps <- as.numeric(difftime(Sys.time(), debut, units = "secs"))
  cat(sprintf("✓ Base chargée : %s lignes × %d colonnes (%.1f sec)\n\n", 
              format(nrow(base), big.mark = ","), ncol(base), temps))
  
} else {
  stop("Aucun fichier de base trouvé (ni Parquet ni CSV)")
}

# --- CONTRÔLE 1 : COLONNES AVEC NA SUSPECTS (VERSION OPTIMISÉE) ---
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("CONTRÔLE 1 : COLONNES AVEC VARIATIONS NA (OPTIMISÉ)\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

colonnes_a_tester <- setdiff(names(base), c("periode", "fichier_source"))
cat(sprintf("  Analyse de %d colonnes...\n", length(colonnes_a_tester)))

# Ajouter colonne annee temporaire
base[, annee := substr(periode, 1, 4)]

# Calculer % NA pour TOUTES les colonnes en UNE SEULE passe
debut <- Sys.time()
resultat_na <- base[, lapply(.SD, function(x) sum(is.na(x)) / .N * 100), 
                    by = annee, 
                    .SDcols = colonnes_a_tester]
temps <- as.numeric(difftime(Sys.time(), debut, units = "secs"))
cat(sprintf("  ✓ Analyse terminée en %.1f secondes\n", temps))

# Identifier colonnes suspectes
colonnes_suspects <- list()
for (col in colonnes_a_tester) {
  pct_na <- resultat_na[[col]]
  if (max(pct_na, na.rm = TRUE) > 95 && min(pct_na, na.rm = TRUE) < 5) {
    colonnes_suspects[[col]] <- data.frame(
      annee = resultat_na$annee,
      pct_na = pct_na
    )
  }
}

if (length(colonnes_suspects) > 0) {
  cat(sprintf("\n⚠️  %d colonnes suspectes détectées\n\n", length(colonnes_suspects)))
  
  for (col_name in head(names(colonnes_suspects), 10)) {
    stats <- colonnes_suspects[[col_name]]
    annees_pleines <- stats[stats$pct_na < 5, ]$annee
    annees_vides <- stats[stats$pct_na > 95, ]$annee
    
    cat(sprintf("• %s :\n", col_name))
    if (length(annees_pleines) > 0) {
      cat(sprintf("  Présente : %s\n", paste(annees_pleines, collapse = ", ")))
    }
    if (length(annees_vides) > 0) {
      cat(sprintf("  Absente  : %s\n", paste(annees_vides, collapse = ", ")))
    }
    cat("\n")
  }
  
  if (length(colonnes_suspects) > 10) {
    cat(sprintf("... et %d autres (voir CSV)\n\n", length(colonnes_suspects) - 10))
  }
  
  # Sauvegarder
  lignes <- list()
  for (col_name in names(colonnes_suspects)) {
    stats <- colonnes_suspects[[col_name]]
    for (i in 1:nrow(stats)) {
      lignes[[length(lignes) + 1]] <- list(
        colonne = col_name,
        annee = stats$annee[i],
        pct_na = stats$pct_na[i]
      )
    }
  }
  
  rapport <- rbindlist(lignes)
  fichier <- file.path(OUTPUT_DIR, "colonnes_suspects_changement_nom.csv")
  write.csv(rapport, fichier, row.names = FALSE)
  cat(sprintf("✓ Rapport : %s\n\n", basename(fichier)))
  
} else {
  cat("\n✓ Aucune colonne suspecte\n\n")
}

base[, annee := NULL]

# --- CONTRÔLE 2 : COMPLÉTUDE (SIMPLIFIÉ) ---
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("CONTRÔLE 2 : COMPLÉTUDE COLONNES CLÉS\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

colonnes_cles <- c("matricule", "nom", "montant_brut", "montant_net", 
                   "CODE_ORGANISME", "CODE_GRADE", "CODE_EMPLOI")
colonnes_cles <- intersect(colonnes_cles, names(base))

cat("  Résumé global :\n")
for (col in colonnes_cles) {
  pct <- 100 * (1 - sum(is.na(base[[col]])) / nrow(base))
  cat(sprintf("    %s : %.1f%% complet\n", col, pct))
}

cat("\n")

# --- CONTRÔLE 3 : DOUBLONS ---
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("CONTRÔLE 3 : DOUBLONS MATRICULE × PÉRIODE\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

if ("matricule" %in% names(base)) {
  doublons <- base[, .N, by = .(matricule, periode)][N > 1]
  
  if (nrow(doublons) > 0) {
    cat(sprintf("⚠️  %s doublons\n", format(nrow(doublons), big.mark = ",")))
    
    fichier <- file.path(OUTPUT_DIR, "doublons_matricule_periode.csv")
    write.csv(doublons, fichier, row.names = FALSE)
    cat(sprintf("✓ Liste : %s\n", basename(fichier)))
  } else {
    cat("✓ Aucun doublon\n")
  }
}

cat("\n")

# --- CONTRÔLE 4 : MONTANTS ---
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("CONTRÔLE 4 : COHÉRENCE MONTANTS\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

if (all(c("montant_brut", "montant_net") %in% names(base))) {
  incoherents <- base[!is.na(montant_brut) & !is.na(montant_net) & 
                       montant_net > montant_brut]
  
  if (nrow(incoherents) > 0) {
    cat(sprintf("⚠️  Net > Brut : %s (%.2f%%)\n", 
               format(nrow(incoherents), big.mark = ","),
               100 * nrow(incoherents) / nrow(base)))
  } else {
    cat("✓ Tous les montants cohérents\n")
  }
}

cat("\n")

# --- CONTRÔLE 5 : DISTRIBUTION SITUATIONS ---
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("CONTRÔLE 5 : DISTRIBUTION SITUATIONS\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

if ("situation_normalisee" %in% names(base)) {
  dist <- base[, .N, by = situation_normalisee]
  for (i in 1:nrow(dist)) {
    pct <- 100 * dist$N[i] / nrow(base)
    cat(sprintf("  %s : %s (%.1f%%)\n", 
               dist$situation_normalisee[i],
               format(dist$N[i], big.mark = ","),
               pct))
  }
}

cat("\n")

# --- RAPPORT FINAL ---
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("RÉSUMÉ VALIDATION\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

cat(sprintf("Base : %s lignes × %d colonnes\n", 
            format(nrow(base), big.mark = ","), ncol(base)))
cat(sprintf("Périodes : %s à %s\n", min(base$periode), max(base$periode)))

if ("matricule" %in% names(base)) {
  cat(sprintf("Agents uniques : %s\n", format(uniqueN(base$matricule), big.mark = ",")))
}

cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("✓ VALIDATION TERMINÉE\n")
cat(paste(rep("=", 70), collapse = ""), "\n")