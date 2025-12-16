install.packages(c("arrow", "data.table", "writexl", "openxlsx", "dplyr", "tidyr", "furrr", "future", "tictoc"))

# ==============================================================================
# CONSTRUCTION DES INDICATEURS DE SALAIRE - VERSION R OPTIMISÉE (PAR ANNÉE)
# ==============================================================================
# ==============================================================================

library(dplyr)
library(tidyr)
library(data.table)
library(writexl)
library(openxlsx)
library(furrr)
library(future)
library(tictoc)
library(arrow)

# ==============================================================================
# DICTIONNAIRE DES MOIS
# ==============================================================================

MOIS_FR <- c(
  `1` = "Janvier", `2` = "Février", `3` = "Mars", `4` = "Avril",
  `5` = "Mai", `6` = "Juin", `7` = "Juillet", `8` = "Août",
  `9` = "Septembre", `10` = "Octobre", `11` = "Novembre", `12` = "Décembre"
)

# ==============================================================================
# FONCTION DE CHARGEMENT DEPUIS FICHIER LOCAL
# ==============================================================================

charger_panel_solde <- function(
    file_path,
    verbose = TRUE
) {
  
  if (verbose) cat(sprintf("📦 Chargement des données depuis: %s\n", file_path))
  
  # Vérifier l'existence du fichier
  if (!file.exists(file_path)) {
    stop(sprintf("❌ Fichier introuvable: %s", file_path))
  }
  
  tic("Chargement")
  
  tryCatch({
    # Détecter l'extension du fichier
    ext <- tolower(tools::file_ext(file_path))
    
    if (ext == "parquet") {
      # Lecture optimisée avec Arrow
      df <- arrow::read_parquet(file_path)
      
    } else if (ext %in% c("csv", "txt")) {
      # Lecture CSV avec data.table (ultra-rapide)
      df <- data.table::fread(file_path, encoding = "UTF-8")
      
    } else if (ext %in% c("rds", "rdata")) {
      # Lecture de fichiers R natifs
      df <- readRDS(file_path)
      
    } else {
      stop(sprintf("❌ Extension non supportée: %s", ext))
    }
    
    if (verbose) {
      cat(sprintf("✓ Données chargées: %s observations × %s variables\n", 
                  format(nrow(df), big.mark = ","), 
                  ncol(df)))
      
      # Taille en mémoire
      size_mb <- as.numeric(object.size(df)) / 1024^2
      cat(sprintf("✓ Taille en mémoire: %.1f MB\n", size_mb))
      
      if ("MATRICULE" %in% names(df)) {
        cat(sprintf("✓ Matricules uniques: %s\n", 
                    format(length(unique(df$MATRICULE)), big.mark = ",")))
      }
      
      if ("DATE_COLLECTE" %in% names(df)) {
        date_min <- min(df$DATE_COLLECTE, na.rm = TRUE)
        date_max <- max(df$DATE_COLLECTE, na.rm = TRUE)
        cat(sprintf("✓ Période: %s → %s\n", date_min, date_max))
      }
      
      if ("ANNEE_COLLECTE" %in% names(df) | "YEAR" %in% names(df)) {
        annee_col <- if ("ANNEE_COLLECTE" %in% names(df)) "ANNEE_COLLECTE" else "YEAR"
        annees <- sort(unique(df[[annee_col]][!is.na(df[[annee_col]])]))
        cat(sprintf("✓ Années disponibles: %s\n", paste(annees, collapse = ", ")))
      }
    }
    
    toc()
    return(df)
    
  }, error = function(e) {
    cat(sprintf("❌ Erreur lors du chargement: %s\n", e$message))
    return(NULL)
  })
}

# ==============================================================================
# FONCTION PRINCIPALE AVEC OPTIONS DE PERFORMANCE
# ==============================================================================

build_indicateurs_salaire_par_annee <- function(
    panel,
    output_dir = "indicateurs_annuels",
    prefix = "indicateurs_salaire",
    generate_global = TRUE,
    use_data_table = TRUE,
    use_parallel = FALSE,
    n_cores = parallel::detectCores() - 1,
    use_openxlsx = FALSE,
    verbose = TRUE
) {
  
  if (verbose) cat("\n🚀 Démarrage du calcul des indicateurs de salaire PAR ANNÉE...\n")
  tic("Temps total")
  
  # Créer le dossier de sortie s'il n'existe pas
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
    if (verbose) cat(sprintf("📁 Dossier créé: %s\n", output_dir))
  }
  
  # --------------------------------------------------------------------------
  # 1. PRÉPARATION DES DONNÉES
  # --------------------------------------------------------------------------
  
  if (verbose) cat("📋 Préparation des données...\n")
  
  if (use_data_table) {
    if (!inherits(panel, "data.table")) {
      df <- as.data.table(panel)
    } else {
      df <- copy(panel)
    }
  } else {
    df <- as.data.frame(panel)
  }
  
  # Détection automatique des colonnes
  ANNEE_COL <- if ("ANNEE_COLLECTE" %in% names(df)) "ANNEE_COLLECTE" else "YEAR"
  MOIS_COL <- if ("MOIS_COLLECTE" %in% names(df)) "MOIS_COLLECTE" else "MONTH"
  BRUT_COL <- if ("MONTANT BRUT_B1" %in% names(df)) "MONTANT BRUT_B1" else "MONTANT BRUT_B0"
  NET_COL <- "MONTANT NET"
  SEXE_COL <- "SEXE_IMPUTE"
  CAT_COL <- "CATEGORIE_1"
  MAT_COL <- "MATRICULE"
  
  # --------------------------------------------------------------------------
  # 2. NETTOYAGE ET TRANSFORMATION (VECTORISÉ)
  # --------------------------------------------------------------------------
  
  if (use_data_table) {
    df[, `:=`(
      NET = as.numeric(get(NET_COL)),
      BRUT = as.numeric(get(BRUT_COL)),
      ANNEE = as.numeric(get(ANNEE_COL)),
      MOIS_NUM = as.numeric(get(MOIS_COL))
    )]
    
    df[, MOIS := MOIS_FR[as.character(MOIS_NUM)]]
    
    df[, SEXE_STD := fcase(
      toupper(get(SEXE_COL)) %in% c("M", "H", "HOMME"), "Homme",
      toupper(get(SEXE_COL)) %in% c("F", "FEMME"), "Femme",
      default = NA_character_
    )]
    
    df[, STATUT := fifelse(
      substr(as.character(get(MAT_COL)), 1, 1) %in% c("5", "6"),
      "Non fonctionnaire",
      "Fonctionnaire"
    )]
    
  } else {
    df <- df %>%
      mutate(
        NET = as.numeric(.data[[NET_COL]]),
        BRUT = as.numeric(.data[[BRUT_COL]]),
        ANNEE = as.numeric(.data[[ANNEE_COL]]),
        MOIS_NUM = as.numeric(.data[[MOIS_COL]]),
        MOIS = MOIS_FR[as.character(MOIS_NUM)],
        SEXE_STD = case_when(
          toupper(.data[[SEXE_COL]]) %in% c("M", "H", "HOMME") ~ "Homme",
          toupper(.data[[SEXE_COL]]) %in% c("F", "FEMME") ~ "Femme",
          TRUE ~ NA_character_
        ),
        STATUT = ifelse(
          substr(as.character(.data[[MAT_COL]]), 1, 1) %in% c("5", "6"),
          "Non fonctionnaire",
          "Fonctionnaire"
        )
      )
  }
  
  if (verbose) cat(sprintf("✅ Données préparées: %s lignes\n", format(nrow(df), big.mark = ",")))
  
  # --------------------------------------------------------------------------
  # 3. RÉCUPÉRATION DES ANNÉES DISPONIBLES
  # --------------------------------------------------------------------------
  
  annees <- sort(unique(df$ANNEE[!is.na(df$ANNEE)]))
  if (verbose) cat(sprintf("📅 Années détectées: %s\n", paste(annees, collapse = ", ")))
  
  # --------------------------------------------------------------------------
  # 4. FONCTION D'AGRÉGATION
  # --------------------------------------------------------------------------
  
  calc_indicateurs <- function(data) {
    if (use_data_table && inherits(data, "data.table")) {
      data[, .(
        `Salaire mensuel net moyen` = mean(NET, na.rm = TRUE),
        `Salaire mensuel brut moyen` = mean(BRUT, na.rm = TRUE),
        `Salaire médian mensuel net` = median(NET, na.rm = TRUE),
        `Salaire médian mensuel brut` = median(BRUT, na.rm = TRUE),
        `Min net` = min(NET, na.rm = TRUE),
        `Max net` = max(NET, na.rm = TRUE),
        `Effectif` = .N
      ), by = .(ANNEE, MOIS_NUM, MOIS)]
    } else {
      data %>%
        group_by(ANNEE, MOIS_NUM, MOIS) %>%
        summarise(
          `Salaire mensuel net moyen` = mean(NET, na.rm = TRUE),
          `Salaire mensuel brut moyen` = mean(BRUT, na.rm = TRUE),
          `Salaire médian mensuel net` = median(NET, na.rm = TRUE),
          `Salaire médian mensuel brut` = median(BRUT, na.rm = TRUE),
          `Min net` = min(NET, na.rm = TRUE),
          `Max net` = max(NET, na.rm = TRUE),
          `Effectif` = n(),
          .groups = "drop"
        )
    }
  }
  
  # --------------------------------------------------------------------------
  # 5. FONCTION DE CALCUL POUR UNE ANNÉE
  # --------------------------------------------------------------------------
  
  process_annee <- function(annee_val, df_complet, verbose_inner = FALSE) {
    
    if (verbose_inner) cat(sprintf("\n📊 Traitement de l'année %s...\n", annee_val))
    
    # Filtrer les données pour cette année
    if (use_data_table) {
      df_annee <- df_complet[ANNEE == annee_val]
    } else {
      df_annee <- df_complet %>% filter(ANNEE == annee_val)
    }
    
    frames_list <- list()
    
    # A. Fonctionnaires par catégorie
    categories <- sort(unique(df_annee[[CAT_COL]][!is.na(df_annee[[CAT_COL]])]))
    
    if (use_data_table) {
      for (cat in categories) {
        nom_groupe <- sprintf("Fonctionnaire - Cat %s", cat)
        sous_df <- df_annee[STATUT == "Fonctionnaire" & get(CAT_COL) == cat]
        if (nrow(sous_df) > 0) {
          frames_list[[nom_groupe]] <- calc_indicateurs(sous_df)
        }
      }
    } else {
      for (cat in categories) {
        nom_groupe <- sprintf("Fonctionnaire - Cat %s", cat)
        sous_df <- df_annee %>% filter(STATUT == "Fonctionnaire", .data[[CAT_COL]] == cat)
        if (nrow(sous_df) > 0) {
          frames_list[[nom_groupe]] <- calc_indicateurs(sous_df)
        }
      }
    }
    
    # B. Non fonctionnaires
    if (use_data_table) {
      sous_df <- df_annee[STATUT == "Non fonctionnaire"]
      if (nrow(sous_df) > 0) {
        frames_list[["Non fonctionnaire"]] <- calc_indicateurs(sous_df)
      }
    } else {
      sous_df <- df_annee %>% filter(STATUT == "Non fonctionnaire")
      if (nrow(sous_df) > 0) {
        frames_list[["Non fonctionnaire"]] <- calc_indicateurs(sous_df)
      }
    }
    
    # C. Ensemble
    frames_list[["Ensemble"]] <- calc_indicateurs(df_annee)
    
    # D. Par sexe
    if (use_data_table) {
      sous_df_h <- df_annee[SEXE_STD == "Homme"]
      sous_df_f <- df_annee[SEXE_STD == "Femme"]
      if (nrow(sous_df_h) > 0) frames_list[["Homme"]] <- calc_indicateurs(sous_df_h)
      if (nrow(sous_df_f) > 0) frames_list[["Femme"]] <- calc_indicateurs(sous_df_f)
    } else {
      sous_df_h <- df_annee %>% filter(SEXE_STD == "Homme")
      sous_df_f <- df_annee %>% filter(SEXE_STD == "Femme")
      if (nrow(sous_df_h) > 0) frames_list[["Homme"]] <- calc_indicateurs(sous_df_h)
      if (nrow(sous_df_f) > 0) frames_list[["Femme"]] <- calc_indicateurs(sous_df_f)
    }
    
    # Assemblage
    result_list <- lapply(names(frames_list), function(nom) {
      df_temp <- frames_list[[nom]]
      if (use_data_table && inherits(df_temp, "data.table")) {
        df_temp <- as.data.frame(df_temp)
      }
      
      cols_to_rename <- setdiff(names(df_temp), c("ANNEE", "MOIS_NUM", "MOIS"))
      names(df_temp)[names(df_temp) %in% cols_to_rename] <- 
        paste(nom, cols_to_rename, sep = " - ")
      
      df_temp
    })
    
    result <- Reduce(function(x, y) {
      merge(x, y, by = c("ANNEE", "MOIS_NUM", "MOIS"), all = TRUE)
    }, result_list)
    
    result <- result[order(result$MOIS_NUM), ]
    result <- result[, !names(result) %in% "MOIS_NUM"]
    
    # Libérer mémoire
    gc()
    
    return(result)
  }
  
  # --------------------------------------------------------------------------
  # 6. TRAITEMENT PAR ANNÉE (AVEC OU SANS PARALLÉLISATION)
  # --------------------------------------------------------------------------
  
  if (use_parallel) {
    plan(multisession, workers = n_cores)
    if (verbose) cat(sprintf("⚡ Parallélisation activée avec %d cœurs\n", n_cores))
    
    resultats_annuels <- future_map(annees, function(annee) {
      process_annee(annee, df, verbose_inner = FALSE)
    }, .options = furrr_options(seed = TRUE))
    names(resultats_annuels) <- annees
    
    plan(sequential)
  } else {
    resultats_annuels <- list()
    for (annee in annees) {
      resultats_annuels[[as.character(annee)]] <- process_annee(annee, df, verbose_inner = verbose)
    }
  }
  
  # --------------------------------------------------------------------------
  # 7. EXPORT DES FICHIERS ANNUELS
  # --------------------------------------------------------------------------
  
  if (verbose) cat("\n💾 Export des fichiers annuels...\n")
  
  for (annee in annees) {
    result_annee <- resultats_annuels[[as.character(annee)]]
    
    # Chemins de sortie
    xlsx_path <- file.path(output_dir, sprintf("%s_%s.xlsx", prefix, annee))
    csv_path <- file.path(output_dir, sprintf("%s_%s.csv", prefix, annee))
    
    # Export CSV
    fwrite(result_annee, csv_path, bom = TRUE)
    
    # Export Excel
    if (use_openxlsx) {
      wb <- createWorkbook()
      addWorksheet(wb, sprintf("Année %s", annee))
      writeData(wb, 1, result_annee)
      
      # Style d'en-tête
      headerStyle <- createStyle(
        fontSize = 11,
        fontColour = "#FFFFFF",
        halign = "center",
        fgFill = "#4F81BD",
        border = "TopBottom",
        textDecoration = "bold"
      )
      addStyle(wb, 1, headerStyle, rows = 1, cols = 1:ncol(result_annee), gridExpand = TRUE)
      
      # Format nombres
      numStyle <- createStyle(numFmt = "#,##0")
      addStyle(wb, 1, numStyle, rows = 2:(nrow(result_annee) + 1), 
               cols = 2:ncol(result_annee), gridExpand = TRUE)
      
      setColWidths(wb, 1, cols = 1:ncol(result_annee), widths = "auto")
      
      saveWorkbook(wb, xlsx_path, overwrite = TRUE)
    } else {
      write_xlsx(result_annee, xlsx_path)
    }
    
    if (verbose) cat(sprintf("  ✓ Année %s: %s\n", annee, basename(xlsx_path)))
  }
  
  # --------------------------------------------------------------------------
  # 8. GÉNÉRATION DU FICHIER GLOBAL (OPTIONNEL)
  # --------------------------------------------------------------------------
  
  if (generate_global) {
    if (verbose) cat("\n📦 Génération du fichier global (toutes années)...\n")
    
    result_global <- do.call(rbind, resultats_annuels)
    
    xlsx_global <- file.path(output_dir, sprintf("%s_GLOBAL_2015_2025.xlsx", prefix))
    csv_global <- file.path(output_dir, sprintf("%s_GLOBAL_2015_2025.csv", prefix))
    
    fwrite(result_global, csv_global, bom = TRUE)
    
    if (use_openxlsx) {
      wb_global <- createWorkbook()
      addWorksheet(wb_global, "Toutes années")
      writeData(wb_global, 1, result_global)
      
      headerStyle <- createStyle(
        fontSize = 11,
        fontColour = "#FFFFFF",
        halign = "center",
        fgFill = "#4F81BD",
        border = "TopBottom",
        textDecoration = "bold"
      )
      addStyle(wb_global, 1, headerStyle, rows = 1, cols = 1:ncol(result_global), gridExpand = TRUE)
      
      numStyle <- createStyle(numFmt = "#,##0")
      addStyle(wb_global, 1, numStyle, rows = 2:(nrow(result_global) + 1), 
               cols = 2:ncol(result_global), gridExpand = TRUE)
      
      setColWidths(wb_global, 1, cols = 1:ncol(result_global), widths = "auto")
      
      saveWorkbook(wb_global, xlsx_global, overwrite = TRUE)
    } else {
      write_xlsx(result_global, xlsx_global)
    }
    
    if (verbose) cat(sprintf("  ✓ Fichier global: %s\n", basename(xlsx_global)))
  }
  
  # --------------------------------------------------------------------------
  # 9. RÉSUMÉ FINAL
  # --------------------------------------------------------------------------
  
  toc()
  
  if (verbose) {
    cat("\n✨ Terminé!\n")
    cat(sprintf("📁 Dossier de sortie: %s\n", output_dir))
    cat(sprintf("📄 Fichiers générés: %d fichiers annuels", length(annees)))
    if (generate_global) cat(" + 1 fichier global")
    cat("\n")
  }
  
  return(invisible(resultats_annuels))
}

# ==============================================================================
# WORKFLOW COMPLET : CHARGEMENT + CALCUL
# ==============================================================================

pipeline_complet <- function(
    file_path,
    output_dir = "indicateurs_annuels",
    prefix = "indicateurs_salaire",
    generate_global = TRUE,
    use_data_table = TRUE,
    use_parallel = FALSE,
    n_cores = parallel::detectCores() - 1,
    use_openxlsx = FALSE,
    verbose = TRUE
) {
  
  cat(rep("=", 80), "\n", sep = "")
  cat("PIPELINE COMPLET: CHARGEMENT + CALCUL INDICATEURS\n")
  cat(rep("=", 80), "\n\n", sep = "")
  
  # Étape 1: Chargement
  panel <- charger_panel_solde(file_path = file_path, verbose = verbose)
  
  if (is.null(panel)) {
    stop("❌ Échec du chargement des données")
  }
  
  cat("\n")
  
  # Étape 2: Calcul des indicateurs
  resultats <- build_indicateurs_salaire_par_annee(
    panel = panel,
    output_dir = output_dir,
    prefix = prefix,
    generate_global = generate_global,
    use_data_table = use_data_table,
    use_parallel = use_parallel,
    n_cores = n_cores,
    use_openxlsx = use_openxlsx,
    verbose = verbose
  )
  
  return(invisible(resultats))
}

# ==============================================================================
# EXEMPLES D'UTILISATION
# ==============================================================================

if (FALSE) {
  
  # Exemple 1: Pipeline complet depuis fichier Parquet local
  pipeline_complet(
    file_path = "data/Panel_solde_complet_2015_2025.parquet",
    output_dir = "resultats/indicateurs_2015_2025",
    prefix = "indicateurs_salaire",
    generate_global = TRUE,
    use_data_table = TRUE,
    use_parallel = TRUE,
    n_cores = 6,
    use_openxlsx = TRUE,
    verbose = TRUE
  )
  
  # Exemple 2: Depuis fichier CSV
  pipeline_complet(
    file_path = "data/panel_solde.csv",
    output_dir = "resultats",
    use_data_table = TRUE,
    verbose = TRUE
  )
  
  # Exemple 3: Chargement manuel puis calcul
  panel <- charger_panel_solde(
    file_path = "data/Panel_solde_complet_2015_2025.parquet",
    verbose = TRUE
  )
  
  resultats <- build_indicateurs_salaire_par_annee(
    panel = panel,
    output_dir = "indicateurs_annuels",
    generate_global = TRUE,
    use_data_table = TRUE,
    use_parallel = TRUE,
    n_cores = 8,
    use_openxlsx = TRUE,
    verbose = TRUE
  )
  
  # Exemple 4: Configuration minimale (rapide)
  pipeline_complet(
    file_path = "data/Panel_solde_complet_2015_2025.parquet",
    output_dir = "resultats_rapides",
    generate_global = FALSE,
    use_data_table = TRUE,
    use_parallel = FALSE,
    use_openxlsx = FALSE,
    verbose = TRUE
  )
  
  # Exemple 5: Configuration maximale (qualité)
  pipeline_complet(
    file_path = "data/Panel_solde_complet_2015_2025.parquet",
    output_dir = "resultats_finaux",
    generate_global = TRUE,
    use_data_table = TRUE,
    use_parallel = TRUE,
    n_cores = parallel::detectCores() - 1,
    use_openxlsx = TRUE,
    verbose = TRUE
  )
  
}


# Simple et rapide
pipeline_complet(
  file_path = "Panel_solde_complet_2015_2025.parquet",
  output_dir = "indicateurs",
  use_data_table = TRUE,
  verbose = TRUE
)