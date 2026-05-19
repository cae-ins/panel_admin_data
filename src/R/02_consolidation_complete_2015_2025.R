# ============================================================
# CONSOLIDATION COMPLÈTE 2015-2025
# Version : Production avec Parquet
# Auteur : Romuald - CRRAE-UMOA
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

dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(LOGS_DIR, showWarnings = FALSE, recursive = TRUE)

# --- PARAMÈTRES DE TRAITEMENT ---
TAILLE_LOT <- 12  # Traiter 12 mois à la fois (1 an)

# --- INITIALISATION LOG ---
log_file <- file.path(LOGS_DIR, paste0("consolidation_complete_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".log"))
log_conn <- NULL

# Fonction pour ouvrir/réouvrir la connexion log
ouvrir_log <- function() {
  if (is.null(log_conn) || !isOpen(log_conn)) {
    log_conn <<- file(log_file, open = "at")  # "at" = append text mode
  }
}

# Fonction log robuste
log_message <- function(msg) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  message <- paste0("[", timestamp, "] ", msg)
  
  # Afficher dans console
  cat(message, "\n")
  
  # Écrire dans fichier (avec gestion d'erreur)
  tryCatch({
    ouvrir_log()  # S'assurer que la connexion est ouverte
    cat(message, "\n", file = log_conn)
    flush(log_conn)
  }, error = function(e) {
    # Si erreur d'écriture, essayer de rouvrir
    tryCatch({
      log_conn <<- file(log_file, open = "at")
      cat(message, "\n", file = log_conn)
      flush(log_conn)
    }, error = function(e2) {
      # En dernier recours, ignorer silencieusement
      # (le message est déjà affiché en console)
    })
  })
}

# Ouvrir le log au démarrage
ouvrir_log()

log_message(paste(rep("=", 70), collapse = ""))
log_message("CONSOLIDATION COMPLÈTE 2015-2025")
log_message(paste(rep("=", 70), collapse = ""))

# ============================================================
# FONCTIONS UTILITAIRES
# ============================================================

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

detecter_entete <- function(fichier, sheet) {
  tryCatch({
    preview <- read_excel(fichier, sheet = sheet, col_names = FALSE, n_max = 20)
    if (nrow(preview) > 1) preview <- preview[-nrow(preview), ]
    for (i in 1:nrow(preview)) {
      ligne <- as.character(preview[i, ])
      ligne_lower <- tolower(trimws(ligne))
      if (any(grepl("matricule", ligne_lower, fixed = TRUE))) return(i)
    }
    return(ifelse(sheet == 2, 2, 1))
  }, error = function(e) {
    return(ifelse(sheet == 2, 2, 1))
  })
}

normaliser_nom_colonne <- function(nom) {
  if (is.na(nom) || nom == "") return("colonne_vide")
  nom <- gsub("^[A-Z_]+[0-9]+\\.", "", nom)
  nom <- toupper(nom)
  nom <- gsub("-", " ", nom, fixed = TRUE)
  nom <- gsub("/", " ", nom, fixed = TRUE)
  nom <- gsub("_", " ", nom, fixed = TRUE)
  nom <- gsub("\\.", " ", nom)
  nom <- stri_trans_general(nom, "Latin-ASCII")
  nom <- gsub("\\s+", " ", nom)
  return(trimws(nom))
}

mapper_colonnes <- function(noms_colonnes) {
  noms_normalises <- sapply(noms_colonnes, normaliser_nom_colonne)
  
  mapping <- list(
    cle_unique = c("MATRICULE.*CODE.*ORGANISME", "MATRICULE.*\\|\\|.*CODE"),
    matricule = c("^MATRICULE$", "^MATRIC$"),
    nom = c("^NOM$", "^NOM PRENOM$", "^NOM ET PRENOM$"),
    date_naissance = c("DATE.*NAISSANCE", "DATE NAISS", "NAISSANCE"),
    sexe = c("^SEXE$"),
    situation_matrimoniale = c("SITUATION.*MATRIMONIALE", "STATUT.*MATRIMONIAL"),
    nombre_enfant = c("NOMBRE.*ENFANT", "NBR.*ENFANT"),
    situation = c("^SITUATION$", "SITUATION ADMINISTRATIVE"),
    date_debut_situation = c("DATE.*DEBUT.*SITUATION", "DATE SITUATION"),
    montant_brut = c("MONTANT BRUT", "SALAIRE BRUT", "REMUNERATION BRUTE", "^BRUT$"),
    montant_net = c("MONTANT NET", "SALAIRE NET", "REMUNERATION NETTE", "^NET$"),
    retenue_pension = c("RETENUE.*PENSION", "PENSION", "COTISATION.*PENSION"),
    impot = c("^IMPOT$", "^IGR$", "IMPOT.*REVENU"),
    charge_patronale = c("CHARGE.*PATRONALE", "CHARGES.*PATRONALES"),
    organisme = c("^ORGANISME$", "MINISTERE", "DIRECTION"),
    lieu_affectation = c("LIEU.*AFFECTATION", "LIEU D'AFFECTATION", "^AFFECTATION$"),
    service = c("^SERVICE$"),
    emploi = c("^EMPLOI$", "^CORPS$", "CORPS.*EMPLOI"),
    fonction = c("^FONCTION$"),
    grade = c("CLASSE.*ECHELON", "CLASSE ECHELON"),
    statut_fonctionnaire = c("^GRADE$"),
    poste = c("^POSTE$", "LIBELLE.*POSTE"),
    prise_service = c("PRISE.*SERVICE", "DATE.*PRISE.*SERVICE"),
    date_retraite = c("DATE.*RETRAITE"),
    age_retraite = c("AGE.*RETRAITE"),
    mois_annee = c("MOIS.*ANNEE", "PERIODE")
  )
  
  correspondance <- data.frame(
    nom_source = noms_colonnes,
    nom_standard = NA_character_,
    stringsAsFactors = FALSE
  )
  
  for (i in 1:nrow(correspondance)) {
    nom_norm <- noms_normalises[i]
    for (std_name in names(mapping)) {
      for (pattern in mapping[[std_name]]) {
        if (grepl(pattern, nom_norm, ignore.case = TRUE)) {
          correspondance$nom_standard[i] <- std_name
          break
        }
      }
      if (!is.na(correspondance$nom_standard[i])) break
    }
    if (is.na(correspondance$nom_standard[i])) {
      correspondance$nom_standard[i] <- tolower(gsub(" ", "_", nom_norm))
    }
  }
  
  return(correspondance)
}

normaliser_situation <- function(situation) {
  if (is.na(situation) || situation == "") return("autre")
  sit_norm <- toupper(trimws(as.character(situation)))
  sit_norm <- stri_trans_general(sit_norm, "Latin-ASCII")
  sit_norm <- gsub("\\s+", " ", sit_norm)
  
  situations_valides <- list(
    en_activite = c("EN ACTIVITE", "ACTIVITE", "EN ACTIVITÉ", "ACTIVITÉ"),
    regul_indemnites = c("REGUL. INDEMNITES", "REGUL INDEMNITES", "REGULARISATION INDEMNITES"),
    demi_solde = c("DEMI-SOLDE", "DEMI SOLDE", "DEMISOLDE", "1/2 SOLDE")
  )
  
  for (categorie in names(situations_valides)) {
    if (sit_norm %in% situations_valides[[categorie]]) return(categorie)
  }
  
  return("autre")
}

enrichir_par_codes <- function(dt) {
  stats_matching <- list()
  
  for (nom_code in names(dictionnaires_codes)) {
    config <- dictionnaires_codes[[nom_code]]
    table_codes <- config$table
    col_code <- config$col_code
    col_jointure <- config$col_jointure_source
    
    if (!col_jointure %in% names(dt)) {
      dt[, (nom_code) := NA_character_]
      stats_matching[[nom_code]] <- list(match = 0, total = nrow(dt), taux = 0)
      next
    }
    
    dt[, col_temp_norm := normaliser_pour_matching(get(col_jointure))]
    
    table_match <- table_codes[!is.na(libelle_norm), .(libelle_norm, code = get(col_code))]
    
    cols_supp_presentes <- c()
    if (!is.null(config$cols_supplementaires) && length(config$cols_supplementaires) > 0) {
      for (col_sup in config$cols_supplementaires) {
        if (col_sup %in% names(table_codes)) {
          vals_sup <- table_codes[!is.na(libelle_norm), get(col_sup)]
          table_match[, (col_sup) := vals_sup]
          cols_supp_presentes <- c(cols_supp_presentes, col_sup)
        }
      }
    }
    
    table_match <- table_match[!duplicated(libelle_norm)]
    
    dt <- merge(dt, table_match, by.x = "col_temp_norm", by.y = "libelle_norm", 
                all.x = TRUE, suffixes = c("", "_dup"))
    
    if ("code" %in% names(dt)) setnames(dt, "code", nom_code)
    
    if (nom_code %in% names(dt)) {
      nb_match <- sum(!is.na(dt[[nom_code]]))
      taux <- round(100 * nb_match / nrow(dt), 1)
      stats_matching[[nom_code]] <- list(match = nb_match, total = nrow(dt), taux = taux)
    } else {
      stats_matching[[nom_code]] <- list(match = 0, total = nrow(dt), taux = 0)
    }
    
    if ("col_temp_norm" %in% names(dt)) dt[, col_temp_norm := NULL]
  }
  
  return(list(dt = dt, stats = stats_matching))
}

# ============================================================
# CHARGEMENT TABLES ANSTAT
# ============================================================

log_message("\n[CHARGEMENT] Tables de codes ANSTAT")
log_message(paste(rep("-", 70), collapse = ""))

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
log_message(sprintf("  CODE_AFFECTATION : %d codes", nrow(dictionnaires_codes[["CODE_AFFECTATION"]]$table)))

dictionnaires_codes[["CODE_ORGANISME"]] <- charger_table_codes(
  "ORGANISME_OK", c("LIBELLÉ_ORGANISME", "LIBELLE_ORGANISME"),
  "CODE_ORGANISME", "organisme")
log_message(sprintf("  CODE_ORGANISME : %d codes", nrow(dictionnaires_codes[["CODE_ORGANISME"]]$table)))

dictionnaires_codes[["CODE_POSITION_SOLDE"]] <- charger_table_codes(
  "CODE_SITUATION_SOLDE", c("LIBELLÉ_POSITION_SOLDE", "LIBELLE_SITUATION_SOLDE", "LIBELLÉ_SITUATION_SOLDE"),
  "CODE_POSITION_SOLDE", "situation_brute")
log_message(sprintf("  CODE_POSITION_SOLDE : %d codes", nrow(dictionnaires_codes[["CODE_POSITION_SOLDE"]]$table)))

config_emploi <- charger_table_codes(
  "HISTORIQUE_ECHELLES_CORPS", c("LIBELLÉ_EMPLOI", "LIBELLE_EMPLOI"),
  "CODE_EMPLOI", "emploi")
config_emploi$cols_supplementaires <- c("Code_CITP", "Metier_CITP")
dictionnaires_codes[["CODE_EMPLOI"]] <- config_emploi
log_message(sprintf("  CODE_EMPLOI : %d codes (+ CITP)", nrow(config_emploi$table)))

dictionnaires_codes[["CODE_SERVICE"]] <- charger_table_codes(
  "SERVICE", c("LIBELLÉ_SERVICE", "LIBELLE_SERVICE"),
  "CODE_SERVICE", "service")
log_message(sprintf("  CODE_SERVICE : %d codes", nrow(dictionnaires_codes[["CODE_SERVICE"]]$table)))

dictionnaires_codes[["CODE_FONCTION"]] <- charger_table_codes(
  "FONCTION", c("LIBELLÉ_FONCTION", "LIBELLE_FONCTION"),
  "CODE_FONCTION", "fonction")
log_message(sprintf("  CODE_FONCTION : %d codes", nrow(dictionnaires_codes[["CODE_FONCTION"]]$table)))

dictionnaires_codes[["CODE_GRADE"]] <- charger_table_codes(
  "GRADE", c("LIBELLÉ_GRADE", "LIBELLE_GRADE"),
  "CODE_GRADE", "grade")
log_message(sprintf("  CODE_GRADE : %d codes", nrow(dictionnaires_codes[["CODE_GRADE"]]$table)))

dictionnaires_codes[["CODE_POSTE"]] <- charger_table_codes(
  "LIBELLE POSTE", c("LIBELLÉ_POSTE", "LIBELLE_POSTE"),
  "CODE_POSTE", "poste")
log_message(sprintf("  CODE_POSTE : %d codes", nrow(dictionnaires_codes[["CODE_POSTE"]]$table)))

log_message("  ✓ Toutes les tables chargées")

# ============================================================
# FONCTION TRAITEMENT FICHIER
# ============================================================

traiter_fichier <- function(fichier_path, periode) {
  
  if (!file.exists(fichier_path)) return(NULL)
  
  tryCatch({
    
    # FEUILLE 1
    ligne_entete_f1 <- detecter_entete(fichier_path, sheet = 1)
    feuille1 <- as.data.table(read_excel(fichier_path, sheet = 1, skip = ligne_entete_f1 - 1, col_types = "text"))
    
    mapping_colonnes <- mapper_colonnes(names(feuille1))
    noms_std <- mapping_colonnes$nom_standard
    doublons <- noms_std[duplicated(noms_std)]
    if (length(doublons) > 0) {
      for (nom_dup in unique(doublons)) {
        idx <- which(noms_std == nom_dup)
        for (i in seq_along(idx)) {
          if (i > 1) noms_std[idx[i]] <- paste0(nom_dup, "_dup", i)
        }
      }
      mapping_colonnes$nom_standard <- noms_std
    }
    
    setnames(feuille1, old = mapping_colonnes$nom_source, new = mapping_colonnes$nom_standard, skip_absent = TRUE)
    
    if (!all(c("matricule", "nom", "situation", "montant_brut", "montant_net") %in% names(feuille1))) {
      return(NULL)
    }
    
    feuille1[, situation_brute := situation]
    feuille1[, situation_normalisee := sapply(situation, normaliser_situation)]
    
    avant_filtrage <- nrow(feuille1)
    feuille1 <- feuille1[situation_normalisee %in% c("en_activite", "regul_indemnites", "demi_solde")]
    apres_filtrage <- nrow(feuille1)
    
    if (nrow(feuille1) == 0) return(NULL)
    
    # FEUILLE 2
    ligne_entete_f2 <- detecter_entete(fichier_path, sheet = 2)
    feuille2 <- as.data.table(read_excel(fichier_path, sheet = 2, skip = ligne_entete_f2 - 1, col_types = "text"))
    
    nom_cle_f2 <- NULL
    for (col_name in names(feuille2)) {
      if (grepl("MATRICULE.*\\|\\|", col_name, ignore.case = TRUE)) {
        nom_cle_f2 <- col_name
        break
      }
    }
    
    if (!is.null(nom_cle_f2)) {
      setnames(feuille2, old = nom_cle_f2, new = "cle_unique_f2")
      
      cols_f1 <- names(feuille1)
      cols_f2 <- setdiff(names(feuille2), "cle_unique_f2")
      cols_communes <- intersect(cols_f1, cols_f2)
      
      if (length(cols_communes) > 0) {
        for (cc in cols_communes) {
          setnames(feuille2, cc, paste0(cc, "_fromF2"))
        }
      }
      
      nom_cle_f1 <- if ("cle_unique" %in% names(feuille1)) "cle_unique" else "matricule"
      feuille_complete <- merge(feuille1, feuille2, by.x = nom_cle_f1, by.y = "cle_unique_f2",
                                all.x = TRUE, suffixes = c("", "_F2dup"))
      
      noms_finaux <- names(feuille_complete)
      dups_finaux <- noms_finaux[duplicated(noms_finaux)]
      if (length(dups_finaux) > 0) {
        for (nd in unique(dups_finaux)) {
          idx <- which(noms_finaux == nd)
          for (i in seq_along(idx)) {
            noms_finaux[idx[i]] <- paste0(nd, "_v", i)
          }
        }
        setnames(feuille_complete, names(feuille_complete), noms_finaux)
      }
      
      feuille1 <- feuille_complete
    }
    
    # ENRICHISSEMENT
    resultat_enrichissement <- enrichir_par_codes(feuille1)
    feuille1 <- resultat_enrichissement$dt
    stats_codes <- resultat_enrichissement$stats
    
    # MÉTADONNÉES
    feuille1[, periode := periode]
    feuille1[, fichier_source := basename(fichier_path)]
    
    # TYPES NUMÉRIQUES
    if ("montant_brut" %in% names(feuille1)) feuille1[, montant_brut := as.numeric(montant_brut)]
    if ("montant_net" %in% names(feuille1)) feuille1[, montant_net := as.numeric(montant_net)]
    
    cols_numeriques <- grep("^[0-9]+$", names(feuille1), value = TRUE)
    for (col in cols_numeriques) {
      feuille1[, (col) := as.numeric(get(col))]
    }
    
    return(list(
      feuille1 = feuille1,
      stats = list(
        lignes_totales = avant_filtrage,
        lignes_gardees = apres_filtrage,
        colonnes = ncol(feuille1),
        stats_codes = stats_codes
      )
    ))
    
  }, error = function(e) {
    log_message(sprintf("  ✗ ERREUR %s : %s", basename(fichier_path), e$message))
    return(NULL)
  })
}

# ============================================================
# TRAITEMENT PAR LOTS
# ============================================================

log_message(paste0("\n", paste(rep("=", 70), collapse = "")))
log_message("PHASE : TRAITEMENT DES FICHIERS")
log_message(paste(rep("=", 70), collapse = ""))

# Lister tous les fichiers
fichiers_tous <- list.files(DATA_SOURCES, pattern = "\\.xlsx$", full.names = TRUE, recursive = TRUE)
fichiers_tous <- sort(fichiers_tous)

log_message(sprintf("Fichiers trouvés : %d", length(fichiers_tous)))
log_message(sprintf("Traitement par lots de %d fichiers", TAILLE_LOT))

# Diviser en lots
nb_lots <- ceiling(length(fichiers_tous) / TAILLE_LOT)
lots <- split(fichiers_tous, ceiling(seq_along(fichiers_tous) / TAILLE_LOT))

log_message(sprintf("Nombre de lots : %d\n", nb_lots))

# Stocker résultats consolidés par lot
bases_par_lot <- list()
stats_globales <- list()

for (i_lot in seq_along(lots)) {
  fichiers_lot <- lots[[i_lot]]
  
  log_message(sprintf("\n[LOT %d/%d] %d fichiers", i_lot, nb_lots, length(fichiers_lot)))
  log_message(paste(rep("-", 70), collapse = ""))
  
  resultats_lot <- list()
  
  for (fichier_path in fichiers_lot) {
    periode <- gsub("\\.xlsx$", "", basename(fichier_path))
    
    log_message(sprintf("  [%s]", periode))
    
    resultat <- traiter_fichier(fichier_path, periode)
    
    if (!is.null(resultat)) {
      resultats_lot[[periode]] <- resultat$feuille1
      stats_globales[[periode]] <- resultat$stats
      
      log_message(sprintf("    ✓ %d lignes × %d colonnes", 
                         nrow(resultat$feuille1), ncol(resultat$feuille1)))
      
      # Afficher taux codes principaux
      for (code in c("CODE_ORGANISME", "CODE_GRADE", "CODE_EMPLOI")) {
        if (!is.null(resultat$stats$stats_codes[[code]])) {
          taux <- resultat$stats$stats_codes[[code]]$taux
          log_message(sprintf("      %s : %.1f%%", code, taux))
        }
      }
    }
  }
  
  # Consolider le lot
  if (length(resultats_lot) > 0) {
    log_message(sprintf("\n  Consolidation lot %d...", i_lot))
    
    toutes_colonnes_lot <- unique(unlist(lapply(resultats_lot, names)))
    
    for (periode in names(resultats_lot)) {
      dt <- resultats_lot[[periode]]
      colonnes_manquantes <- setdiff(toutes_colonnes_lot, names(dt))
      if (length(colonnes_manquantes) > 0) {
        for (col in colonnes_manquantes) {
          dt[, (col) := NA]
        }
      }
      setcolorder(dt, toutes_colonnes_lot)
      resultats_lot[[periode]] <- dt
    }
    
    base_lot <- rbindlist(resultats_lot, use.names = TRUE, fill = TRUE)
    
    log_message(sprintf("  ✓ Lot %d : %d lignes × %d colonnes", 
                       i_lot, nrow(base_lot), ncol(base_lot)))
    
    bases_par_lot[[paste0("lot_", i_lot)]] <- base_lot
    
    # Libérer mémoire
    rm(resultats_lot, base_lot)
    gc()
  }
}

# ============================================================
# CONSOLIDATION FINALE
# ============================================================

log_message(paste0("\n", paste(rep("=", 70), collapse = "")))
log_message("PHASE : CONSOLIDATION FINALE")
log_message(paste(rep("=", 70), collapse = ""))

if (length(bases_par_lot) > 0) {
  
  # Harmoniser colonnes entre lots
  toutes_colonnes_finales <- unique(unlist(lapply(bases_par_lot, names)))
  log_message(sprintf("  Colonnes uniques totales : %d", length(toutes_colonnes_finales)))
  
  for (nom_lot in names(bases_par_lot)) {
    dt_lot <- bases_par_lot[[nom_lot]]
    colonnes_manquantes <- setdiff(toutes_colonnes_finales, names(dt_lot))
    if (length(colonnes_manquantes) > 0) {
      for (col in colonnes_manquantes) {
        dt_lot[, (col) := NA]
      }
    }
    setcolorder(dt_lot, toutes_colonnes_finales)
    bases_par_lot[[nom_lot]] <- dt_lot
  }
  
  # Empilement final
  log_message("  Empilement vertical final...")
  base_finale <- rbindlist(bases_par_lot, use.names = TRUE, fill = TRUE)
  
  log_message(sprintf("  ✓ Base finale : %d lignes × %d colonnes", 
                     nrow(base_finale), ncol(base_finale)))
  
  # ============================================================
  # VALIDATION
  # ============================================================
  
  log_message(paste0("\n", paste(rep("=", 70), collapse = "")))
  log_message("PHASE : VALIDATION")
  log_message(paste(rep("=", 70), collapse = ""))
  
  # Codes présents
  codes_attendus <- c("CODE_AFFECTATION", "CODE_ORGANISME", "CODE_POSITION_SOLDE",
                      "CODE_EMPLOI", "CODE_SERVICE", "CODE_FONCTION", "CODE_GRADE", "CODE_POSTE")
  codes_presents <- intersect(codes_attendus, names(base_finale))
  
  log_message(sprintf("  Codes présents : %d/%d", length(codes_presents), length(codes_attendus)))
  
  for (code in codes_presents) {
    taux <- 100 * (1 - sum(is.na(base_finale[[code]])) / nrow(base_finale))
    log_message(sprintf("    %s : %.1f%% complet", code, taux))
  }
  
  # Périodes
  periodes_uniques <- unique(base_finale$periode)
  log_message(sprintf("\n  Périodes : %d", length(periodes_uniques)))
  log_message(sprintf("    Première : %s", min(periodes_uniques)))
  log_message(sprintf("    Dernière : %s", max(periodes_uniques)))
  
  # Agents uniques
  if ("matricule" %in% names(base_finale)) {
    agents_uniques <- uniqueN(base_finale$matricule)
    log_message(sprintf("\n  Agents uniques : %,d", agents_uniques))
  }
  
  # Distribution situations
  log_message("\n  Distribution situations :")
  dist_sit <- table(base_finale$situation_normalisee)
  for (sit in names(dist_sit)) {
    pct <- 100 * dist_sit[sit] / nrow(base_finale)
    log_message(sprintf("    %s : %,d (%.1f%%)", sit, dist_sit[sit], pct))
  }
 




 # ============================================================
  # VALIDATION
  # ============================================================
  
  log_message(paste0("\n", paste(rep("=", 70), collapse = "")))
  log_message("PHASE : VALIDATION")
  log_message(paste(rep("=", 70), collapse = ""))
  
  # Codes présents
  codes_attendus <- c("CODE_AFFECTATION", "CODE_ORGANISME", "CODE_POSITION_SOLDE",
                      "CODE_EMPLOI", "CODE_SERVICE", "CODE_FONCTION", "CODE_GRADE", "CODE_POSTE")
  codes_presents <- intersect(codes_attendus, names(base_finale))
  
  log_message(sprintf("  Codes présents : %d/%d", length(codes_presents), length(codes_attendus)))
  
  for (code in codes_presents) {
    taux <- 100 * (1 - sum(is.na(base_finale[[code]])) / nrow(base_finale))
    log_message(sprintf("    %s : %.1f%% complet", code, taux))
  }
  
  # Périodes
  periodes_uniques <- unique(base_finale$periode)
  log_message(sprintf("\n  Périodes : %d", length(periodes_uniques)))
  log_message(sprintf("    Première : %s", min(periodes_uniques)))
  log_message(sprintf("    Dernière : %s", max(periodes_uniques)))
  
  # Agents uniques (CORRIGÉ)
  if ("matricule" %in% names(base_finale)) {
    agents_uniques <- uniqueN(base_finale$matricule)
    log_message(sprintf("\n  Agents uniques : %s", format(agents_uniques, big.mark = ",")))
  }
  
  # Distribution situations (CORRIGÉ)
  log_message("\n  Distribution situations :")
  dist_sit <- table(base_finale$situation_normalisee)
  for (sit in names(dist_sit)) {
    pct <- 100 * dist_sit[sit] / nrow(base_finale)
    log_message(sprintf("    %s : %s (%.1f%%)", sit, format(dist_sit[sit], big.mark = ","), pct))
  }
  
  # ============================================================
  # SAUVEGARDE
  # ============================================================
  
  log_message(paste0("\n", paste(rep("=", 70), collapse = "")))
  log_message("PHASE : SAUVEGARDE")
  log_message(paste(rep("=", 70), collapse = ""))
  
  # PARQUET (FORMAT PRINCIPAL)
  fichier_parquet <- file.path(OUTPUT_DIR, "base_consolidee_2015_2025.parquet")
  
  log_message("  Sauvegarde en format Parquet...")
  
  tryCatch({
    write_parquet(base_finale, fichier_parquet, compression = "snappy")
    taille_parquet <- file.info(fichier_parquet)$size / 1024^2
    log_message(sprintf("  ✓ PARQUET : %s (%.0f MB)", basename(fichier_parquet), taille_parquet))
  }, error = function(e) {
    log_message(sprintf("  ✗ ERREUR Parquet : %s", e$message))
    taille_parquet <- 0
  })
  
  # CSV (BACKUP)
  fichier_csv <- file.path(OUTPUT_DIR, "base_consolidee_2015_2025.csv")
  
  log_message("  Sauvegarde CSV (backup)...")
  
  tryCatch({
    fwrite(base_finale, fichier_csv)
    taille_csv <- file.info(fichier_csv)$size / 1024^2
    log_message(sprintf("  ✓ CSV : %s (%.0f MB)", basename(fichier_csv), taille_csv))
  }, error = function(e) {
    log_message(sprintf("  ✗ ERREUR CSV : %s", e$message))
    taille_csv <- 0
  })
  
  # ÉCHANTILLON EXCEL
  log_message("  Création échantillon Excel...")
  
  if (nrow(base_finale) > 100000) {
    log_message("    Base > 100k lignes, échantillon de 100k")
    echantillon <- head(base_finale, 100000)
  } else {
    echantillon <- base_finale
  }
  
  fichier_sample <- file.path(OUTPUT_DIR, "echantillon_excel.xlsx")
  
  tryCatch({
    write.xlsx(echantillon, fichier_sample)
    taille_sample <- file.info(fichier_sample)$size / 1024^2
    log_message(sprintf("  ✓ Excel échantillon : %s (%.0f MB, %s lignes)", 
                       basename(fichier_sample), taille_sample, format(nrow(echantillon), big.mark = ",")))
  }, error = function(e) {
    log_message(sprintf("  ✗ ERREUR Excel : %s", e$message))
    taille_sample <- 0
  })
  
  # MÉTADONNÉES
  fichier_metadata <- file.path(OUTPUT_DIR, "METADATA.txt")
  
  metadata_content <- sprintf("
=================================================================
MÉTADONNÉES BASE CONSOLIDÉE 2015-2025
=================================================================

Date création : %s
Script : 02_consolidation_complete_2015_2025.R

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

--- FORMATS DISPONIBLES ---
- base_consolidee_2015_2025.parquet  (%.0f MB) - FORMAT PRINCIPAL
- base_consolidee_2015_2025.csv      (%.0f MB) - BACKUP
- echantillon_excel.xlsx             (%.0f MB) - CONSULTATION

--- COLONNES PRINCIPALES ---
%s

--- CODES ANSTAT ---
%s

=================================================================
",
    format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    format(nrow(base_finale), big.mark = ","),
    ncol(base_finale),
    object.size(base_finale) / 1024^3,
    min(base_finale$periode),
    max(base_finale$periode),
    length(unique(base_finale$periode)),
    if ("matricule" %in% names(base_finale)) format(uniqueN(base_finale$matricule), big.mark = ",") else "N/A",
    taille_parquet,
    taille_csv,
    taille_sample,
    paste(head(names(base_finale), 20), collapse = ", "),
    paste(grep("^CODE_", names(base_finale), value = TRUE), collapse = ", ")
  )
  
  cat(metadata_content, file = fichier_metadata)
  log_message(sprintf("  ✓ Métadonnées : %s", basename(fichier_metadata)))
  
  # STATISTIQUES PAR PÉRIODE
  stats_df <- data.frame(
    periode = names(stats_globales),
    lignes_totales = sapply(stats_globales, function(x) x$lignes_totales),
    lignes_gardees = sapply(stats_globales, function(x) x$lignes_gardees),
    colonnes = sapply(stats_globales, function(x) x$colonnes),
    stringsAsFactors = FALSE
  )
  
  fichier_stats <- file.path(OUTPUT_DIR, "statistiques_par_periode.csv")
  write.csv(stats_df, fichier_stats, row.names = FALSE)
  log_message(sprintf("  ✓ Statistiques : %s", basename(fichier_stats)))
  
  # COMPARAISON TAILLES
  if (exists("taille_parquet") && exists("taille_csv") && taille_parquet > 0 && taille_csv > 0) {
    log_message("\n  Comparaison tailles fichiers :")
    log_message(sprintf("    Parquet : %.0f MB (compression snappy)", taille_parquet))
    log_message(sprintf("    CSV     : %.0f MB (texte brut)", taille_csv))
    
    if (taille_parquet < taille_csv) {
      gain <- 100 * (1 - taille_parquet / taille_csv)
      log_message(sprintf("    Gain Parquet vs CSV : %.1f%%", gain))
    }
  }
  
  # ============================================================
  # RAPPORT FINAL
  # ============================================================
  
  log_message(paste0("\n", paste(rep("=", 70), collapse = "")))
  log_message("RAPPORT FINAL")
  log_message(paste(rep("=", 70), collapse = ""))
  
  log_message(sprintf("Fichiers traités : %d / %d", length(stats_globales), length(fichiers_tous)))
  log_message(sprintf("Lignes totales : %s", format(nrow(base_finale), big.mark = ",")))
  log_message(sprintf("Colonnes : %d", ncol(base_finale)))
  
  log_message("\n✓✓✓ CONSOLIDATION TERMINÉE AVEC SUCCÈS ✓✓✓")
  
} else {
  log_message("\n✗✗✗ ERREUR : Aucun fichier traité ✗✗✗")
}

close(log_conn)

cat(paste0("\n", paste(rep("=", 70), collapse = ""), "\n"))
cat("✓ SCRIPT TERMINÉ\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")
cat(sprintf("📋 Log : %s\n", log_file))
cat(sprintf("📊 Résultats : %s\n", OUTPUT_DIR))
cat("\n")




library(arrow)
base <- read_parquet("~/consolidation_solde_2015_2025/03_data_output/base_finale/base_consolidee_2015_2025.parquet")

# Vérifier
nrow(base)  # 31,871,913
ncol(base)  # 425
names(base)  # Affiche les noms de colonnes
