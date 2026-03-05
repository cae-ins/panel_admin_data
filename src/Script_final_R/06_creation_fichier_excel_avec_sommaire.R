# ============================================================
# CRÉATION FICHIER EXCEL AVEC SOMMAIRE STRUCTURÉ (4 SECTIONS)
# Inspiré du code CITP avec grands groupes
# ============================================================

library(data.table)
library(openxlsx)

# ============================================================
# PARAMÈTRES DE CONFIGURATION
# (doivent correspondre exactement à ceux de 06_calcul_indicateur.R)
# ============================================================

# TRUE  = Salaire brut (cle_unique) | FALSE = Revenu salarial (matricule)
MODE_SALAIRE_BRUT <- TRUE

# TRUE  = 2024-2025 | FALSE = 2015 à l'année la plus récente disponible
PERIODE_RECENTE <- TRUE

# TRUE  = avec zéros | FALSE = salaire strictement > 0
INCLURE_ZEROS <- FALSE

# ============================================================

PROJECT_ROOT <- path.expand("~/consolidation_solde_2015_2025")

.mode_label    <- ifelse(MODE_SALAIRE_BRUT, "salaire_brut", "revenu_salarial")
.periode_label <- ifelse(PERIODE_RECENTE, "2024_2025", "2015_recent")
.zeros_label   <- ifelse(INCLURE_ZEROS, "avec_zeros", "sans_zeros")
.base_name     <- paste0("indicateurs_", .mode_label, "_", .periode_label, "_", .zeros_label)

INPUT_FILE  <- file.path(PROJECT_ROOT, "03_data_output/export", paste0(.base_name, ".xlsx"))
OUTPUT_FILE <- file.path(PROJECT_ROOT, "03_data_output/export", paste0(.base_name, "_SOMMAIRE.xlsx"))

cat("\n", strrep("=", 70), "\n")
cat("CRÉATION FICHIER AVEC SOMMAIRE STRUCTURÉ (4 SECTIONS)\n")
cat(strrep("=", 70), "\n\n")

# ============================================================
# VÉRIFICATION & CHARGEMENT
# ============================================================

if (!file.exists(INPUT_FILE)) {
  stop("❌ Fichier introuvable : ", INPUT_FILE)
}

cat("✓ Chargement du fichier source...\n")
cat(sprintf("  %s\n\n", INPUT_FILE))

wb_source <- loadWorkbook(INPUT_FILE)
feuilles_existantes <- names(wb_source)

cat(sprintf("Feuilles disponibles (%d) :\n", length(feuilles_existantes)))
for (f in feuilles_existantes) {
  cat(sprintf("  • %s\n", f))
}
cat("\n")

# ============================================================
# LECTURE DES DONNÉES
# ============================================================

cat("📖 Lecture des feuilles...\n")

dt_citp <- NULL
dt_grade <- NULL
dt_grade_sexe <- NULL
dt_multi <- NULL

if ("REVENU_CITP_Detail" %in% feuilles_existantes) {
  dt_citp <- as.data.table(read.xlsx(INPUT_FILE, sheet = "REVENU_CITP_Detail"))
  cat(sprintf("  ✓ CITP : %d lignes\n", nrow(dt_citp)))
}

if ("REVENU_Grade_Detail" %in% feuilles_existantes) {
  dt_grade <- as.data.table(read.xlsx(INPUT_FILE, sheet = "REVENU_Grade_Detail"))
  cat(sprintf("  ✓ Grade : %d lignes\n", nrow(dt_grade)))
}

if ("REVENU_Grade_Sexe" %in% feuilles_existantes) {
  dt_grade_sexe <- as.data.table(read.xlsx(INPUT_FILE, sheet = "REVENU_Grade_Sexe"))
  cat(sprintf("  ✓ Grade-Sexe : %d lignes\n", nrow(dt_grade_sexe)))
}

if ("MULTI_Par_Grade_NbPostes" %in% feuilles_existantes) {
  dt_multi <- as.data.table(read.xlsx(INPUT_FILE, sheet = "MULTI_Par_Grade_NbPostes"))
  cat(sprintf("  ✓ Multi-postes : %d lignes\n", nrow(dt_multi)))
}

cat("\n")

# ============================================================
# CLASSIFICATION CITP (GRANDS GROUPES)
# ============================================================

extract_grand_groupe <- function(code_citp) {
  code_str <- as.character(code_citp)
  premier_chiffre <- substr(gsub("[^0-9]", "", code_str), 1, 1)
  grand_groupe <- as.integer(premier_chiffre)
  grand_groupe[is.na(grand_groupe) | grand_groupe < 1 | grand_groupe > 9] <- NA_integer_
  return(grand_groupe)
}

CITP_GRANDS_GROUPES <- data.frame(
  Code = 1:9,
  Titre = c(
    "1 - Directeurs, cadres de direction et gérants",
    "2 - Professions intellectuelles et scientifiques",
    "3 - Professions intermédiaires",
    "4 - Employés de type administratif",
    "5 - Personnel des services directs aux particuliers, commerçants et vendeurs",
    "6 - Agriculteurs et ouvriers qualifiés de l'agriculture, de la sylviculture et de la pêche",
    "7 - Métiers qualifiés de l'industrie et de l'artisanat",
    "8 - Conducteurs d'installations et de machines, et ouvriers de l'assemblage",
    "9 - Professions élémentaires"
  ),
  stringsAsFactors = FALSE
)

# ============================================================
# PRÉPARATION DES DONNÉES PAR SECTION
# ============================================================

cat("🔨 Préparation des données par section...\n")

# SECTION 1 : CITP PAR GRAND GROUPE
if (!is.null(dt_citp)) {
  dt_citp[, GRAND_GROUPE := extract_grand_groupe(CODE_CITP)]
  
  stats_citp <- dt_citp[, .(
    Effectif_total = sum(Effectif, na.rm = TRUE),
    Nb_metiers = uniqueN(CODE_CITP)
  ), by = GRAND_GROUPE]
  
  cat(sprintf("  ✓ CITP : %d grands groupes\n", nrow(stats_citp)))
}

# SECTION 2 : PAR GRADE
if (!is.null(dt_grade)) {
  stats_grade <- dt_grade[, .(
    Effectif_total = sum(Effectif, na.rm = TRUE),
    Revenu_moyen_global = weighted.mean(Revenu_moyen, Effectif, na.rm = TRUE)
  ), by = GRADE_PRINCIPAL]
  
  cat(sprintf("  ✓ Grade : %d grades\n", nrow(stats_grade)))
}

# SECTION 3 : PAR SEXE
if (!is.null(dt_grade_sexe)) {
  stats_sexe <- dt_grade_sexe[, .(
    Effectif_total = sum(Effectif, na.rm = TRUE),
    Revenu_moyen_global = weighted.mean(Revenu_moyen, Effectif, na.rm = TRUE)
  ), by = SEXE_STD]
  
  cat(sprintf("  ✓ Sexe : %d catégories\n", nrow(stats_sexe)))
}

# SECTION 4 : MULTI-POSTES PAR GRADE
if (!is.null(dt_multi)) {
  grades_multi <- unique(dt_multi$GRADE_PRINCIPAL)
  cat(sprintf("  ✓ Multi-postes : %d grades\n", length(grades_multi)))
}

cat("\n")

# ============================================================
# CRÉATION DU NOUVEAU WORKBOOK
# ============================================================

cat("📄 Création du workbook...\n")

wb <- createWorkbook()

# ============================================================
# STYLES
# ============================================================

styleTitle <- createStyle(
  fontSize = 16,
  fontColour = "#FFFFFF",
  fgFill = "#1F4E78",
  halign = "center",
  valign = "center",
  textDecoration = "bold",
  border = "TopBottomLeftRight"
)

styleSubTitle <- createStyle(
  fontSize = 14,
  fontColour = "#FFFFFF",
  fgFill = "#4472C4",
  halign = "center",
  valign = "center",
  textDecoration = "bold",
  border = "TopBottomLeftRight"
)

styleHeader <- createStyle(
  fontSize = 11,
  fontColour = "#FFFFFF",
  fgFill = "#4F81BD",
  halign = "center",
  valign = "center",
  textDecoration = "bold",
  border = "TopBottomLeftRight"
)

styleLien <- createStyle(
  fontSize = 12,
  fontColour = "#0563C1",
  textDecoration = "underline"
)

# ============================================================
# FEUILLE SOMMAIRE
# ============================================================

cat("📋 Création du SOMMAIRE...\n")

addWorksheet(wb, "SOMMAIRE")

# Titre principal
.titre_sommaire <- sprintf(
  "INDICATEURS SALAIRES — %s | %s | %s",
  ifelse(MODE_SALAIRE_BRUT, "Salaire brut", "Revenu salarial"),
  ifelse(PERIODE_RECENTE, "2024-2025", "2015 à récent"),
  ifelse(INCLURE_ZEROS, "avec zéros", "sans zéros")
)
writeData(wb, "SOMMAIRE", .titre_sommaire,
          startRow = 1, startCol = 1)
mergeCells(wb, "SOMMAIRE", cols = 1:4, rows = 1)
addStyle(wb, "SOMMAIRE", styleTitle, rows = 1, cols = 1:4, gridExpand = TRUE)

writeData(wb, "SOMMAIRE", "Cliquez sur une section pour accéder aux détails", 
          startRow = 2, startCol = 1)
mergeCells(wb, "SOMMAIRE", cols = 1:4, rows = 2)

current_row <- 4

# ============================================================
# SECTION 1 : CLASSIFICATION PAR GRAND GROUPE CITP
# ============================================================

if (!is.null(dt_citp)) {
  writeData(wb, "SOMMAIRE", "📊 SECTION 1 : CLASSIFICATION PAR GRAND GROUPE CITP", 
            startRow = current_row, startCol = 1)
  mergeCells(wb, "SOMMAIRE", cols = 1:4, rows = current_row)
  addStyle(wb, "SOMMAIRE", styleSubTitle, rows = current_row, cols = 1:4, gridExpand = TRUE)
  current_row <- current_row + 1
  
  # En-têtes
  writeData(wb, "SOMMAIRE", 
            data.frame(
              Code = "Code",
              Titre = "Grand Groupe CITP",
              Effectif = "Effectif total",
              Metiers = "Nb métiers"
            ), 
            startRow = current_row, startCol = 1)
  addStyle(wb, "SOMMAIRE", styleHeader, rows = current_row, cols = 1:4, gridExpand = TRUE)
  current_row <- current_row + 1
  
  # Lignes des grands groupes
  for (i in 1:9) {
    gg_titre <- CITP_GRANDS_GROUPES$Titre[i]
    gg_stats <- stats_citp[GRAND_GROUPE == i]
    
    effectif <- ifelse(nrow(gg_stats) > 0, gg_stats$Effectif_total, 0)
    nb_metiers <- ifelse(nrow(gg_stats) > 0, gg_stats$Nb_metiers, 0)
    
    writeData(wb, "SOMMAIRE", i, startRow = current_row, startCol = 1)
    writeData(wb, "SOMMAIRE", gg_titre, startRow = current_row, startCol = 2)
    writeData(wb, "SOMMAIRE", effectif, startRow = current_row, startCol = 3)
    writeData(wb, "SOMMAIRE", nb_metiers, startRow = current_row, startCol = 4)
    
    # Hyperlien
    sheet_name <- paste0("CITP_GG", i)
    writeFormula(wb, "SOMMAIRE", 
                 startRow = current_row, 
                 startCol = 2,
                 x = makeHyperlinkString(
                   sheet = sheet_name, 
                   row = 1, 
                   col = 1, 
                   text = gg_titre
                 ))
    
    addStyle(wb, "SOMMAIRE", styleLien, rows = current_row, cols = 2)
    current_row <- current_row + 1
  }
  
  current_row <- current_row + 2
}

# ============================================================
# SECTION 2 : PAR GRADE
# ============================================================

if (!is.null(dt_grade)) {
  writeData(wb, "SOMMAIRE", "📊 SECTION 2 : CLASSIFICATION PAR GRADE", 
            startRow = current_row, startCol = 1)
  mergeCells(wb, "SOMMAIRE", cols = 1:4, rows = current_row)
  addStyle(wb, "SOMMAIRE", styleSubTitle, rows = current_row, cols = 1:4, gridExpand = TRUE)
  current_row <- current_row + 1
  
  # En-têtes
  writeData(wb, "SOMMAIRE", 
            data.frame(
              Grade = "Grade",
              Description = "Description",
              Effectif = "Effectif total",
              RevenuMoyen = "Revenu moyen"
            ), 
            startRow = current_row, startCol = 1)
  addStyle(wb, "SOMMAIRE", styleHeader, rows = current_row, cols = 1:4, gridExpand = TRUE)
  current_row <- current_row + 1
  
  # Lignes par grade
  setorder(stats_grade, GRADE_PRINCIPAL)
  
  for (i in 1:nrow(stats_grade)) {
    grade <- stats_grade$GRADE_PRINCIPAL[i]
    effectif <- stats_grade$Effectif_total[i]
    revenu_moy <- round(stats_grade$Revenu_moyen_global[i])
    
    grade_desc <- ifelse(grepl("^A", grade), "Catégorie A",
                  ifelse(grepl("^B", grade), "Catégorie B",
                  ifelse(grepl("^C", grade), "Catégorie C",
                  ifelse(grepl("^D", grade), "Catégorie D", "Autre"))))
    
    writeData(wb, "SOMMAIRE", grade, startRow = current_row, startCol = 1)
    writeData(wb, "SOMMAIRE", grade_desc, startRow = current_row, startCol = 2)
    writeData(wb, "SOMMAIRE", effectif, startRow = current_row, startCol = 3)
    writeData(wb, "SOMMAIRE", revenu_moy, startRow = current_row, startCol = 4)
    
    # Hyperlien
    sheet_name <- paste0("GRADE_", grade)
    writeFormula(wb, "SOMMAIRE", 
                 startRow = current_row, 
                 startCol = 1,
                 x = makeHyperlinkString(
                   sheet = sheet_name, 
                   row = 1, 
                   col = 1, 
                   text = grade
                 ))
    
    addStyle(wb, "SOMMAIRE", styleLien, rows = current_row, cols = 1)
    current_row <- current_row + 1
  }
  
  current_row <- current_row + 2
}

# ============================================================
# SECTION 3 : PAR SEXE
# ============================================================

if (!is.null(dt_grade_sexe)) {
  writeData(wb, "SOMMAIRE", "📊 SECTION 3 : CLASSIFICATION PAR SEXE", 
            startRow = current_row, startCol = 1)
  mergeCells(wb, "SOMMAIRE", cols = 1:4, rows = current_row)
  addStyle(wb, "SOMMAIRE", styleSubTitle, rows = current_row, cols = 1:4, gridExpand = TRUE)
  current_row <- current_row + 1
  
  # En-têtes
  writeData(wb, "SOMMAIRE", 
            data.frame(
              Sexe = "Sexe",
              Description = "Description",
              Effectif = "Effectif total",
              RevenuMoyen = "Revenu moyen"
            ), 
            startRow = current_row, startCol = 1)
  addStyle(wb, "SOMMAIRE", styleHeader, rows = current_row, cols = 1:4, gridExpand = TRUE)
  current_row <- current_row + 1
  
  # Lignes par sexe
  for (i in 1:nrow(stats_sexe)) {
    sexe <- stats_sexe$SEXE_STD[i]
    effectif <- stats_sexe$Effectif_total[i]
    revenu_moy <- round(stats_sexe$Revenu_moyen_global[i])
    
    desc <- ifelse(sexe == "Homme", "Agents masculins",
           ifelse(sexe == "Femme", "Agents féminins", "Non renseigné"))
    
    writeData(wb, "SOMMAIRE", sexe, startRow = current_row, startCol = 1)
    writeData(wb, "SOMMAIRE", desc, startRow = current_row, startCol = 2)
    writeData(wb, "SOMMAIRE", effectif, startRow = current_row, startCol = 3)
    writeData(wb, "SOMMAIRE", revenu_moy, startRow = current_row, startCol = 4)
    
    # Hyperlien
    sheet_name <- paste0("SEXE_", sexe)
    writeFormula(wb, "SOMMAIRE", 
                 startRow = current_row, 
                 startCol = 1,
                 x = makeHyperlinkString(
                   sheet = sheet_name, 
                   row = 1, 
                   col = 1, 
                   text = sexe
                 ))
    
    addStyle(wb, "SOMMAIRE", styleLien, rows = current_row, cols = 1)
    current_row <- current_row + 1
  }
  
  current_row <- current_row + 2
}

# ============================================================
# SECTION 4 : MULTI-POSTES PAR GRADE
# ============================================================

if (!is.null(dt_multi)) {
  writeData(wb, "SOMMAIRE", "📊 SECTION 4 : MULTI-POSTES PAR GRADE", 
            startRow = current_row, startCol = 1)
  mergeCells(wb, "SOMMAIRE", cols = 1:4, rows = current_row)
  addStyle(wb, "SOMMAIRE", styleSubTitle, rows = current_row, cols = 1:4, gridExpand = TRUE)
  current_row <- current_row + 1
  
  # En-têtes
  writeData(wb, "SOMMAIRE", 
            data.frame(
              Grade = "Grade",
              Description = "Description",
              MonoPoste = "Mono-postes",
              MultiPoste = "Multi-postes"
            ), 
            startRow = current_row, startCol = 1)
  addStyle(wb, "SOMMAIRE", styleHeader, rows = current_row, cols = 1:4, gridExpand = TRUE)
  current_row <- current_row + 1
  
  # Stats multi-postes par grade
  for (grade in sort(unique(dt_multi$GRADE_PRINCIPAL))) {
    dt_g <- dt_multi[GRADE_PRINCIPAL == grade]
    
    mono <- sum(dt_g[["1"]], na.rm = TRUE)
    multi <- sum(dt_g[["2"]], na.rm = TRUE) + 
             sum(dt_g[["3"]], na.rm = TRUE) + 
             sum(dt_g[["4"]], na.rm = TRUE)
    
    if ("5+" %in% names(dt_g)) {
      multi <- multi + sum(dt_g[["5+"]], na.rm = TRUE)
    }
    
    grade_desc <- ifelse(grepl("^A", grade), "Catégorie A",
                  ifelse(grepl("^B", grade), "Catégorie B",
                  ifelse(grepl("^C", grade), "Catégorie C",
                  ifelse(grepl("^D", grade), "Catégorie D", "Autre"))))
    
    writeData(wb, "SOMMAIRE", grade, startRow = current_row, startCol = 1)
    writeData(wb, "SOMMAIRE", grade_desc, startRow = current_row, startCol = 2)
    writeData(wb, "SOMMAIRE", mono, startRow = current_row, startCol = 3)
    writeData(wb, "SOMMAIRE", multi, startRow = current_row, startCol = 4)
    
    # Hyperlien
    sheet_name <- paste0("MULTI_", grade)
    writeFormula(wb, "SOMMAIRE", 
                 startRow = current_row, 
                 startCol = 1,
                 x = makeHyperlinkString(
                   sheet = sheet_name, 
                   row = 1, 
                   col = 1, 
                   text = grade
                 ))
    
    addStyle(wb, "SOMMAIRE", styleLien, rows = current_row, cols = 1)
    current_row <- current_row + 1
  }
}

# Formatage sommaire
setColWidths(wb, "SOMMAIRE", cols = 1, widths = 15)
setColWidths(wb, "SOMMAIRE", cols = 2, widths = 70)
setColWidths(wb, "SOMMAIRE", cols = 3:4, widths = 18)
setRowHeights(wb, "SOMMAIRE", rows = 1, heights = 30)

cat("  ✓ SOMMAIRE créé\n\n")

# ============================================================
# FEUILLES DÉTAIL PAR SECTION
# ============================================================

cat("📄 Création des feuilles détail...\n")

# SECTION 1 : Feuilles CITP par grand groupe
if (!is.null(dt_citp)) {
  for (i in 1:9) {
    sheet_name <- paste0("CITP_GG", i)
    gg_titre <- CITP_GRANDS_GROUPES$Titre[i]
    gg_data <- dt_citp[GRAND_GROUPE == i]
    
    if (nrow(gg_data) > 0) {
      addWorksheet(wb, sheet_name)
      
      # Titre
      writeData(wb, sheet_name, gg_titre, startRow = 1, startCol = 1)
      mergeCells(wb, sheet_name, cols = 1:10, rows = 1)
      addStyle(wb, sheet_name, styleTitle, rows = 1, cols = 1:10, gridExpand = TRUE)
      
      # Lien retour
      writeFormula(wb, sheet_name,
                   startRow = 2,
                   startCol = 1,
                   x = makeHyperlinkString(
                     sheet = "SOMMAIRE",
                     row = 1,
                     col = 1,
                     text = "← Retour au sommaire"
                   ))
      addStyle(wb, sheet_name, styleLien, rows = 2, cols = 1)
      
      # Données
      writeData(wb, sheet_name, gg_data, startRow = 4, startCol = 1)
      addStyle(wb, sheet_name, styleHeader, rows = 4, cols = 1:ncol(gg_data), gridExpand = TRUE)
      freezePane(wb, sheet_name, firstRow = TRUE)
      
      cat(sprintf("  ✓ %s\n", sheet_name))
    }
  }
}

# SECTION 2 : Feuilles par GRADE
if (!is.null(dt_grade)) {
  for (grade in unique(dt_grade$GRADE_PRINCIPAL)) {
    sheet_name <- paste0("GRADE_", grade)
    grade_data <- dt_grade[GRADE_PRINCIPAL == grade]
    
    addWorksheet(wb, sheet_name)
    
    writeData(wb, sheet_name, paste("GRADE", grade), startRow = 1, startCol = 1)
    mergeCells(wb, sheet_name, cols = 1:8, rows = 1)
    addStyle(wb, sheet_name, styleTitle, rows = 1, cols = 1:8, gridExpand = TRUE)
    
    writeFormula(wb, sheet_name,
                 startRow = 2,
                 startCol = 1,
                 x = makeHyperlinkString(
                   sheet = "SOMMAIRE",
                   row = 1,
                   col = 1,
                   text = "← Retour au sommaire"
                 ))
    addStyle(wb, sheet_name, styleLien, rows = 2, cols = 1)
    
    writeData(wb, sheet_name, grade_data, startRow = 4, startCol = 1)
    addStyle(wb, sheet_name, styleHeader, rows = 4, cols = 1:ncol(grade_data), gridExpand = TRUE)
    freezePane(wb, sheet_name, firstRow = TRUE)
    
    cat(sprintf("  ✓ %s\n", sheet_name))
  }
}

# SECTION 3 : Feuilles par SEXE
if (!is.null(dt_grade_sexe)) {
  for (sexe in unique(dt_grade_sexe$SEXE_STD)) {
    sheet_name <- paste0("SEXE_", sexe)
    sexe_data <- dt_grade_sexe[SEXE_STD == sexe]
    
    addWorksheet(wb, sheet_name)
    
    writeData(wb, sheet_name, paste("SEXE:", sexe), startRow = 1, startCol = 1)
    mergeCells(wb, sheet_name, cols = 1:8, rows = 1)
    addStyle(wb, sheet_name, styleTitle, rows = 1, cols = 1:8, gridExpand = TRUE)
    
    writeFormula(wb, sheet_name,
                 startRow = 2,
                 startCol = 1,
                 x = makeHyperlinkString(
                   sheet = "SOMMAIRE",
                   row = 1,
                   col = 1,
                   text = "← Retour au sommaire"
                 ))
    addStyle(wb, sheet_name, styleLien, rows = 2, cols = 1)
    
    writeData(wb, sheet_name, sexe_data, startRow = 4, startCol = 1)
    addStyle(wb, sheet_name, styleHeader, rows = 4, cols = 1:ncol(sexe_data), gridExpand = TRUE)
    freezePane(wb, sheet_name, firstRow = TRUE)
    
    cat(sprintf("  ✓ %s\n", sheet_name))
  }
}

# SECTION 4 : Feuilles MULTI-POSTES par GRADE
if (!is.null(dt_multi)) {
  for (grade in unique(dt_multi$GRADE_PRINCIPAL)) {
    sheet_name <- paste0("MULTI_", grade)
    multi_data <- dt_multi[GRADE_PRINCIPAL == grade]
    
    addWorksheet(wb, sheet_name)
    
    writeData(wb, sheet_name, paste("MULTI-POSTES:", grade), startRow = 1, startCol = 1)
    mergeCells(wb, sheet_name, cols = 1:7, rows = 1)
    addStyle(wb, sheet_name, styleTitle, rows = 1, cols = 1:7, gridExpand = TRUE)
    
    writeFormula(wb, sheet_name,
                 startRow = 2,
                 startCol = 1,
                 x = makeHyperlinkString(
                   sheet = "SOMMAIRE",
                   row = 1,
                   col = 1,
                   text = "← Retour au sommaire"
                 ))
    addStyle(wb, sheet_name, styleLien, rows = 2, cols = 1)
    
    writeData(wb, sheet_name, multi_data, startRow = 4, startCol = 1)
    addStyle(wb, sheet_name, styleHeader, rows = 4, cols = 1:ncol(multi_data), gridExpand = TRUE)
    freezePane(wb, sheet_name, firstRow = TRUE)
    
    cat(sprintf("  ✓ %s\n", sheet_name))
  }
}

cat("\n")

# ============================================================
# SAUVEGARDE
# ============================================================

cat("💾 Sauvegarde...\n")

saveWorkbook(wb, OUTPUT_FILE, overwrite = TRUE)

taille_mb <- file.info(OUTPUT_FILE)$size / 1024^2

cat(sprintf("\n✅ Fichier créé : %s\n", OUTPUT_FILE))
cat(sprintf("   Taille : %.1f MB\n", taille_mb))
cat(sprintf("   Feuilles : %d\n\n", length(names(wb))))

cat(strrep("=", 70), "\n")
cat("✅ TERMINÉ\n")
cat(strrep("=", 70), "\n\n")

cat("📊 STRUCTURE DU FICHIER :\n\n")
cat("FEUILLE SOMMAIRE (avec 4 sections cliquables) :\n")
cat("  • Section 1 : Classification par grand groupe CITP (9 groupes)\n")
cat("  • Section 2 : Classification par grade (A1-D3)\n")
cat("  • Section 3 : Classification par sexe (Homme/Femme)\n")
cat("  • Section 4 : Multi-postes par grade\n\n")

cat("💡 UTILISATION :\n")
cat("  1. Ouvrez le fichier Excel\n")
cat("  2. La feuille SOMMAIRE s'affiche avec 4 sections\n")
cat("  3. Cliquez sur n'importe quel élément (liens bleus)\n")
cat("  4. Utilisez '← Retour au sommaire' pour revenir\n\n")

cat(strrep("=", 70), "\n")