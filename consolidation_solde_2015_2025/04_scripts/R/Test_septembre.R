# ============================================================
# EXTRACTION SEPTEMBRE 2025 - FICHIER EXCEL COMPLET
# ============================================================

library(data.table)
library(arrow)
library(openxlsx)

PROJECT_ROOT <- path.expand("~/consolidation_solde_2015_2025")
BASE_FILE <- file.path(PROJECT_ROOT, "03_data_output/base_finale/base_selectionnee_2015_2025.parquet")
OUTPUT_DIR <- file.path(PROJECT_ROOT, "03_data_output/export")
OUTPUT_FILE <- file.path(OUTPUT_DIR, "donnees_septembre_2025_2.xlsx")

dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

GRADES_VALIDES <- c(
  paste0("A", 1:7),
  paste0("B", 1:6),
  paste0("C", 1:5),
  paste0("D", 1:3)
)

cat("\n", strrep("=", 70), "\n")
cat("EXTRACTION SEPTEMBRE 2025 EN EXCEL\n")
cat(strrep("=", 70), "\n\n")

# ============================================================
# FONCTIONS
# ============================================================

extraire_grade <- function(statut) {
  statut <- as.character(statut)
  statut[is.na(statut)] <- ""
  
  grade <- rep("NF", length(statut))
  
  for (i in seq_along(statut)) {
    if (nchar(statut[i]) > 0 && grepl("Catégorie", statut[i], ignore.case = TRUE)) {
      match <- regmatches(statut[i], regexpr("Catégorie\\s+([ABCD][0-9]*)", statut[i], ignore.case = TRUE))
      if (length(match) > 0) {
        grade_part <- sub(".*Catégorie\\s+", "", match, ignore.case = TRUE)
        grade[i] <- toupper(trimws(grade_part))
      }
    }
  }
  
  return(grade)
}

# ============================================================
# CHARGEMENT
# ============================================================

cat("1. Chargement de la base complète...\n")
base <- as.data.table(read_parquet(BASE_FILE))
cat(sprintf("   ✓ %s lignes totales\n\n", format(nrow(base), big.mark = ",")))

# ============================================================
# EXTRACTION SEPTEMBRE 2025
# ============================================================

cat("2. Extraction septembre 2025...\n")

# Extraire année et mois
base[, `:=`(
  ANNEE = as.integer(substr(periode, 3, 6)),
  MOIS_NUM = as.integer(substr(periode, 1, 2))
)]

# Filtrer septembre 2025
base_sept <- base[ANNEE == 2025 & MOIS_NUM == 9]

cat(sprintf("   ✓ %s lignes pour septembre 2025\n\n", format(nrow(base_sept), big.mark = ",")))

if (nrow(base_sept) == 0) {
  stop("❌ Aucune donnée pour septembre 2025 !")
}

# ============================================================
# PRÉPARATION DES DONNÉES
# ============================================================

cat("3. Préparation des données...\n")

# Extraire grade
base_sept[, GRADE := extraire_grade(statut_fonctionnaire)]

# Calculer salaire brut
composantes <- intersect(c("124", "228", "271", "421"), names(base_sept))
for (comp in composantes) {
  base_sept[, (comp) := as.numeric(get(comp))]
}

base_sept[, SALAIRE_BRUT := rowSums(.SD, na.rm = TRUE), .SDcols = composantes]

# Nombre de postes par matricule
base_sept[, NB_POSTES := .N, by = matricule]

# Standardiser sexe
if ("sexe" %in% names(base_sept)) {
  base_sept[, SEXE_STD := {
    s <- toupper(trimws(as.character(sexe)))
    s <- gsub("É", "E", s)
    ifelse(s %in% c("MASCULIN","M","H","HOMME","MALE","1"), "Homme",
    ifelse(s %in% c("FEMININ","F","FEMME","FEMALE","2"), "Femme", "Inconnu"))
  }]
}

cat(sprintf("   ✓ GRADE extrait\n"))
cat(sprintf("   ✓ SALAIRE_BRUT calculé (%s)\n", paste(composantes, collapse = "+")))
cat(sprintf("   ✓ NB_POSTES calculé\n\n"))

# ============================================================
# SÉLECTION DES COLONNES À EXPORTER
# ============================================================

cat("4. Sélection des colonnes pour export...\n")

# Colonnes principales
colonnes_principales <- c(
  "periode", "cle_unique", "matricule", "nom", "grade_2",
  "date_naissance", "sexe", "SEXE_STD",
  "organisme", "CODE_ORGANISME", 
  "lieu_affectation", "CODE_AFFECTATION",
  "service", "CODE_SERVICE",
  "emploi", "CODE_EMPLOI",
  "fonction", "CODE_FONCTION",
  "statut_fonctionnaire", "GRADE",
  "Code_CITP", "Metier_CITP",
  "124", "228", "271", "421","425",
  "SALAIRE_BRUT", "NB_POSTES"
)

# Ne garder que les colonnes qui existent
colonnes_disponibles <- intersect(colonnes_principales, names(base_sept))

base_export <- base_sept[, ..colonnes_disponibles]

cat(sprintf("   ✓ %d colonnes sélectionnées\n\n", length(colonnes_disponibles)))

# ============================================================
# CRÉATION DES DIFFÉRENTES FEUILLES
# ============================================================

cat("5. Préparation des feuilles Excel...\n")

# FEUILLE 1 : Toutes les lignes (peut être très gros, limiter à 1M lignes pour Excel)
if (nrow(base_export) > 1000000) {
  cat("   ⚠️ Plus de 1M lignes, création de feuilles séparées par grade...\n")
  feuille_complete <- base_export[1:1000000]  # Limite Excel
} else {
  feuille_complete <- base_export
}

# FEUILLE 2 : Uniquement grades valides
feuille_grades_valides <- base_export[GRADE %chin% GRADES_VALIDES]

# FEUILLE 3 : Résumé par grade
resume_grade <- base_export[SALAIRE_BRUT > 0, .(
  Nb_lignes = .N,
  Nb_matricules_uniques = uniqueN(matricule),
  Nb_cle_unique = uniqueN(cle_unique),
  Salaire_brut_moyen = mean(SALAIRE_BRUT, na.rm = TRUE),
  Salaire_brut_median = median(SALAIRE_BRUT, na.rm = TRUE),
  Salaire_brut_min = min(SALAIRE_BRUT, na.rm = TRUE),
  Salaire_brut_max = max(SALAIRE_BRUT, na.rm = TRUE)
), by = GRADE]

setorder(resume_grade, GRADE)

# FEUILLE 4 : Multi-postes uniquement
multi_postes <- base_export[NB_POSTES > 1]

# FEUILLE 5 : Par grade (un exemple : A1)
if (nrow(base_export[GRADE == "A1"]) > 0) {
  feuille_a1 <- base_export[GRADE == "A1"]
} else {
  feuille_a1 <- NULL
}

cat(sprintf("   ✓ Feuilles préparées\n\n"))

# ============================================================
# EXPORT EXCEL
# ============================================================

cat("6. Création du fichier Excel...\n")

wb <- createWorkbook()

# Style en-tête
hs <- createStyle(
  fontSize = 11, 
  fontColour = "#FFFFFF", 
  halign = "center",
  fgFill = "#4F81BD", 
  textDecoration = "bold",
  wrapText = TRUE
)

# Fonction pour ajouter une feuille
add_sheet <- function(nom, data, largeur_colonnes = NULL) {
  if (is.null(data) || nrow(data) == 0) {
    cat(sprintf("   ⚠️ Feuille '%s' vide, ignorée\n", nom))
    return(invisible())
  }
  
  addWorksheet(wb, nom)
  writeData(wb, nom, data)
  addStyle(wb, nom, hs, rows = 1, cols = 1:ncol(data), gridExpand = TRUE)
  freezePane(wb, nom, firstRow = TRUE)
  
  # Largeur colonnes automatique
  if (is.null(largeur_colonnes)) {
    setColWidths(wb, nom, cols = 1:ncol(data), widths = "auto")
  }
  
  cat(sprintf("   ✓ Feuille '%s' : %s lignes × %d colonnes\n", 
             nom, format(nrow(data), big.mark = ","), ncol(data)))
}

# Ajouter les feuilles
add_sheet("Resume_par_Grade", resume_grade)
add_sheet("Grades_Valides", feuille_grades_valides)
add_sheet("Multi_Postes", multi_postes)

# Si pas trop de lignes, ajouter feuille complète
if (nrow(base_export) <= 1000000) {
  add_sheet("Donnees_Completes", feuille_complete)
} else {
  cat(sprintf("   ⚠️ %s lignes > limite Excel (1M), feuille complète non créée\n", 
             format(nrow(base_export), big.mark = ",")))
  cat("   💡 Création de feuilles par grade à la place...\n")
  
  # Créer une feuille par grade (top 5 grades)
  top_grades <- resume_grade[order(-Nb_lignes)][1:min(5, .N)]$GRADE
  
  for (g in top_grades) {
    feuille_grade <- base_export[GRADE == g]
    if (nrow(feuille_grade) <= 1000000) {
      add_sheet(paste0("Grade_", g), feuille_grade)
    } else {
      cat(sprintf("   ⚠️ Grade %s : %s lignes > 1M, échantillon de 100k lignes\n", 
                 g, format(nrow(feuille_grade), big.mark = ",")))
      add_sheet(paste0("Grade_", g, "_Echantillon"), feuille_grade[1:100000])
    }
  }
}

# Ajouter feuille info
info <- data.table(
  Information = c(
    "Date extraction",
    "Période",
    "Année",
    "Mois",
    "Nombre total de lignes",
    "Nombre de matricules uniques",
    "Nombre de cle_unique",
    "Composantes salaire",
    "Nombre de grades différents",
    "Multi-postes (%)"
  ),
  Valeur = c(
    as.character(Sys.Date()),
    "092025",
    "2025",
    "9 (Septembre)",
    format(nrow(base_sept), big.mark = ","),
    format(uniqueN(base_sept$matricule), big.mark = ","),
    format(uniqueN(base_sept$cle_unique), big.mark = ","),
    paste(composantes, collapse = " + "),
    as.character(uniqueN(base_sept$GRADE)),
    sprintf("%.2f%%", 100 * uniqueN(base_sept[NB_POSTES > 1]$matricule) / uniqueN(base_sept$matricule))
  )
)

add_sheet("Informations", info)

# Sauvegarder
saveWorkbook(wb, OUTPUT_FILE, overwrite = TRUE)

taille_mb <- file.info(OUTPUT_FILE)$size / 1024^2

cat(sprintf("\n✅ Fichier créé : %s\n", OUTPUT_FILE))
cat(sprintf("   Taille : %.1f MB\n", taille_mb))
cat(sprintf("   Feuilles : %d\n\n", length(names(wb))))

cat(strrep("=", 70), "\n")
cat("✅ TERMINÉ\n")
cat(strrep("=", 70), "\n\n")

cat("📊 FEUILLES CRÉÉES :\n\n")
cat("  • Informations : Métadonnées sur l'extraction\n")
cat("  • Resume_par_Grade : Statistiques agrégées par grade\n")
cat("  • Grades_Valides : Toutes lignes avec grades valides\n")
cat("  • Multi_Postes : Agents avec plusieurs postes\n")
if (nrow(base_export) <= 1000000) {
  cat("  • Donnees_Completes : Toutes les lignes de septembre 2025\n")
} else {
  cat("  • Grade_XX : Feuilles par grade (top 5 grades)\n")
}
cat("\n")

cat("📂 Pour ouvrir le fichier :\n")
cat(sprintf("   libreoffice %s\n", OUTPUT_FILE))
cat("\n")

cat("💡 COLONNES EXPORTÉES :\n")
for (col in names(base_export)) {
  cat(sprintf("   • %s\n", col))
}
cat("\n")

cat(strrep("=", 70), "\n")