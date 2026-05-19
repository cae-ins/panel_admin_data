# ============================================================
# INDICATEURS SALAIRES - VERSION FINALE CORRIGÉE
# Filtrage AVANT agrégation
# ============================================================

rm(list = ls())
gc()

library(data.table)
library(arrow)
library(openxlsx)

setDTthreads(0L)

# ============================================================
# PARAMÈTRES DE CONFIGURATION
# ============================================================

# Mode de calcul des indicateurs de revenu :
#   TRUE  = Salaire brut      → agrégation par cle_unique (emploi individuel)
#   FALSE = Revenu salarial   → agrégation par matricule  (total par agent)
MODE_SALAIRE_BRUT <- TRUE

# Période d'analyse :
#   TRUE  = Années récentes uniquement (2024-2025)
#   FALSE = Période complète (2015 à l'année la plus récente disponible)
PERIODE_RECENTE <- TRUE

# Inclusion des valeurs nulles/zéro pour le salaire brut / revenu salarial :
#   TRUE  = Inclure les lignes avec salaire = 0 ou NA
#   FALSE = Exclure ces lignes (comportement par défaut, salaire strictement > 0)
INCLURE_ZEROS <- TRUE

# Méthode de correction des grades invalides :
#   FALSE = Historique Agent  → mode par (cle_unique, emploi) sur l'historique de l'agent
#   TRUE  = Emploi en cascade → 3 priorités : P1 mois courant, P2 année, P3 global
CORRECTION_PAR_EMPLOI <- TRUE

# ============================================================

PROJECT_ROOT <- path.expand("~/consolidation_solde_2015_2025")
BASE_FILE <- file.path(PROJECT_ROOT, "03_data_output/base_finale/base_selectionnee_2015_2025.parquet")
OUTPUT_DIR <- file.path(PROJECT_ROOT, "03_data_output/export")

dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

.mode_label    <- ifelse(MODE_SALAIRE_BRUT, "salaire_brut", "revenu_salarial")
.periode_label <- ifelse(PERIODE_RECENTE, "2024_2025", "2015_recent")
.zeros_label   <- ifelse(INCLURE_ZEROS, "avec_zeros", "sans_zeros")
OUTPUT_FILE <- file.path(OUTPUT_DIR, paste0("indicateurs_", .mode_label, "_", .periode_label, "_", .zeros_label, ".xlsx"))

MOIS_FR <- c(
  "1" = "Janvier", "2" = "Février", "3" = "Mars", "4" = "Avril",
  "5" = "Mai", "6" = "Juin", "7" = "Juillet", "8" = "Août",
  "9" = "Septembre", "10" = "Octobre", "11" = "Novembre", "12" = "Décembre"
)

GRADES_VALIDES <- c(
  paste0("A", 1:7),
  paste0("B", 1:6),
  paste0("C", 1:5),
  paste0("D", 1:3)
)

GRADES_INVALIDES <- c("B7", "C7", "D7", "A", "B", "C", "D")

cat(paste(rep("=", 70), collapse = ""), "\n")
cat(sprintf("INDICATEURS SALAIRES — Mode : %s | Période : %s\n",
            ifelse(MODE_SALAIRE_BRUT, "Salaire brut (cle_unique)", "Revenu salarial (matricule)"),
            ifelse(PERIODE_RECENTE, "2024-2025", "2015 à récent")))
cat(paste(rep("=", 70), collapse = ""), "\n\n")

# ============================================================
# FONCTIONS
# ============================================================

safe_mean   <- function(x) { x <- x[is.finite(x)]; if (length(x) == 0) NA_real_ else mean(x) }
safe_median <- function(x) { x <- x[is.finite(x)]; if (length(x) == 0) NA_real_ else median(x) }
safe_min    <- function(x) { x <- x[is.finite(x)]; if (length(x) == 0) NA_real_ else min(x) }
safe_max    <- function(x) { x <- x[is.finite(x)]; if (length(x) == 0) NA_real_ else max(x) }
safe_q      <- function(x, p) { x <- x[is.finite(x)]; if (length(x) == 0) NA_real_ else as.numeric(quantile(x, p, na.rm = TRUE)) }

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

grade_order_value <- function(grade) {
  out <- rep(0L, length(grade))
  
  valid <- grepl("^[ABCD][0-9]$", grade)
  if (any(valid)) {
    letter <- substr(grade[valid], 1, 1)
    digit  <- as.integer(substr(grade[valid], 2, 2))
    base   <- ifelse(letter == "A", 80L, 
              ifelse(letter == "B", 70L,
              ifelse(letter == "C", 60L, 50L)))
    out[valid] <- base + digit
  }
  
  out[grade == "A"] <- 84L
  out[grade == "B"] <- 73L
  out[grade == "C"] <- 63L
  out[grade == "D"] <- 52L
  
  out
}

# ============================================================
# CHARGEMENT
# ============================================================

cat("1. Chargement...\n")
base <- as.data.table(read_parquet(BASE_FILE))
cat(sprintf("   ✓ %s lignes\n\n", format(nrow(base), big.mark = ",")))

# ============================================================
# EXTRACTION ANNÉE/MOIS
# ============================================================

cat("2. Extraction année/mois...\n")

base[, `:=`(
  ANNEE = as.integer(substr(periode, 3, 6)),
  MOIS_NUM = as.integer(substr(periode, 1, 2))
)]

cat(sprintf("   Exemple : '%s' → %d/%d\n", base$periode[1], base$MOIS_NUM[1], base$ANNEE[1]))

annees_dispo <- sort(unique(base$ANNEE))
cat(sprintf("   Années disponibles : %s\n", paste(annees_dispo, collapse = ", ")))

# Définition de la période selon le paramètre PERIODE_RECENTE
if (PERIODE_RECENTE) {
  ANNEES_ANALYSE <- c(2024L, 2025L)
} else {
  ANNEES_ANALYSE <- annees_dispo[annees_dispo >= 2015L]
}
cat(sprintf("   Période retenue    : %s\n\n", paste(ANNEES_ANALYSE, collapse = ", ")))

# Filtrage
base <- base[ANNEE %in% ANNEES_ANALYSE]
cat(sprintf("   Filtrage %s : %s lignes\n\n",
           paste(range(ANNEES_ANALYSE), collapse = "-"),
           format(nrow(base), big.mark = ",")))

if (nrow(base) == 0) {
  stop("❌ Aucune donnée pour ", paste(ANNEES_ANALYSE, collapse = "-"))
}

base[, MOIS := MOIS_FR[as.character(MOIS_NUM)]]

# ============================================================
# EXTRACTION GRADE
# ============================================================

cat("3. Extraction grade depuis statut_fonctionnaire...\n")

base[, GRADE := extraire_grade(statut_fonctionnaire)]

n_nf <- base[GRADE == "NF", .N]
n_valides <- base[GRADE %chin% GRADES_VALIDES, .N]
n_invalides <- base[GRADE %chin% GRADES_INVALIDES, .N]

cat(sprintf("   Non fonctionnaires : %s (%.1f%%)\n",
           format(n_nf, big.mark = ","),
           100 * n_nf / nrow(base)))
cat(sprintf("   Grades valides : %s (%.1f%%)\n",
           format(n_valides, big.mark = ","),
           100 * n_valides / nrow(base)))
cat(sprintf("   Grades invalides : %s (%.1f%%)\n\n",
           format(n_invalides, big.mark = ","),
           100 * n_invalides / nrow(base)))

# Correction grades invalides
if (n_invalides > 0) {
  cat("   Correction grades invalides...\n")

  if (!CORRECTION_PAR_EMPLOI) {

    # ── CAS 1 : Historique Agent ─────────────────────────────────────────
    # Mode du grade valide calculé sur l'historique de l'agent (cle_unique)
    cat("   Mode : historique agent (cle_unique x emploi)\n")

    setorder(base, cle_unique, emploi, ANNEE, MOIS_NUM)

    base[, GRADE_MODAL := {
      gv <- GRADE[!(GRADE %chin% GRADES_INVALIDES)]
      if (length(gv) == 0L) NA_character_ else {
        tab <- table(gv)
        names(tab)[which.max(tab)]
      }
    }, by = .(cle_unique, emploi)]

    base[GRADE %chin% GRADES_INVALIDES & !is.na(GRADE_MODAL), GRADE := GRADE_MODAL]
    base[, GRADE_MODAL := NULL]

  } else {

    # ── CAS 2 : Emploi en Cascade (3 niveaux) ────────────────────────────
    cat("   Mode : emploi en cascade (P1: mois, P2: annee, P3: global)\n")

    # Fonction utilitaire : grade valide le plus frequent
    .mode_grade <- function(g) {
      if (length(g) == 0L) NA_character_ else names(which.max(table(g)))
    }

    # Lookup P1 : emploi x ANNEE x MOIS_NUM  (mois courant)
    lk_p1 <- base[GRADE %chin% GRADES_VALIDES, {
      tab <- table(GRADE)
      .(GRADE_P1 = names(tab)[which.max(tab)])
    }, by = .(emploi, ANNEE, MOIS_NUM)]

    # Lookup P2 : emploi x ANNEE  (annee courante - proxy mois precedents)
    lk_p2 <- base[GRADE %chin% GRADES_VALIDES, {
      tab <- table(GRADE)
      .(GRADE_P2 = names(tab)[which.max(tab)])
    }, by = .(emploi, ANNEE)]

    # Lookup P3 : emploi  (global - proxy annees precedentes)
    lk_p3 <- base[GRADE %chin% GRADES_VALIDES, {
      tab <- table(GRADE)
      .(GRADE_P3 = names(tab)[which.max(tab)])
    }, by = .(emploi)]

    # Jointure des 3 lookups sur la base (right join = toutes les lignes conservees)
    base <- lk_p1[base, on = .(emploi, ANNEE, MOIS_NUM)]
    base <- lk_p2[base, on = .(emploi, ANNEE)]
    base <- lk_p3[base, on = .(emploi)]

    # Application en cascade : P1 > P2 > P3, uniquement sur les grades invalides
    base[GRADE %chin% GRADES_INVALIDES,
         GRADE := fifelse(!is.na(GRADE_P1), GRADE_P1,
                  fifelse(!is.na(GRADE_P2), GRADE_P2,
                  fifelse(!is.na(GRADE_P3), GRADE_P3, GRADE)))]

    # Nettoyage des colonnes temporaires
    base[, c("GRADE_P1", "GRADE_P2", "GRADE_P3") := NULL]
  }

  n_restants <- base[GRADE %chin% GRADES_INVALIDES, .N]
  cat(sprintf("   Corriges : %s | Restants : %s\n\n",
             format(n_invalides - n_restants, big.mark = ","),
             format(n_restants, big.mark = ",")))
}

# ============================================================
# CALCUL SALAIRE
# ============================================================

cat("4. Calcul salaire brut...\n")

composantes <- intersect(c("124", "228", "271", "421","425"), names(base))
for (comp in composantes) base[, (comp) := as.numeric(get(comp))]
base[, SALAIRE_BRUT := rowSums(.SD, na.rm = TRUE), .SDcols = composantes]

cat(sprintf("   Salaires > 0 : %s (%.1f%%)\n\n",
           format(base[SALAIRE_BRUT > 0, .N], big.mark = ","),
           100 * base[SALAIRE_BRUT > 0, .N] / nrow(base)))

# ============================================================
# SEXE
# ============================================================

cat("5. Standardisation sexe...\n")

if ("sexe" %in% names(base)) {
  base[, SEXE_STD := {
    s <- toupper(trimws(as.character(sexe)))
    s <- gsub("É", "E", s)
    ifelse(s %in% c("MASCULIN","M","H","HOMME","MALE","1"), "Homme",
    ifelse(s %in% c("FEMININ","F","FEMME","FEMALE","2"), "Femme", "Inconnu"))
  }]
  
  n_inconnu <- base[SEXE_STD == "Inconnu", .N]
  if (n_inconnu > 0) {
    cat(sprintf("   Exclusion : %s\n", format(n_inconnu, big.mark = ",")))
    base <- base[SEXE_STD != "Inconnu"]
  }
  
  cat(sprintf("   Homme : %s | Femme : %s\n\n",
             format(base[SEXE_STD == "Homme", .N], big.mark = ","),
             format(base[SEXE_STD == "Femme", .N], big.mark = ",")))
}

# ============================================================
# MULTI-POSTES PAR MATRICULE
# ============================================================

cat("6. Détection multi-postes (par MATRICULE)...\n")

base[, NB_POSTES := .N, by = .(periode, matricule)]

total_multi <- uniqueN(base[NB_POSTES > 1, .(periode, matricule)])
total_agents <- uniqueN(base[, .(periode, matricule)])

cat(sprintf("   Multi-postes : %s / %s (%.1f%%)\n\n",
           format(total_multi, big.mark = ","),
           format(total_agents, big.mark = ","),
           100 * total_multi / total_agents))

# ============================================================
# ✅ FILTRAGE AVANT AGRÉGATION (GRADES VALIDES)
# ============================================================

cat("7. Filtrage grades valides AVANT agrégation...\n")

if (INCLURE_ZEROS) {
  base_grades_valides <- base[GRADE %chin% GRADES_VALIDES]
} else {
  base_grades_valides <- base[GRADE %chin% GRADES_VALIDES & SALAIRE_BRUT > 0]
}

cat(sprintf("   Lignes conservées : %s (%.1f%%)\n\n",
           format(nrow(base_grades_valides), big.mark = ","),
           100 * nrow(base_grades_valides) / nrow(base)))

# ============================================================
# AGRÉGATION PAR CLE_UNIQUE (pour REVENU TOTAL)
# ============================================================

agg_col  <- if (MODE_SALAIRE_BRUT) "cle_unique" else "matricule"
agg_label <- if (MODE_SALAIRE_BRUT) "cle_unique (salaire brut)" else "matricule (revenu salarial)"
cat(sprintf("8. Agrégation par %s (pour REVENU TOTAL)...\n", agg_label))

base_grades_valides[, GRADE_ORDER := grade_order_value(GRADE)]

base_agrege_revenu <- base_grades_valides[, {
  idx <- which.max(GRADE_ORDER)

  .(
    REVENU_TOTAL    = sum(SALAIRE_BRUT, na.rm = TRUE),
    NB_POSTES       = NB_POSTES[1],
    GRADE_PRINCIPAL = GRADE[idx],
    SEXE_STD        = if ("SEXE_STD"    %in% names(.SD)) SEXE_STD[1]    else "Non renseigné",
    CODE_CITP       = if ("Code_CITP"   %in% names(.SD)) Code_CITP[idx] else "Non renseigné",
    METIER_CITP     = if ("Metier_CITP" %in% names(.SD)) Metier_CITP[idx] else "Non renseigné"
  )
}, by = c("ANNEE", "MOIS_NUM", "MOIS", agg_col)]

base_grades_valides[, GRADE_ORDER := NULL]

if (INCLURE_ZEROS) {
  base_agrege_revenu <- base_agrege_revenu[is.finite(REVENU_TOTAL)]
} else {
  base_agrege_revenu <- base_agrege_revenu[REVENU_TOTAL > 0 & is.finite(REVENU_TOTAL)]
}

cat(sprintf("   %s agents agrégés (REVENU)\n\n", format(nrow(base_agrege_revenu), big.mark = ",")))

# ============================================================
# DONNÉES PAR LIGNE (pour SALAIRE BRUT)
# ============================================================

cat("9. Préparation données par ligne (pour SALAIRE BRUT)...\n")

base_lignes <- base_grades_valides

cat(sprintf("   %s lignes avec salaire brut > 0\n\n", format(nrow(base_lignes), big.mark = ",")))

# ============================================================
# INDICATEURS SUR REVENU TOTAL (agrégé par cle_unique)
# ============================================================

cat("10. Calcul indicateurs REVENU TOTAL...\n")

compute_indicators_revenu <- function(dt, by_cols) {
  dt[, .(
    Effectif = .N,
    Revenu_moyen = safe_mean(REVENU_TOTAL),
    Revenu_median = safe_median(REVENU_TOTAL),
    Revenu_min = safe_min(REVENU_TOTAL),
    Revenu_max = safe_max(REVENU_TOTAL),
    Revenu_p25 = safe_q(REVENU_TOTAL, 0.25),
    Revenu_p75 = safe_q(REVENU_TOTAL, 0.75)
  ), by = by_cols]
}

indic_grade_revenu <- compute_indicators_revenu(
  base_agrege_revenu,
  c("ANNEE", "MOIS_NUM", "MOIS", "GRADE_PRINCIPAL")
)

indic_grade_sexe_revenu <- compute_indicators_revenu(
  base_agrege_revenu,
  c("ANNEE", "MOIS_NUM", "MOIS", "SEXE_STD", "GRADE_PRINCIPAL")
)

indic_citp_revenu <- compute_indicators_revenu(
  base_agrege_revenu[CODE_CITP != "Non renseigné" & !is.na(CODE_CITP)],
  c("ANNEE", "MOIS_NUM", "MOIS", "CODE_CITP", "METIER_CITP")
)

cat(sprintf("   %d lignes d'indicateurs REVENU créées\n\n",
           nrow(indic_grade_revenu) + nrow(indic_grade_sexe_revenu) + nrow(indic_citp_revenu)))

# ============================================================
# INDICATEURS SUR SALAIRE BRUT (par ligne)
# ============================================================

cat("11. Calcul indicateurs SALAIRE BRUT...\n")

# Salaire brut moyen par année-mois-grade
salaire_brut_moyen <- base_lignes[, .(
  Salaire_brut_moyen = safe_mean(SALAIRE_BRUT)
), by = .(ANNEE, MOIS_NUM, MOIS, GRADE)]

# Salaire brut médian par année-mois-grade
salaire_brut_median <- base_lignes[, .(
  Salaire_brut_median = safe_median(SALAIRE_BRUT)
), by = .(ANNEE, MOIS_NUM, MOIS, GRADE)]

cat(sprintf("   %d lignes salaire brut moyen\n", nrow(salaire_brut_moyen)))
cat(sprintf("   %d lignes salaire brut médian\n\n", nrow(salaire_brut_median)))

# Pivot : colonnes = grades
salaire_brut_moyen_wide <- dcast(
  salaire_brut_moyen,
  ANNEE + MOIS_NUM + MOIS ~ GRADE,
  value.var = "Salaire_brut_moyen"
)

salaire_brut_median_wide <- dcast(
  salaire_brut_median,
  ANNEE + MOIS_NUM + MOIS ~ GRADE,
  value.var = "Salaire_brut_median"
)

setorder(salaire_brut_moyen_wide, ANNEE, MOIS_NUM)
setorder(salaire_brut_median_wide, ANNEE, MOIS_NUM)

# ============================================================
# MULTI-POSTES : NOUVELLE DISPOSITION
# ============================================================

cat("12. Création feuille multi-postes (disposition modifiée)...\n")

# Compter par année-mois-grade-nb_postes
multi_postes_detail <- base_agrege_revenu[, .(
  Effectif = .N,
  Revenu_moyen = safe_mean(REVENU_TOTAL)
), by = .(ANNEE, MOIS_NUM, MOIS, GRADE_PRINCIPAL, NB_POSTES)]

setorder(multi_postes_detail, ANNEE, MOIS_NUM, GRADE_PRINCIPAL, NB_POSTES)

# Pivot : colonnes = nombre de postes (1, 2, 3, 4, 5+)
multi_postes_detail[, NB_POSTES_LABEL := fifelse(
  NB_POSTES >= 5, "5+", as.character(NB_POSTES)
)]

multi_postes_effectif <- dcast(
  multi_postes_detail,
  ANNEE + MOIS_NUM + MOIS + GRADE_PRINCIPAL ~ NB_POSTES_LABEL,
  value.var = "Effectif",
  fill = 0,
  fun.aggregate = sum
)

setorder(multi_postes_effectif, ANNEE, MOIS_NUM, GRADE_PRINCIPAL)

# Réorganiser colonnes : ANNEE, MOIS, GRADE, puis 1, 2, 3, 4, 5+
cols_postes <- c("1", "2", "3", "4", "5+")
cols_existantes <- intersect(cols_postes, names(multi_postes_effectif))
setcolorder(multi_postes_effectif, c("ANNEE", "MOIS_NUM", "MOIS", "GRADE_PRINCIPAL", cols_existantes))

cat(sprintf("   %d lignes multi-postes\n\n", nrow(multi_postes_effectif)))

# ============================================================
# DISTRIBUTION MULTI-POSTES (ancien format conservé)
# ============================================================

distrib_postes <- base_agrege_revenu[, .(
  Effectif = .N,
  Pct = 100 * .N / nrow(base_agrege_revenu),
  Revenu_moyen = safe_mean(REVENU_TOTAL)
), by = NB_POSTES]

setorder(distrib_postes, NB_POSTES)

# ============================================================
# EXPORT EXCEL
# ============================================================

cat("13. Export Excel...\n")

wb <- createWorkbook()
hs <- createStyle(fontSize = 11, fontColour = "#FFFFFF", halign = "center",
                  fgFill = "#4F81BD", textDecoration = "bold")

add_sheet <- function(name, data) {
  if (nrow(data) > 0) {
    addWorksheet(wb, name)
    writeData(wb, name, data)
    addStyle(wb, name, hs, rows = 1, cols = 1:ncol(data), gridExpand = TRUE)
    freezePane(wb, name, firstRow = TRUE)
    cat(sprintf("   ✓ %s (%d lignes)\n", name, nrow(data)))
  }
}

# FEUILLES REVENU TOTAL (basé sur cle_unique agrégé)
add_sheet("REVENU_Grade_Detail", indic_grade_revenu)
add_sheet("REVENU_Grade_Sexe", indic_grade_sexe_revenu)
add_sheet("REVENU_CITP_Detail", indic_citp_revenu)

# FEUILLES SALAIRE BRUT (basé sur lignes individuelles)
add_sheet("SALAIRE_BRUT_Moyen", salaire_brut_moyen_wide)
add_sheet("SALAIRE_BRUT_Median", salaire_brut_median_wide)

# FEUILLES MULTI-POSTES
add_sheet("MULTI_Par_Grade_NbPostes", multi_postes_effectif)
add_sheet("MULTI_Distribution", distrib_postes)

saveWorkbook(wb, OUTPUT_FILE, overwrite = TRUE)

cat(sprintf("\n✅ Fichier créé : %s\n", OUTPUT_FILE))
cat(sprintf("   Taille : %.1f KB\n", file.info(OUTPUT_FILE)$size / 1024))
cat(sprintf("   Feuilles : %d\n\n", length(names(wb))))

cat(paste(rep("=", 70), collapse = ""), "\n")
cat("✅ TERMINÉ\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

cat("📊 FEUILLES CRÉÉES :\n\n")
cat("REVENUS (agrégé par cle_unique, APRÈS filtrage grades valides) :\n")
cat("  • REVENU_Grade_Detail : Indicateurs revenu total par grade\n")
cat("  • REVENU_Grade_Sexe : Indicateurs revenu total par grade et sexe\n")
cat("  • REVENU_CITP_Detail : Indicateurs revenu total par CITP\n\n")
cat("SALAIRES BRUTS (par ligne, grades valides seulement) :\n")
cat("  • SALAIRE_BRUT_Moyen : Moyennes par année-mois-grade (pivot)\n")
cat("  • SALAIRE_BRUT_Median : Médianes par année-mois-grade (pivot)\n\n")
cat("MULTI-POSTES (NB_POSTES par MATRICULE) :\n")
cat("  • MULTI_Par_Grade_NbPostes : Effectifs par année-mois-grade-nb_postes\n")
cat("  • MULTI_Distribution : Distribution globale des multi-postes\n\n")