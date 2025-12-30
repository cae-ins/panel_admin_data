
library(data.table)
library(arrow)
library(openxlsx)
library(dplyr)
library(rlang)

MOIS_FR <- c(
  "1" = "Janvier", "2" = "Février", "3" = "Mars", "4" = "Avril",
  "5" = "Mai", "6" = "Juin", "7" = "Juillet", "8" = "Août",
  "9" = "Septembre", "10" = "Octobre", "11" = "Novembre", "12" = "Décembre"
)

# -----------------------------
# Helpers stats robustes
# -----------------------------
safe_mean   <- function(x) { x <- x[is.finite(x)]; if (length(x) == 0) NA_real_ else mean(x) }
safe_median <- function(x) { x <- x[is.finite(x)]; if (length(x) == 0) NA_real_ else median(x) }
safe_min    <- function(x) { x <- x[is.finite(x)]; if (length(x) == 0) NA_real_ else min(x) }
safe_max    <- function(x) { x <- x[is.finite(x)]; if (length(x) == 0) NA_real_ else max(x) }

safe_sd <- function(x) {
  x <- x[is.finite(x)]
  n <- length(x)
  if (n <= 1) NA_real_ else sd(x)
}
safe_se <- function(x) {
  x <- x[is.finite(x)]
  n <- length(x)
  if (n <= 1) NA_real_ else sd(x) / sqrt(n)
}
safe_cv <- function(x) {
  x <- x[is.finite(x)]
  n <- length(x)
  if (n <= 1) return(NA_real_)
  m <- mean(x)
  if (!is.finite(m) || m == 0) NA_real_ else sd(x) / m
}

# ============================================================
# GRADE_DEFINITIF (plus récent) via GRADE texte
# ============================================================
extract_grade_code <- function(x) {
  s <- toupper(trimws(as.character(x)))
  s <- gsub("É", "E", s)
  
  out <- rep(NA_character_, length(s))
  is_fonc <- grepl("^FONCTIONNAIRE", s)
  
  # Ex: "FONCTIONNAIRE CATEGORIE C TITULAIRE" => C
  #     "FONCTIONNAIRE CATEGORIE A7" => A7
  out[is_fonc] <- sub(".*CATEGORIE\\s+([ABCD])\\s*([0-9]?)\\b.*", "\\1\\2", s[is_fonc])
  
  ok <- !is.na(out) & grepl("^[ABCD][0-9]?$", out)
  out[!ok] <- NA_character_
  out
}

build_gradedf_recent <- function(
    dt,
    src_col,                      # "GRADE"
    id_col = "MATRICULE_UNIQUE",
    year_col = "ANNEE",
    month_col = "MOIS_NUM",
    carry_dt = NULL               # data.table(MATRICULE_UNIQUE, grade_last)
) {
  stopifnot(is.data.table(dt))
  
  dt[, GRADE_SRC__ := extract_grade_code(get(src_col))]
  
  if (is.null(carry_dt)) carry_dt <- data.table(MATRICULE_UNIQUE = character(), grade_last = character())
  setkeyv(carry_dt, id_col)
  
  dt[, grade_start__ := NA_character_]
  if (nrow(carry_dt) > 0) dt[carry_dt, grade_start__ := i.grade_last, on = setNames(id_col, id_col)]
  
  setorderv(dt, c(id_col, year_col, month_col))
  
  # Forward-fill manuel (chaînes)
  dt[, GRADE_DEFINITIF := {
    x <- GRADE_SRC__
    if (length(x) >= 1L && is.na(x[1L]) && !is.na(grade_start__[1L])) x[1L] <- grade_start__[1L]
    for (i in seq_along(x)) if (i > 1 && is.na(x[i])) x[i] <- x[i - 1]
    x
  }, by = id_col]
  
  dt[is.na(GRADE_DEFINITIF) | trimws(GRADE_DEFINITIF) == "", GRADE_DEFINITIF := "NF"]
  
  carry_new <- dt[, .(grade_last = GRADE_DEFINITIF[.N]), by = id_col]
  setkeyv(carry_new, id_col)
  
  dt[, c("GRADE_SRC__", "grade_start__") := NULL]
  list(dt = dt, carry = carry_new)
}

# -----------------------------
# Winsorisation par groupe (in-place)
# -----------------------------
winsorize_dt_by <- function(dt, value_col, by, p = 0.01, min_n = 30, fallback_by = NULL) {
  stopifnot(is.data.table(dt))
  wcol <- paste0(value_col, "_W")
  
  dt[, (wcol) := as.numeric(get(value_col))]
  dt[!is.finite(get(wcol)), (wcol) := NA_real_]
  
  dt[, .grp_n__ := .N, by = by]
  
  bounds <- dt[!is.na(get(value_col)) & is.finite(get(value_col)) & .grp_n__ >= min_n,
               .(lo = as.numeric(quantile(get(value_col), probs = p, na.rm = TRUE, type = 7)),
                 hi = as.numeric(quantile(get(value_col), probs = 1 - p, na.rm = TRUE, type = 7))),
               by = by]
  
  if (nrow(bounds) > 0) {
    setkeyv(bounds, by)
    dt[bounds, `:=`(.lo__ = i.lo, .hi__ = i.hi), on = by]
    
    dt[!is.na(.lo__) & !is.na(get(wcol)),
       (wcol) := fifelse(get(wcol) < .lo__, .lo__,
                         fifelse(get(wcol) > .hi__, .hi__, get(wcol)))]
  } else {
    dt[, `:=`(.lo__ = NA_real_, .hi__ = NA_real_)]
  }
  
  if (!is.null(fallback_by)) {
    fb <- dt[!is.na(get(value_col)) & is.finite(get(value_col)),
             .(lo_fb = as.numeric(quantile(get(value_col), probs = p, na.rm = TRUE, type = 7)),
               hi_fb = as.numeric(quantile(get(value_col), probs = 1 - p, na.rm = TRUE, type = 7))),
             by = fallback_by]
    
    if (nrow(fb) > 0) {
      setkeyv(fb, fallback_by)
      dt[fb, `:=`(.lo_fb__ = i.lo_fb, .hi_fb__ = i.hi_fb), on = fallback_by]
      
      dt[.grp_n__ < min_n & !is.na(get(wcol)),
         (wcol) := fifelse(get(wcol) < .lo_fb__, .lo_fb__,
                           fifelse(get(wcol) > .hi_fb__, .hi_fb__, get(wcol)))]
    } else {
      dt[, `:=`(.lo_fb__ = NA_real_, .hi_fb__ = NA_real_)]
    }
  }
  
  dt[, c(".lo__", ".hi__", ".lo_fb__", ".hi_fb__", ".grp_n__") := NULL]
  invisible(dt)
}

# -----------------------------
# Agrégations
# -----------------------------
agg_grade <- function(dt) {
  dt[, .(
    Effectif = .N,
    `Brut moyen (W)`          = safe_mean(BRUT_B0_W),
    `Brut médian (W)`         = safe_median(BRUT_B0_W),
    `Brut erreur std (W)`     = safe_se(BRUT_B0_W),
    `Brut ecart-type (W)`     = safe_sd(BRUT_B0_W),
    `Brut coef variation (W)` = safe_cv(BRUT_B0_W),
    `Brut min`                = safe_min(BRUT_B0),
    `Brut max`                = safe_max(BRUT_B0)
  ), by = .(ANNEE, MOIS_NUM, MOIS, GRADE_DEFINITIF)]
}

agg_grade_sexe <- function(dt) {
  dt[, .(
    Effectif = .N,
    `Brut moyen (W)`          = safe_mean(BRUT_B0_W),
    `Brut médian (W)`         = safe_median(BRUT_B0_W),
    `Brut erreur std (W)`     = safe_se(BRUT_B0_W),
    `Brut ecart-type (W)`     = safe_sd(BRUT_B0_W),
    `Brut coef variation (W)` = safe_cv(BRUT_B0_W),
    `Brut min`                = safe_min(BRUT_B0),
    `Brut max`                = safe_max(BRUT_B0)
  ), by = .(ANNEE, MOIS_NUM, MOIS, SEXE_STD, GRADE_DEFINITIF)]
}

# -----------------------------
# Fonction principale (2024-2025 uniquement) + Multi-postes par grade
# -----------------------------
build_indicateurs_grade_brut_b0_2024_2025 <- function(
    panel_path = "data/panel_solde_final_2015_2025.parquet",
    out_xlsx_path = "indicateurs_BRUT_B0_par_grade_2024_2025.xlsx",
    years_keep = c(2024L, 2025L),
    winsor_p = 0.01,
    winsor_min_n = 30,
    winsor_by = c("ANNEE","MOIS_NUM","SEXE_STD","GRADE_DEFINITIF","STATUT"),
    winsor_fallback_by = c("ANNEE","SEXE_STD","GRADE_DEFINITIF","STATUT"),
    # multi-postes
    poste_identifier = c("EMPLOYEUR","STRUCTURE","MINISTERE","SIRET","CODE_EMPLOYEUR"),
    top_postes_bucket = 4,
    debug_multipostes = TRUE
) {
  message("📊 Ouverture dataset Arrow...")
  ds <- open_dataset(panel_path)
  col_names <- names(ds)
  
  ANNEE_COL <- if ("ANNEE_COLLECTE" %in% col_names) "ANNEE_COLLECTE" else "YEAR"
  MOIS_COL  <- if ("MOIS_COLLECTE"  %in% col_names) "MOIS_COLLECTE"  else "MONTH"
  
  BRUT_COL  <- if ("MONTANT BRUT_B0" %in% col_names) "MONTANT BRUT_B0" else stop("Colonne 'MONTANT BRUT_B0' introuvable.")
  SEXE_COL  <- if ("SEXE_IMPUTE" %in% col_names) "SEXE_IMPUTE" else stop("Colonne 'SEXE_IMPUTE' introuvable.")
  GRADE_COL <- if ("GRADE" %in% col_names) "GRADE" else stop("Colonne 'GRADE' introuvable.")
  MAT_COL   <- if ("MATRICULE_UNIQUE" %in% col_names) "MATRICULE_UNIQUE" else stop("Colonne 'MATRICULE_UNIQUE' introuvable.")
  
  # Colonnes "poste" si disponibles (sinon fallback: comptage des lignes)
  poste_cols <- intersect(poste_identifier, col_names)
  if (length(poste_cols) == 0) {
    warning("⚠️ Aucune colonne employeur/structure détectée. Multi-postes = comptage des lignes.")
    poste_cols <- NULL
  } else {
    message(sprintf("✓ Colonnes poste détectées: %s", paste(poste_cols, collapse = ", ")))
  }
  
  message("🔍 Détection des années...")
  annees_all <- ds |>
    dplyr::select(dplyr::all_of(ANNEE_COL)) |>
    dplyr::distinct() |>
    dplyr::collect() |>
    dplyr::pull() |>
    as.integer() |>
    sort()
  
  annees <- intersect(annees_all, years_keep)
  if (length(annees) == 0) stop("Aucune observation pour les années demandées (2024/2025).")
  message(sprintf("   Années retenues: %s", paste(annees, collapse = ", ")))
  
  # Carry inter-années (utile si 2024 commence avec NA)
  grade_carry <- data.table(MATRICULE_UNIQUE = character(), grade_last = character())
  
  grade_res_by_year <- list()
  grade_sexe_res_by_year <- list()
  multipostes_grade_by_year <- list()
  
  for (annee in annees) {
    message(sprintf("\n📅 Année %s...", annee))
    
    dt <- ds |>
      dplyr::filter(!!rlang::sym(ANNEE_COL) == annee) |>
      dplyr::collect() |>
      as.data.table()
    
    # ----- Conversions de base -----
    dt[, ANNEE    := suppressWarnings(as.integer(as.character(get(ANNEE_COL))))]
    dt[, MOIS_NUM := suppressWarnings(as.integer(as.character(get(MOIS_COL))))]
    dt[, MOIS     := MOIS_FR[as.character(MOIS_NUM)]]
    
    dt[, BRUT_B0  := suppressWarnings(as.numeric(as.character(get(BRUT_COL))))]
    dt[!is.finite(BRUT_B0), BRUT_B0 := NA_real_]
    
    dt[, MAT_B0 := trimws(as.character(get(MAT_COL)))]
    dt[, MATRICULE_UNIQUE := substr(MAT_B0, 1, 7)]
    
    # ----- SEXE standardisé -----
    dt[, SEXE_STD := {
      s <- toupper(trimws(as.character(get(SEXE_COL))))
      s <- gsub("É", "E", s)
      fcase(
        s %in% c("MASCULIN","M","H","HOMME","MALE","MASC","1"), "Homme",
        s %in% c("FEMININ","F","FEMME","FEMALE","FEM","2"),     "Femme",
        default = "Inconnu"
      )
    }]
    
    # ----- STATUT (depuis le matricule) -----
    dt[, STATUT := fcase(
      grepl("^5", MATRICULE_UNIQUE), "Contractuel",
      grepl("^6", MATRICULE_UNIQUE), "Gens de maison",
      default = "Fonctionnaire"
    )]
    
    # ----- GRADE_DEFINITIF (plus récent) -----
    tmpg <- build_gradedf_recent(
      dt = dt,
      src_col = GRADE_COL,
      id_col = "MATRICULE_UNIQUE",
      year_col = "ANNEE",
      month_col = "MOIS_NUM",
      carry_dt = grade_carry
    )
    dt <- tmpg$dt
    grade_carry <- tmpg$carry
    
    # Non-fonctionnaires => NF
    dt[STATUT %in% c("Contractuel","Gens de maison"), GRADE_DEFINITIF := "NF"]
    
    # ----- Filtrage sexe -----
    nb_inconnu <- nrow(dt[SEXE_STD == "Inconnu"])
    dt <- dt[SEXE_STD %in% c("Homme","Femme")]
    message(sprintf("   ⚠️ Exclusion SEXE_STD='Inconnu': %s lignes supprimées (%s restantes)",
                    format(nb_inconnu, big.mark = ","),
                    format(nrow(dt), big.mark = ",")))
    
    # ----- Winsorisation (uniquement Contractuel + Gens de maison) -----
    message("   🔧 Winsorisation 1% sur BRUT_B0 (Contractuel + Gens de maison)...")
    dt[, row_id__ := .I]
    dt[, BRUT_B0_W := BRUT_B0]
    
    mask_win <- (dt$STATUT %in% c("Contractuel", "Gens de maison")) &
      !is.na(dt$BRUT_B0) & is.finite(dt$BRUT_B0) &
      !is.na(dt$GRADE_DEFINITIF) & dt$GRADE_DEFINITIF != ""
    
    if (any(mask_win)) {
      idx_win <- which(mask_win)
      sub <- dt[idx_win, .(row_id__, BRUT_B0, ANNEE, MOIS_NUM, SEXE_STD, GRADE_DEFINITIF, STATUT)]
      
      winsorize_dt_by(
        sub,
        value_col = "BRUT_B0",
        by = winsor_by,
        p = winsor_p,
        min_n = winsor_min_n,
        fallback_by = winsor_fallback_by
      )
      
      dt[sub, BRUT_B0_W := i.BRUT_B0_W, on = .(row_id__)]
      rm(sub, idx_win)
      gc()
    }
    dt[, row_id__ := NULL]
    
    # ============================================================
    # MULTI-POSTES PAR GRADE (1,2,3,4=4+)
    # ============================================================
    message("   👥 Multi-postes par GRADE...")
    dt_mat_ok <- !is.na(dt$MAT_B0) & dt$MAT_B0 != ""
    dt_g_ok   <- !is.na(dt$GRADE_DEFINITIF) & dt$GRADE_DEFINITIF != "" & dt$GRADE_DEFINITIF != "NF"
    
    if (!is.null(poste_cols)) {
      for (pc in poste_cols) dt[, (pc) := fifelse(is.na(get(pc)), "", trimws(as.character(get(pc)))) ]
      dt[, poste_key := do.call(paste, c(.SD, sep = "_")), .SDcols = poste_cols]
      
      dt_posts <- dt[dt_mat_ok & dt_g_ok,
                     .(nb_postes = uniqueN(poste_key)),
                     by = .(ANNEE, MOIS_NUM, MOIS, GRADE_DEFINITIF, MAT = MAT_B0)]
    } else {
      dt_posts <- dt[dt_mat_ok & dt_g_ok,
                     .(nb_postes = .N),
                     by = .(ANNEE, MOIS_NUM, MOIS, GRADE_DEFINITIF, MAT = MAT_B0)]
    }
    
    if (debug_multipostes && nrow(dt_posts) > 0) {
      distrib <- dt_posts[, .(count = .N), by = nb_postes][order(nb_postes)]
      message(sprintf("      MAT uniques: %s", format(uniqueN(dt_posts$MAT), big.mark = ",")))
      for (i in 1:min(10, nrow(distrib))) {
        message(sprintf("        %d poste(s): %s (%.2f%%)",
                        distrib$nb_postes[i],
                        format(distrib$count[i], big.mark = ","),
                        100 * distrib$count[i] / sum(distrib$count)))
      }
    }
    
    # Bucket: 4 = 4+ (pour coller à ton exemple 1..4)
    dt_posts[, nb_postes_bucket := fifelse(
      nb_postes >= top_postes_bucket,
      as.character(top_postes_bucket),
      as.character(nb_postes)
    )]
    
    mp <- dt_posts[, .(effectif = .N), by = .(ANNEE, MOIS_NUM, MOIS, GRADE_DEFINITIF, nb_postes_bucket)]
    
    mp_wide <- dcast(
      mp,
      ANNEE + MOIS_NUM + MOIS + GRADE_DEFINITIF ~ nb_postes_bucket,
      value.var = "effectif",
      fill = 0
    )
    
    # Forcer colonnes 1..4
    for (k in as.character(1:top_postes_bucket)) {
      if (!(k %in% names(mp_wide))) mp_wide[, (k) := 0L]
    }
    setcolorder(mp_wide, c("ANNEE","MOIS_NUM","MOIS","GRADE_DEFINITIF", as.character(1:top_postes_bucket)))
    
    setorder(mp_wide, ANNEE, MOIS_NUM, GRADE_DEFINITIF)
    mp_wide[, MOIS_NUM := NULL]
    
    multipostes_grade_by_year[[as.character(annee)]] <- mp_wide
    
    # ----- Indicateurs par grade -----
    message("   📊 Indicateurs par GRADE...")
    grade_res_by_year[[as.character(annee)]] <- agg_grade(dt)
    
    # ----- Indicateurs par grade x sexe -----
    message("   📊 Indicateurs par GRADE x SEXE...")
    grade_sexe_res_by_year[[as.character(annee)]] <- agg_grade_sexe(dt)
    
    # nettoyage
    if (!is.null(poste_cols) && "poste_key" %in% names(dt)) dt[, poste_key := NULL]
    rm(dt, dt_posts)
    gc()
  }
  
  # ----- Consolidation -----
  grade_long      <- rbindlist(grade_res_by_year, fill = TRUE)
  grade_sexe_long <- rbindlist(grade_sexe_res_by_year, fill = TRUE)
  multipostes_grade_final <- rbindlist(multipostes_grade_by_year, fill = TRUE)
  
  setorder(grade_long, ANNEE, MOIS_NUM, GRADE_DEFINITIF)
  setorder(grade_sexe_long, ANNEE, MOIS_NUM, SEXE_STD, GRADE_DEFINITIF)
  
  grade_sexe_long[, GR_SEXE := paste0(GRADE_DEFINITIF, "_", SEXE_STD)]
  
  # ----- Filtrage des lignes vides (grades) -----
  message("🧹 Filtrage des lignes avec grades vides ou 'NF'...")
  
  nb_avant_grade <- nrow(grade_long)
  grade_long <- grade_long[!is.na(GRADE_DEFINITIF) & 
                             trimws(GRADE_DEFINITIF) != "" & 
                             GRADE_DEFINITIF != "NF"]
  nb_apres_grade <- nrow(grade_long)
  message(sprintf("   Grade: %s lignes supprimées (%s → %s)",
                  format(nb_avant_grade - nb_apres_grade, big.mark = ","),
                  format(nb_avant_grade, big.mark = ","),
                  format(nb_apres_grade, big.mark = ",")))
  
  nb_avant_gs <- nrow(grade_sexe_long)
  grade_sexe_long <- grade_sexe_long[!is.na(GRADE_DEFINITIF) & 
                                       trimws(GRADE_DEFINITIF) != "" & 
                                       GRADE_DEFINITIF != "NF"]
  nb_apres_gs <- nrow(grade_sexe_long)
  message(sprintf("   Grade x Sexe: %s lignes supprimées (%s → %s)",
                  format(nb_avant_gs - nb_apres_gs, big.mark = ","),
                  format(nb_avant_gs, big.mark = ","),
                  format(nb_apres_gs, big.mark = ",")))
  
  nb_avant_mp <- nrow(multipostes_grade_final)
  multipostes_grade_final <- multipostes_grade_final[!is.na(GRADE_DEFINITIF) & 
                                                       trimws(GRADE_DEFINITIF) != "" & 
                                                       GRADE_DEFINITIF != "NF"]
  nb_apres_mp <- nrow(multipostes_grade_final)
  message(sprintf("   Multi-postes: %s lignes supprimées (%s → %s)",
                  format(nb_avant_mp - nb_apres_mp, big.mark = ","),
                  format(nb_avant_mp, big.mark = ","),
                  format(nb_apres_mp, big.mark = ",")))
  
  # ----- Feuilles Excel -----
  message("📄 Construction des feuilles Excel...")
  
  sheet_map_grade <- list(
    "G_Eff" = "Effectif",
    "G_Moy" = "Brut moyen (W)",
    "G_Med" = "Brut médian (W)",
    "G_SE"  = "Brut erreur std (W)",
    "G_SD"  = "Brut ecart-type (W)",
    "G_CV"  = "Brut coef variation (W)"
  )
  sheet_map_grade_sexe <- list(
    "GS_Eff" = "Effectif",
    "GS_Moy" = "Brut moyen (W)",
    "GS_Med" = "Brut médian (W)",
    "GS_SE"  = "Brut erreur std (W)",
    "GS_SD"  = "Brut ecart-type (W)",
    "GS_CV"  = "Brut coef variation (W)"
  )
  
  sheets <- list()
  
  # Par grade
  for (nm in names(sheet_map_grade)) {
    m <- sheet_map_grade[[nm]]
    sh <- dcast(
      grade_long,
      ANNEE + MOIS_NUM + MOIS ~ GRADE_DEFINITIF,
      value.var = m,
      fill = NA_real_,
      drop = FALSE
    )
    setorder(sh, ANNEE, MOIS_NUM)
    sh[, MOIS_NUM := NULL]
    
    # Filtrer les lignes complètement vides (sauf colonnes ANNEE et MOIS)
    cols_values <- setdiff(names(sh), c("ANNEE", "MOIS"))
    if (length(cols_values) > 0) {
      sh <- sh[rowSums(!is.na(sh[, ..cols_values])) > 0]
    }
    
    sheets[[nm]] <- sh
  }
  
  # Par grade x sexe
  for (nm in names(sheet_map_grade_sexe)) {
    m <- sheet_map_grade_sexe[[nm]]
    sh <- dcast(
      grade_sexe_long,
      ANNEE + MOIS_NUM + MOIS ~ GR_SEXE,
      value.var = m,
      fill = NA_real_,
      drop = FALSE
    )
    setorder(sh, ANNEE, MOIS_NUM)
    sh[, MOIS_NUM := NULL]
    
    # Filtrer les lignes complètement vides (sauf colonnes ANNEE et MOIS)
    cols_values <- setdiff(names(sh), c("ANNEE", "MOIS"))
    if (length(cols_values) > 0) {
      sh <- sh[rowSums(!is.na(sh[, ..cols_values])) > 0]
    }
    
    sheets[[nm]] <- sh
  }
  
  # Multi-postes par grade (1,2,3,4=4+) - Filtrage des lignes vides
  mp_final <- multipostes_grade_final
  cols_mp <- setdiff(names(mp_final), c("ANNEE", "MOIS", "GRADE_DEFINITIF"))
  if (length(cols_mp) > 0) {
    mp_final <- mp_final[rowSums(mp_final[, ..cols_mp], na.rm = TRUE) > 0]
  }
  sheets[["MP_GRADE"]] <- mp_final
  
  # ----- Export Excel -----
  message("💾 Export Excel...")
  wb <- createWorkbook()
  
  headerStyle <- createStyle(
    fontSize = 11, fontColour = "#FFFFFF", halign = "center",
    fgFill = "#4F81BD", border = "TopBottom", textDecoration = "bold"
  )
  
  for (nm in names(sheets)) {
    addWorksheet(wb, nm)
    writeData(wb, nm, sheets[[nm]])
    addStyle(wb, nm, headerStyle, rows = 1, cols = 1:ncol(sheets[[nm]]), gridExpand = TRUE)
    
    setColWidths(wb, nm, cols = 1, widths = 10)  # ANNEE
    setColWidths(wb, nm, cols = 2, widths = 12)  # MOIS
    if (ncol(sheets[[nm]]) > 2) setColWidths(wb, nm, cols = 3:ncol(sheets[[nm]]), widths = 16)
    
    message(sprintf("   ✓ %s (%d lignes, %d colonnes)", nm, nrow(sheets[[nm]]), ncol(sheets[[nm]])))
  }
  
  saveWorkbook(wb, out_xlsx_path, overwrite = TRUE)
  message(sprintf("\n✅ Terminé: %s", out_xlsx_path))
  
  invisible(list(
    sheets = sheets,
    grade_long = grade_long,
    grade_sexe_long = grade_sexe_long,
    multipostes_grade = multipostes_grade_final
  ))
}

# ============================================================
# UTILISATION (2024-2025)
# ============================================================
res <- build_indicateurs_grade_brut_b0_2024_2025(
  panel_path = "data/panel_solde_final_2015_2025.parquet",
  out_xlsx_path = "indicateurs_BRUT_B0_par_grade_2024_2025.xlsx",
  years_keep = c(2024L, 2025L),
  winsor_p = 0.01,
  winsor_min_n = 30,
  winsor_by = c("ANNEE","MOIS_NUM","SEXE_STD","GRADE_DEFINITIF","STATUT"),
  winsor_fallback_by = c("ANNEE","SEXE_STD","GRADE_DEFINITIF","STATUT"),
  poste_identifier = c("EMPLOYEUR","STRUCTURE","MINISTERE","SIRET","CODE_EMPLOYEUR"),
  top_postes_bucket = 4,
  debug_multipostes = TRUE
)