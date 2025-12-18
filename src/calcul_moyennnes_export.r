library(data.table)
library(arrow)
library(openxlsx)

# -----------------------------
# Dictionnaire des mois
# -----------------------------
MOIS_FR <- c(
  "1" = "Janvier", "2" = "Février", "3" = "Mars", "4" = "Avril",
  "5" = "Mai", "6" = "Juin", "7" = "Juillet", "8" = "Août",
  "9" = "Septembre", "10" = "Octobre", "11" = "Novembre", "12" = "Décembre"
)

# -----------------------------
# Winsorisation robuste par groupe
# - Si un groupe a < min_n, on winsorise au niveau fallback_by (ex: ANNEE)
# -----------------------------
winsorize_dt_by <- function(dt, value_cols, by, p = 0.01, min_n = 30, fallback_by = NULL) {
  stopifnot(is.data.table(dt))
  for (col in value_cols) {
    wcol <- paste0(col, "_W")
    
    # Base: copie
    dt[, (wcol) := as.numeric(get(col))]
    
    # Group sizes
    dt[, .grp_n__ := .N, by = by]
    
    # Group bounds (seulement sur groupes >= min_n)
    bounds <- dt[!is.na(get(col)) & .grp_n__ >= min_n,
                 .(lo = as.numeric(quantile(get(col), probs = p, na.rm = TRUE, type = 7)),
                   hi = as.numeric(quantile(get(col), probs = 1 - p, na.rm = TRUE, type = 7))),
                 by = by]
    
    # Join bounds
    setkeyv(bounds, by)
    dt[bounds, `:=`(.lo__ = i.lo, .hi__ = i.hi), on = by]
    
    # Clamp avec bornes de groupe
    dt[!is.na(.lo__) & !is.na(get(wcol)),
       (wcol) := fifelse(get(wcol) < .lo__, .lo__,
                         fifelse(get(wcol) > .hi__, .hi__, get(wcol)))]
    
    # Fallback pour petits groupes (optionnel)
    if (!is.null(fallback_by)) {
      # Bounds fallback (ex: ANNEE)
      fb <- dt[!is.na(get(col)),
               .(lo_fb = as.numeric(quantile(get(col), probs = p, na.rm = TRUE, type = 7)),
                 hi_fb = as.numeric(quantile(get(col), probs = 1 - p, na.rm = TRUE, type = 7))),
               by = fallback_by]
      setkeyv(fb, fallback_by)
      dt[fb, `:=`(.lo_fb__ = i.lo_fb, .hi_fb__ = i.hi_fb), on = fallback_by]
      
      dt[.grp_n__ < min_n & !is.na(get(wcol)),
         (wcol) := fifelse(get(wcol) < .lo_fb__, .lo_fb__,
                           fifelse(get(wcol) > .hi_fb__, .hi_fb__, get(wcol)))]
    }
    
    # Cleanup colonnes temporaires
    dt[, c(".lo__", ".hi__", ".lo_fb__", ".hi_fb__") := NULL]
    dt[, .grp_n__ := NULL]
  }
  
  dt
}

# -----------------------------
# Agrégation indicateurs (sur colonnes winsorisées)
# -----------------------------
agg_stats <- function(dt, group_label) {
  dt[, .(
    groupe = group_label,
    Effectif = .N,
    `Net moyen`   = mean(NET_W,  na.rm = TRUE),
    `Brut moyen`  = mean(BRUT_W, na.rm = TRUE),
    `Net médian`  = median(NET_W,  na.rm = TRUE),
    `Brut médian` = median(BRUT_W, na.rm = TRUE),
    `Net min` = min(NET_W, na.rm = TRUE),
    `Net max` = max(NET_W, na.rm = TRUE),
    `Brut min` = min(BRUT_W, na.rm = TRUE),
    `Brut max` = max(BRUT_W, na.rm = TRUE)
  ), by = .(ANNEE, MOIS_NUM, MOIS)]
}

# -----------------------------
# Fonction principale
# -----------------------------
build_indicateurs_salaire <- function(
    panel_path,
    out_xlsx_path = "indicateurs_salaire_2015_2025_OPT.xlsx",
    winsor_p = 0.01,
    winsor_min_n = 30,
    # Winsorisation par défaut : ANNEE x STATUT_AGG (homogène)
    winsor_by = c("ANNEE", "STATUT_AGG"),
    winsor_fallback_by = c("ANNEE"),
    top_postes_bucket = 6,
    poste_identifier = c("EMPLOYEUR", "STRUCTURE", "MINISTERE", "SIRET", "CODE_EMPLOYEUR"),
    debug_multipostes = TRUE
) {
  message("📊 Ouverture du dataset Arrow...")
  ds <- open_dataset(panel_path)
  col_names <- names(ds)
  
  ANNEE_COL <- if ("ANNEE_COLLECTE" %in% col_names) "ANNEE_COLLECTE" else "YEAR"
  MOIS_COL  <- if ("MOIS_COLLECTE"  %in% col_names) "MOIS_COLLECTE"  else "MONTH"
  BRUT_COL  <- if ("MONTANT BRUT_B1" %in% col_names) "MONTANT BRUT_B1" else "MONTANT BRUT_B0"
  NET_COL   <- "MONTANT NET"
  SEXE_COL  <- "SEXE_IMPUTE"
  CAT_COL   <- "CATEGORIE_1"
  MAT_COL   <- "MATRICULE"
  
  # Colonnes poste (si dispo)
  poste_cols <- intersect(poste_identifier, col_names)
  if (length(poste_cols) == 0) {
    warning("⚠️ Aucune colonne employeur/structure détectée. Multi-postes = comptage des lignes.")
    poste_cols <- NULL
  } else {
    message(sprintf("✓ Colonnes poste détectées: %s", paste(poste_cols, collapse = ", ")))
  }
  
  # Années
  message("🔍 Détection des années disponibles...")
  annees <- ds |>
    dplyr::select(dplyr::all_of(ANNEE_COL)) |>
    dplyr::distinct() |>
    dplyr::collect() |>
    dplyr::pull() |>
    sort()
  message(sprintf("   Années trouvées: %s", paste(annees, collapse = ", ")))
  
  # Accumulateurs légers (uniquement agrégats)
  indic_by_year <- list()
  multipostes_by_year <- list()
  
  for (annee in annees) {
    message(sprintf("\n📅 Année %s...", annee))
    
    # Charger une année
    dt <- ds |>
      dplyr::filter(!!rlang::sym(ANNEE_COL) == annee) |>
      dplyr::collect() |>
      as.data.table()
    
    # Conversions
    dt[, NET      := suppressWarnings(as.numeric(as.character(get(NET_COL))))]
    dt[, BRUT     := suppressWarnings(as.numeric(as.character(get(BRUT_COL))))]
    dt[, ANNEE    := suppressWarnings(as.numeric(as.character(get(ANNEE_COL))))]
    dt[, MOIS_NUM := suppressWarnings(as.numeric(as.character(get(MOIS_COL))))]
    dt[, MOIS     := MOIS_FR[as.character(MOIS_NUM)]]
    
    # Sexe standardisé
    dt[, SEXE_STD := fcase(
      toupper(trimws(as.character(get(SEXE_COL)))) %in% c("M", "H", "HOMME"), "Homme",
      toupper(trimws(as.character(get(SEXE_COL)))) %in% c("F", "FEMME"), "Femme",
      default = NA_character_
    )]
    
    # Statut détaillé + agrégé
    dt[, STATUT := fcase(
      grepl("^5", as.character(get(MAT_COL))), "Contractuel",
      grepl("^6", as.character(get(MAT_COL))), "Gens de maison",
      default = "Fonctionnaire"
    )]
    dt[, STATUT_AGG := fifelse(STATUT %in% c("Contractuel", "Gens de maison"),
                               "Non fonctionnaire", "Fonctionnaire")]
    
    # Catégorie (clean)
    dt[, CATEGORIE := toupper(trimws(as.character(get(CAT_COL))))]
    dt[CATEGORIE == "" | is.na(CATEGORIE), CATEGORIE := "NON CLASSE"]
    
    # -----------------------------
    # Winsorisation AVANT indicateurs
    # -----------------------------
    message("   🔧 Winsorisation (NET/BRUT)...")
    dt <- winsorize_dt_by(
      dt,
      value_cols = c("NET", "BRUT"),
      by = winsor_by,
      p = winsor_p,
      min_n = winsor_min_n,
      fallback_by = winsor_fallback_by
    )
    
    # -----------------------------
    # Multi-postes (par mois)
    # -----------------------------
    message("   👥 Multi-postes...")
    dt_mat_ok <- !is.na(get(MAT_COL)) & trimws(as.character(get(MAT_COL))) != ""
    
    if (!is.null(poste_cols)) {
      # Nettoyage colonnes poste pour éviter "NA_NA"
      for (pc in poste_cols) {
        dt[, (pc) := fifelse(is.na(get(pc)), "", trimws(as.character(get(pc))))]
      }
      dt[, poste_key := do.call(paste, c(.SD, sep = "_")), .SDcols = poste_cols]
      
      dt_posts <- dt[dt_mat_ok,
                     .(nb_postes = uniqueN(poste_key)),
                     by = .(ANNEE, MOIS_NUM, MOIS, MAT = trimws(as.character(get(MAT_COL))))]
    } else {
      dt_posts <- dt[dt_mat_ok,
                     .(nb_postes = .N),
                     by = .(ANNEE, MOIS_NUM, MOIS, MAT = trimws(as.character(get(MAT_COL))))]
    }
    
    if (debug_multipostes) {
      distrib <- dt_posts[, .(count = .N), by = nb_postes][order(nb_postes)]
      message(sprintf("      Matricules uniques: %s", format(uniqueN(dt_posts$MAT), big.mark = ",")))
      message(sprintf("      Distribution (top 10):"))
      for (i in 1:min(10, nrow(distrib))) {
        message(sprintf("        %d poste(s): %s (%.2f%%)",
                        distrib$nb_postes[i],
                        format(distrib$count[i], big.mark = ","),
                        100 * distrib$count[i] / sum(distrib$count)))
      }
    }
    
    dt_posts[, nb_postes_bucket := fifelse(
      nb_postes >= top_postes_bucket,
      paste0(top_postes_bucket, "+ postes"),
      paste0(nb_postes, " poste", fifelse(nb_postes > 1, "s", ""))
    )]
    
    multipostes <- dt_posts[
      , .(effectif = .N),
      by = .(ANNEE, MOIS_NUM, MOIS, nb_postes_bucket)
    ][
      , `:=`(
        total_matricules = sum(effectif),
        proportion = effectif / sum(effectif)
      ),
      by = .(ANNEE, MOIS_NUM, MOIS)
    ]
    
    multipostes_wide <- dcast(
      multipostes,
      ANNEE + MOIS_NUM + MOIS ~ nb_postes_bucket,
      value.var = c("effectif", "proportion"),
      fill = 0
    )
    setorder(multipostes_wide, ANNEE, MOIS_NUM)
    multipostes_wide[, MOIS_NUM := NULL]
    multipostes_by_year[[as.character(annee)]] <- multipostes_wide
    
    # -----------------------------
    # Indicateurs (table longue)
    # -----------------------------
    message("   📊 Indicateurs...")
    year_res <- list()
    
    # 1) Ensemble
    year_res[[length(year_res) + 1]] <- agg_stats(dt, "Ensemble")
    
    # 2) Sexe
    if (nrow(dt[SEXE_STD == "Homme"]) > 0) year_res[[length(year_res) + 1]] <- agg_stats(dt[SEXE_STD == "Homme"], "Homme")
    if (nrow(dt[SEXE_STD == "Femme"]) > 0) year_res[[length(year_res) + 1]] <- agg_stats(dt[SEXE_STD == "Femme"], "Femme")
    
    # 3) Non fonctionnaire (agrégé) + détail
    if (nrow(dt[STATUT_AGG == "Non fonctionnaire"]) > 0) year_res[[length(year_res) + 1]] <- agg_stats(dt[STATUT_AGG == "Non fonctionnaire"], "Non fonctionnaire")
    if (nrow(dt[STATUT == "Contractuel"]) > 0) year_res[[length(year_res) + 1]] <- agg_stats(dt[STATUT == "Contractuel"], "Contractuel")
    if (nrow(dt[STATUT == "Gens de maison"]) > 0) year_res[[length(year_res) + 1]] <- agg_stats(dt[STATUT == "Gens de maison"], "Gens de maison")
    
    # 4) Fonctionnaires par catégorie
    cats <- sort(unique(dt[STATUT == "Fonctionnaire", CATEGORIE]))
    for (cat in cats) {
      dt_cat <- dt[STATUT == "Fonctionnaire" & CATEGORIE == cat]
      if (nrow(dt_cat) > 0) {
        year_res[[length(year_res) + 1]] <- agg_stats(dt_cat, paste0("Fonctionnaire - Cat ", cat))
      }
    }
    
    indic_by_year[[as.character(annee)]] <- rbindlist(year_res, fill = TRUE)
    
    rm(dt, dt_posts)
    gc()
  }
  
  # -----------------------------
  # Consolidation (petite)
  # -----------------------------
  message("\n🔄 Consolidation finale...")
  indic_long <- rbindlist(indic_by_year, fill = TRUE)
  setorder(indic_long, ANNEE, MOIS_NUM, groupe)
  
  multipostes_final <- rbindlist(multipostes_by_year, fill = TRUE)
  
  # -----------------------------
  # Génération feuilles via pivot
  # -----------------------------
  message("📄 Construction des feuilles...")
  metrics <- c("Effectif", "Net moyen", "Brut moyen", "Net médian", "Brut médian",
               "Net min", "Net max", "Brut min", "Brut max")
  
  sheets <- list()
  for (m in metrics) {
    sh <- dcast(
      indic_long,
      ANNEE + MOIS_NUM + MOIS ~ groupe,
      value.var = m,
      fill = NA_real_
    )
    setorder(sh, ANNEE, MOIS_NUM)
    sh[, MOIS_NUM := NULL]
    sheets[[m]] <- sh
  }
  sheets[["Multi-postes"]] <- multipostes_final
  
  # -----------------------------
  # Export Excel (sans autosize coûteux)
  # -----------------------------
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
    
    # Largeurs simples et rapides
    setColWidths(wb, nm, cols = 1, widths = 10)  # ANNEE
    setColWidths(wb, nm, cols = 2, widths = 12)  # MOIS
    if (ncol(sheets[[nm]]) > 2) {
      setColWidths(wb, nm, cols = 3:ncol(sheets[[nm]]), widths = 18)
    }
    message(sprintf("   ✓ %s (%d lignes, %d colonnes)", nm, nrow(sheets[[nm]]), ncol(sheets[[nm]])))
  }
  
  saveWorkbook(wb, out_xlsx_path, overwrite = TRUE)
  message(sprintf("\n✅ Terminé: %s", out_xlsx_path))
  
  invisible(sheets)
}

# -----------------------------
# UTILISATION
# -----------------------------
panel_path <- "data/panel_solde_complet_2015_2025.parquet"

res <- build_indicateurs_salaire(
  panel_path = panel_path,
  out_xlsx_path = "indicateurs_salaire_2015_2025_OPT.xlsx",
  winsor_p = 0.01,
  winsor_min_n = 30,
  winsor_by = c("ANNEE", "STATUT_AGG"),     # recommandé
  winsor_fallback_by = c("ANNEE"),
  top_postes_bucket = 6,
  poste_identifier = c("EMPLOYEUR", "STRUCTURE", "MINISTERE", "SIRET", "CODE_EMPLOYEUR"),
  debug_multipostes = TRUE
)
