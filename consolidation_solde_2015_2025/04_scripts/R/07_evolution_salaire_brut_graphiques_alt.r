# ============================================================
# ÉVOLUTION DU SALAIRE BRUT MOYEN MENSUEL 2015-2025
# Analyse de l'impact des hausses de SMIG / revalorisation
# ============================================================

rm(list = ls()); gc()
suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(data.table)
  library(ggplot2)
  library(scales)
  library(patchwork)
})

# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT <- "C:/Users/f.migone/Desktop/projects/panel_admin_data/consolidation_solde_2015_2025"
PARQUET_FILE <- file.path(PROJECT_ROOT, "03_data_output/base_finale/base_consolidee_2015_2025.parquet")
OUTPUT_DIR   <- file.path(PROJECT_ROOT, "03_data_output/export")
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

# Événements à annoter
evenements <- data.frame(
  date    = as.Date(c("2018-01-01", "2020-01-01", "2023-01-01")),
  label   = c("Hausse SMIG 2018", "Hausse SMIG 2020", "Revalorisation 2023"),
  type    = c("SMIG", "SMIG", "Fonctionnaires"),
  stringsAsFactors = FALSE
)

couleur_evenement <- c("SMIG" = "#E74C3C", "Fonctionnaires" = "#27AE60")
GRADES_VALIDES    <- c(paste0("A", 1:7), paste0("B", 1:6), paste0("C", 1:5), paste0("D", 1:3))

# ============================================================
# 1. LECTURE & NETTOYAGE (Correction Proactive)
# ============================================================

cat("Lecture parquet (colonnes sélectionnées)...\n")

dt <- open_dataset(PARQUET_FILE) |>
  select(periode, montant_brut, grade) |>
  filter(!is.na(montant_brut), montant_brut > 0) |>
  collect() |>
  as.data.table()

# --- Normalisation critique ---
# On nettoie les espaces et on force la casse pour correspondre à GRADES_VALIDES
dt[, grade := toupper(trimws(as.character(grade)))]
dt[, `:=`(
  mois  = as.integer(substr(periode, 1, 2)),
  annee = as.integer(substr(periode, 3, 6))
)]
dt[, date := as.Date(paste(annee, mois, "01", sep = "-"))]
dt[, categorie := substr(grade, 1, 1)]

cat(sprintf("  ✓ %s lignes chargées\n", format(nrow(dt), big.mark = " ")))

# ============================================================
# 2. AGRÉGATIONS
# ============================================================

cat("Calcul des agrégats...\n")

# --- 2a. Global ---
agg_global <- dt[, .(
  moy = mean(montant_brut, na.rm = TRUE),
  med = median(montant_brut, na.rm = TRUE),
  p25 = quantile(montant_brut, 0.25, na.rm = TRUE),
  p75 = quantile(montant_brut, 0.75, na.rm = TRUE),
  effectif = .N
), by = .(date, annee, mois)]
setorder(agg_global, date)

# --- 2b. Par catégorie (Sécurisé) ---
agg_cat <- dt[grade %in% GRADES_VALIDES, .(
  moy = mean(montant_brut, na.rm = TRUE),
  med = median(montant_brut, na.rm = TRUE),
  effectif = .N
), by = .(date, annee, mois, categorie)]

# Test de sécurité pour éviter l'erreur de facettage
if (nrow(agg_cat) == 0) {
  stop("ERREUR : Aucune donnée ne correspond aux GRADES_VALIDES. 
       Exemple de grades trouvés dans vos données : ", paste(head(unique(dt$grade)), collapse=", "))
}

agg_cat <- agg_cat[categorie %in% c("A", "B", "C", "D")]
setorder(agg_cat, date)

# ============================================================
# 3. THÈME & FONCTIONS GRAPHIQUES
# ============================================================

theme_panel <- function(base_size = 12) {
  theme_minimal(base_size = base_size) +
    theme(
      plot.title         = element_text(face = "bold", size = base_size + 3, hjust = 0),
      plot.subtitle      = element_text(color = "grey40", size = base_size - 1, hjust = 0),
      axis.text.x        = element_text(angle = 45, hjust = 1),
      panel.grid.minor   = element_blank(),
      legend.position    = "bottom",
      strip.background   = element_rect(fill = "grey95", color = NA),
      strip.text         = element_text(face = "bold")
    )
}

ajouter_evenements <- function(p, data_agg, include_labels = TRUE) {
  if (nrow(evenements) == 0) return(p)
  y_max <- max(data_agg$moy, na.rm = TRUE)
  
  p <- p + geom_vline(data = evenements, aes(xintercept = date, color = type),
                 linetype = "dashed", linewidth = 0.7, alpha = 0.8, inherit.aes = FALSE)
  
  if(include_labels) {
    p <- p + geom_label(data = evenements, aes(x = date, y = y_max * 1.05, label = label, color = type),
                   size = 3, fontface = "bold", fill = "white", label.size = 0, inherit.aes = FALSE)
  }
  return(p)
}

# ============================================================
# 4. CONSTRUCTION DES GRAPHIQUES
# ============================================================

# G1 : Global
g1 <- ggplot(agg_global, aes(x = date)) +
  geom_ribbon(aes(ymin = p25, ymax = p75), fill = "#1565C0", alpha = 0.1) +
  geom_line(aes(y = med, linetype = "Médiane"), color = "#42A5F5", linewidth = 0.8) +
  geom_line(aes(y = moy, linetype = "Moyenne"), color = "#1565C0", linewidth = 1.2) +
  scale_y_continuous(labels = label_number(big.mark = " ", suffix = " F")) +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  scale_linetype_manual(values = c("Moyenne" = "solid", "Médiane" = "dashed"), name = NULL) +
  labs(title = "Évolution du salaire brut mensuel moyen", y = "Brut (FCFA)", x = NULL) +
  theme_panel()

g1 <- ajouter_evenements(g1, agg_global)

# G2 : Par catégorie (Facettage)
g2 <- ggplot(agg_cat, aes(x = date, y = moy, color = categorie)) +
  geom_line(linewidth = 1) +
  facet_wrap(~ categorie, scales = "free_y", ncol = 2) +
  scale_y_continuous(labels = label_number(big.mark = " ", suffix = " F")) +
  scale_color_manual(values = c("A"="#1565C0", "B"="#2E7D32", "C"="#E65100", "D"="#6A1B9A")) +
  labs(title = "Par catégorie de grade", y = "Salaire moyen", x = NULL) +
  theme_panel() + theme(legend.position = "none")

g2 <- ajouter_evenements(g2, agg_cat, include_labels = FALSE)

# G3 : Effectifs
g3 <- ggplot(agg_global, aes(x = date, y = effectif / 1000)) +
  geom_area(fill = "#546E7A", alpha = 0.15) +
  geom_line(color = "#546E7A") +
  scale_y_continuous(labels = label_number(suffix = " k")) +
  labs(title = "Couverture des données (Effectifs)", y = "Agents (milliers)", x = NULL) +
  theme_panel()

# ============================================================
# 5. ASSEMBLAGE ET EXPORT
# ============================================================

p_final <- (g1 / g2 / g3) + 
  plot_layout(heights = c(1.2, 1.5, 0.7)) +
  plot_annotation(
    title = "Analyse des dynamiques salariales 2015-2025",
    caption = paste0("Source : Panel Administratif | Export : ", Sys.Date())
  )

output_pdf <- file.path(OUTPUT_DIR, "evolution_salaire_2015_2025.pdf")
ggsave(output_pdf, p_final, width = 12, height = 18, device = cairo_pdf)

cat(sprintf("\n✅ Traitement terminé. Fichier disponible : %s\n", output_pdf))
