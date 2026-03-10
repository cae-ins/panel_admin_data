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

# Adapter les dates aux hausses réelles connues
evenements <- data.frame(
  date  = as.Date(c("2018-01-01", "2020-01-01", "2023-01-01")),
  label = c("Hausse SMIG 2018", "Hausse SMIG 2020", "Revalorisation 2023"),
  type  = c("SMIG", "SMIG", "Fonctionnaires"),
  stringsAsFactors = FALSE
)
couleur_evt <- c("SMIG" = "#E74C3C", "Fonctionnaires" = "#27AE60")

# ============================================================
# 1. LECTURE
# ============================================================

cat("Lecture parquet...\n")

dt <- open_dataset(PARQUET_FILE) |>
  select(periode, montant_brut) |>
  filter(!is.na(montant_brut), montant_brut > 0) |>
  collect() |>
  as.data.table()

cat(sprintf("  OK : %s lignes chargees\n\n", format(nrow(dt), big.mark = " ")))

# ============================================================
# 2. PARSING DES DATES
# ============================================================

dt[, mois  := as.integer(substr(periode, 1, 2))]
dt[, annee := as.integer(substr(periode, 3, 6))]
dt[, date  := as.Date(paste(annee, mois, "01", sep = "-"))]

cat("Periodes disponibles :", paste(sort(unique(dt$annee)), collapse = ", "), "\n\n")

# ============================================================
# 3. AGREGATIONS MENSUELLES
# ============================================================

cat("Calcul des agregats...\n")

agg <- dt[, .(
  moy      = mean(montant_brut,           na.rm = TRUE),
  med      = median(montant_brut,         na.rm = TRUE),
  p25      = quantile(montant_brut, 0.25, na.rm = TRUE),
  p75      = quantile(montant_brut, 0.75, na.rm = TRUE),
  effectif = .N
), by = .(date, annee, mois)]
setorder(agg, date)

cat(sprintf("  OK : %d points mensuels\n\n", nrow(agg)))

# ============================================================
# 4. THEME
# ============================================================

theme_panel <- function(base_size = 12) {
  theme_minimal(base_size = base_size) +
    theme(
      plot.title         = element_text(face = "bold", size = base_size + 3,
                                        hjust = 0, margin = margin(b = 4)),
      plot.subtitle      = element_text(color = "grey40", size = base_size - 1,
                                        hjust = 0, margin = margin(b = 10)),
      plot.caption       = element_text(color = "grey55", size = base_size - 3,
                                        hjust = 1, margin = margin(t = 8)),
      axis.title         = element_text(face = "bold", size = base_size - 1, color = "grey30"),
      axis.text          = element_text(size = base_size - 2, color = "grey40"),
      axis.text.x        = element_text(angle = 45, hjust = 1),
      panel.grid.minor   = element_blank(),
      panel.grid.major.x = element_line(color = "grey92", linewidth = 0.4),
      panel.grid.major.y = element_line(color = "grey88", linewidth = 0.4),
      plot.background    = element_rect(fill = "white", color = NA),
      panel.background   = element_rect(fill = "white", color = NA),
      legend.position    = "bottom",
      legend.text        = element_text(size = base_size - 2)
    )
}

# ============================================================
# 5. GRAPHIQUE 1 — Tendance globale
# ============================================================

cat("Construction graphique 1...\n")

y_annot <- max(agg$moy, na.rm = TRUE) * 1.05

g1 <- ggplot(agg, aes(x = date)) +
  geom_ribbon(aes(ymin = p25, ymax = p75), fill = "#1565C0", alpha = 0.10) +
  geom_vline(
    data = evenements,
    aes(xintercept = date, color = type),
    linetype = "dashed", linewidth = 0.8, alpha = 0.9,
    inherit.aes = FALSE
  ) +
  geom_label(
    data = evenements,
    aes(x = date, y = y_annot, label = label, color = type),
    size = 3, hjust = -0.05, fontface = "bold",
    fill = "white", label.size = 0,
    inherit.aes = FALSE
  ) +
  geom_line(aes(y = med, linetype = "Mediane"), color = "#42A5F5",
            linewidth = 0.9, alpha = 0.85) +
  geom_line(aes(y = moy, linetype = "Moyenne"), color = "#1565C0", linewidth = 1.4) +
  geom_point(aes(y = moy), color = "#1565C0", size = 1.5, alpha = 0.55) +
  scale_y_continuous(
    labels = label_number(big.mark = " ", suffix = " F"),
    expand = expansion(mult = c(0.02, 0.16))
  ) +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y",
               expand = expansion(mult = c(0.01, 0.01))) +
  scale_linetype_manual(
    values = c("Moyenne" = "solid", "Mediane" = "dashed"), name = NULL
  ) +
  scale_color_manual(
    values = couleur_evt, name = "Evenement",
    guide  = guide_legend(override.aes = list(linewidth = 1.2))
  ) +
  labs(
    title    = "Evolution du salaire brut mensuel moyen - 2015 a 2025",
    subtitle = "Tous agents | salaires > 0 | ruban = interquartile (P25-P75)",
    x        = NULL,
    y        = "Salaire brut (FCFA)",
    caption  = "Source : Base panel solde 2015-2025"
  ) +
  theme_panel()

# ============================================================
# 6. GRAPHIQUE 2 — Effectifs mensuels
# ============================================================

cat("Construction graphique 2...\n")

g2 <- ggplot(agg, aes(x = date, y = effectif / 1000)) +
  geom_vline(
    data = evenements,
    aes(xintercept = date),
    linetype = "dashed", color = "grey60", linewidth = 0.6, alpha = 0.7,
    inherit.aes = FALSE
  ) +
  geom_area(fill = "#546E7A", alpha = 0.18) +
  geom_line(color = "#37474F", linewidth = 1.0) +
  scale_y_continuous(
    labels = label_number(suffix = " k", big.mark = " "),
    expand = expansion(mult = c(0.02, 0.08))
  ) +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y",
               expand = expansion(mult = c(0.01, 0.01))) +
  labs(
    title    = "Effectif mensuel enregistre",
    subtitle = "Nombre de lignes de paie avec salaire > 0",
    x        = NULL,
    y        = "Effectif (milliers)",
    caption  = "Source : Base panel solde 2015-2025"
  ) +
  theme_panel()

# ============================================================
# 7. ASSEMBLAGE & EXPORT
# ============================================================

cat("Assemblage...\n")

p_final <- (g1 / g2) +
  plot_layout(heights = c(2, 1)) +
  plot_annotation(
    title   = "Dynamiques salariales des agents de l'Etat - 2015-2025",
    caption = paste0("Produit le ", format(Sys.Date(), "%d/%m/%Y")),
    theme   = theme(
      plot.title      = element_text(face = "bold", size = 17, hjust = 0),
      plot.caption    = element_text(color = "grey55", size = 9, hjust = 1),
      plot.background = element_rect(fill = "white", color = NA)
    )
  )

output_pdf <- file.path(OUTPUT_DIR, "evolution_salaire_brut_2015_2025.pdf")
ggsave(output_pdf, p_final, width = 14, height = 16, units = "in", device = cairo_pdf)
cat(sprintf("\nPDF : %s\n", output_pdf))

output_png <- file.path(OUTPUT_DIR, "evolution_salaire_brut_2015_2025.png")
ggsave(output_png, p_final, width = 14, height = 16, units = "in", dpi = 180)
cat(sprintf("PNG : %s\n\n", output_png))

cat("TERMINE.\n")

# ============================================================
# 7. ASSEMBLAGE & EXPORT
# ============================================================

# Vecteur ordonné par date                                                                                                                         salaires_moyens <- agg[order(date), moy]
                                                                                                                                                  
# Avec les noms mois-année pour s'y retrouver
salaires_moyens <- agg[order(date), moy]
names(salaires_moyens) <- agg[order(date), format(date, "%m/%Y")]

print(salaires_moyens)

# Si tu veux aussi le tableau complet date + moyenne :

agg[order(date), .(date, moy)]

# Et si tu veux l'exporter en CSV :

fwrite(agg[order(date), .(date, moy, med, effectif)],
file.path(OUTPUT_DIR, "salaires_moyens_mensuels.csv"))
