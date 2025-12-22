################################################################################
# ESTIMATION DU SALAIRE MOYEN AVEC TRAITEMENT DES DONNÉES MANQUANTES
# Implémentation des méthodes décrites dans la note méthodologique
################################################################################

# Chargement des packages nécessaires
library(tidyverse)
library(mice)        # Imputation multiple
library(survey)      # Pondération et estimations
library(broom)       # Nettoyage des résultats

data = "data/panel_solde_complet_2015_2025.parquet"

################################################################################
# CAS 1: DÉCLARATION PAR L'EMPLOYÉ (NIVEAU INDIVIDUEL)
################################################################################

estimation_cas1_individuel <- function(data) {
  # data doit contenir: salaire, R (indicateur déclaration), et variables X
  
  cat("\n=== CAS 1: Déclaration individuelle ===\n")
  
  # ÉTAPE 1: Modélisation de la probabilité de réponse (IPW)
  cat("\nÉtape 1: Calcul des probabilités de réponse...\n")
  
  # Modèle logistique pour P(R=1|X)
  modele_reponse <- glm(R ~ age + sexe + profession + anciennete + secteur,
                        data = data,
                        family = binomial(link = "logit"))
  
  # Probabilités prédites
  data$pi_hat <- predict(modele_reponse, type = "response")
  
  # Poids IPW stabilisés
  R_barre <- mean(data$R)
  data$w_ipw <- ifelse(data$R == 1, R_barre / data$pi_hat, 0)
  
  # Troncature des poids extrêmes (99e percentile)
  seuil_max <- quantile(data$w_ipw[data$R == 1], 0.99)
  data$w_ipw <- pmin(data$w_ipw, seuil_max)
  
  cat("Poids IPW calculés (min:", min(data$w_ipw[data$R == 1]), 
      ", max:", max(data$w_ipw[data$R == 1]), ")\n")
  
  # ÉTAPE 2: Imputation multiple
  cat("\nÉtape 2: Imputation multiple...\n")
  
  # Préparation des données pour MICE
  data_imp <- data %>%
    mutate(salaire_imp = ifelse(R == 1, salaire, NA))
  
  # Configuration de l'imputation
  M <- 20  # Nombre d'imputations
  
  # Méthode d'imputation: PMM (Predictive Mean Matching) pour le log-salaire
  data_imp$log_salaire <- log(data_imp$salaire_imp)
  
  imp <- mice(data_imp %>% 
                select(log_salaire, age, sexe, profession, anciennete, secteur),
              m = M,
              method = "pmm",
              printFlag = FALSE,
              seed = 123)
  
  # ÉTAPE 3: Estimation combinée IPW + MI
  cat("\nÉtape 3: Estimation combinée...\n")
  
  estimations <- numeric(M)
  variances <- numeric(M)
  
  for(m in 1:M) {
    # Extraction du jeu imputé m
    data_m <- complete(imp, m)
    data_m$salaire_imp <- exp(data_m$log_salaire)
    data_m$w_ipw <- data$w_ipw
    
    # Estimation pondérée
    estimations[m] <- weighted.mean(data_m$salaire_imp, w = data_m$w_ipw)
    
    # Variance intra-imputation (approximation)
    variances[m] <- weighted.mean((data_m$salaire_imp - estimations[m])^2, 
                                   w = data_m$w_ipw) / sum(data_m$w_ipw > 0)
  }
  
  # Règles de combinaison de Rubin
  mu_final <- mean(estimations)
  W <- mean(variances)  # Variance intra
  B <- var(estimations)  # Variance inter
  V_total <- W + (1 + 1/M) * B
  
  # Intervalle de confiance
  df <- (M - 1) * (1 + W / ((1 + 1/M) * B))^2
  ic_lower <- mu_final - qt(0.975, df) * sqrt(V_total)
  ic_upper <- mu_final + qt(0.975, df) * sqrt(V_total)
  
  resultats <- list(
    salaire_moyen = mu_final,
    erreur_type = sqrt(V_total),
    ic_95 = c(ic_lower, ic_upper),
    n_observations = nrow(data),
    taux_reponse = mean(data$R),
    details = data.frame(
      imputation = 1:M,
      estimation = estimations,
      variance = variances
    )
  )
  
  cat("\n--- Résultats CAS 1 ---\n")
  cat("Salaire moyen estimé:", round(mu_final, 2), "\n")
  cat("Erreur-type:", round(sqrt(V_total), 2), "\n")
  cat("IC 95%: [", round(ic_lower, 2), ",", round(ic_upper, 2), "]\n")
  cat("Taux de réponse:", round(mean(data$R) * 100, 1), "%\n")
  
  return(resultats)
}

################################################################################
# CAS 2: DÉCLARATION PAR L'EMPLOYEUR (NIVEAU ENTREPRISE)
################################################################################

estimation_cas2_employeur <- function(data_indiv) {
  # data_indiv: données individuelles avec entreprise_id, mois, salaire, R, etc.
  
  cat("\n=== CAS 2: Déclaration par l'employeur ===\n")
  
  # ÉTAPE 1: Construction de la base entreprise-mois
  cat("\nÉtape 1: Agrégation au niveau entreprise-mois...\n")
  
  base_entreprise <- data_indiv %>%
    group_by(entreprise_id, mois) %>%
    summarise(
      N_jt = n(),  # Effectif total
      n_jt = sum(R),  # Nombre de salariés déclarés
      D_jt = as.numeric(n_jt > 0),  # Indicateur de déclaration
      salaire_moy_obs = ifelse(n_jt > 0, mean(salaire[R == 1], na.rm = TRUE), NA),
      age_moyen = mean(age, na.rm = TRUE),
      prop_femmes = mean(sexe == "F", na.rm = TRUE),
      taille = first(taille_entreprise),
      secteur = first(secteur),
      .groups = "drop"
    ) %>%
    arrange(entreprise_id, mois)
  
  # Historique de déclaration (décalé)
  base_entreprise <- base_entreprise %>%
    group_by(entreprise_id) %>%
    mutate(
      hist_decl_1 = lag(D_jt, 1, default = 1),
      hist_decl_3 = rollmean(lag(D_jt, 1:3, default = 1), 3, fill = 1, align = "right")
    ) %>%
    ungroup()
  
  cat("Base entreprise-mois créée:", nrow(base_entreprise), "observations\n")
  
  # ÉTAPE 2: Modélisation de la probabilité de déclaration
  cat("\nÉtape 2: Modélisation de la probabilité de déclaration...\n")
  
  modele_decl <- glm(D_jt ~ log(taille) + age_moyen + prop_femmes + 
                       secteur + hist_decl_1 + hist_decl_3 + factor(mois),
                     data = base_entreprise,
                     family = binomial(link = "logit"))
  
  # Probabilités prédites
  base_entreprise$pi_jt <- predict(modele_decl, type = "response")
  
  # Poids stabilisés
  D_barre <- mean(base_entreprise$D_jt)
  base_entreprise$w_jt <- ifelse(base_entreprise$D_jt == 1, 
                                  D_barre / base_entreprise$pi_jt, 
                                  0)
  
  # Troncature
  seuil_max <- quantile(base_entreprise$w_jt[base_entreprise$D_jt == 1], 0.99)
  base_entreprise$w_jt <- pmin(base_entreprise$w_jt, seuil_max)
  
  # ÉTAPE 3: Imputation multiple des salaires moyens entreprise-mois
  cat("\nÉtape 3: Imputation multiple...\n")
  
  M <- 20
  
  # Préparation pour imputation
  base_imp <- base_entreprise %>%
    mutate(log_sal_moy = log(salaire_moy_obs))
  
  imp <- mice(base_imp %>% 
                select(log_sal_moy, log(taille), age_moyen, prop_femmes, 
                       secteur, hist_decl_1, hist_decl_3, mois),
              m = M,
              method = "pmm",
              printFlag = FALSE,
              seed = 456)
  
  # ÉTAPE 4: Estimation du salaire moyen national
  cat("\nÉtape 4: Estimation finale...\n")
  
  estimations <- numeric(M)
  
  for(m in 1:M) {
    data_m <- complete(imp, m)
    data_m$salaire_moy_imp <- exp(data_m$`log(taille)`)  # Correction: utiliser la bonne variable
    data_m <- bind_cols(base_entreprise %>% select(N_jt, w_jt), data_m)
    
    # Estimation pondérée par effectifs et poids
    estimations[m] <- weighted.mean(data_m$salaire_moy_imp, 
                                     w = data_m$w_jt * data_m$N_jt)
  }
  
  # Règles de Rubin
  mu_final <- mean(estimations)
  B <- var(estimations)
  V_total <- B * (1 + 1/M)  # Variance simplifiée
  
  ic_lower <- mu_final - 1.96 * sqrt(V_total)
  ic_upper <- mu_final + 1.96 * sqrt(V_total)
  
  resultats <- list(
    salaire_moyen = mu_final,
    erreur_type = sqrt(V_total),
    ic_95 = c(ic_lower, ic_upper),
    n_entreprises = n_distinct(base_entreprise$entreprise_id),
    taux_declaration = mean(base_entreprise$D_jt),
    details = data.frame(
      imputation = 1:M,
      estimation = estimations
    )
  )
  
  cat("\n--- Résultats CAS 2 ---\n")
  cat("Salaire moyen estimé:", round(mu_final, 2), "\n")
  cat("Erreur-type:", round(sqrt(V_total), 2), "\n")
  cat("IC 95%: [", round(ic_lower, 2), ",", round(ic_upper, 2), "]\n")
  cat("Taux de déclaration entreprises:", round(mean(base_entreprise$D_jt) * 100, 1), "%\n")
  
  return(resultats)
}

################################################################################
# CAS 3: DÉCLARATION EMPLOYEUR PARTIELLE (DEUX NIVEAUX)
################################################################################

estimation_cas3_partielle <- function(data_indiv) {
  # Méthode en deux étages
  
  cat("\n=== CAS 3: Déclaration partielle (deux niveaux) ===\n")
  
  # ÉTAPE 1: Probabilités à deux niveaux
  cat("\nÉtape 1: Calcul des probabilités multi-niveaux...\n")
  
  # Niveau entreprise-mois
  base_entreprise <- data_indiv %>%
    group_by(entreprise_id, mois) %>%
    summarise(
      N_jt = n(),
      n_jt = sum(R),
      R_jt = as.numeric(n_jt > 0),
      taille = first(taille_entreprise),
      secteur = first(secteur),
      .groups = "drop"
    )
  
  # Modèle niveau entreprise
  modele_R <- glm(R_jt ~ log(taille) + secteur + factor(mois),
                  data = base_entreprise,
                  family = binomial())
  
  base_entreprise$pi_jt <- predict(modele_R, type = "response")
  
  # Jointure avec données individuelles
  data_indiv <- data_indiv %>%
    left_join(base_entreprise %>% select(entreprise_id, mois, R_jt, pi_jt),
              by = c("entreprise_id", "mois"))
  
  # Niveau individuel (conditionnel à R_jt = 1)
  data_decl <- data_indiv %>% filter(R_jt == 1)
  
  modele_S <- glm(R ~ age + sexe + profession + anciennete,
                  data = data_decl,
                  family = binomial())
  
  data_decl$rho_ijt <- predict(modele_S, type = "response")
  
  # ÉTAPE 2: Poids niveau entreprise
  cat("\nÉtape 2: Correction niveau entreprise...\n")
  
  R_barre <- mean(base_entreprise$R_jt)
  base_entreprise$w_E <- ifelse(base_entreprise$R_jt == 1,
                                 R_barre / base_entreprise$pi_jt,
                                 0)
  
  # ÉTAPE 3: Poids niveau individuel
  cat("\nÉtape 3: Correction niveau individuel...\n")
  
  S_barre <- mean(data_decl$R)
  data_decl$w_I <- ifelse(data_decl$R == 1,
                          S_barre / data_decl$rho_ijt,
                          0)
  
  # ÉTAPE 4: Estimation finale
  cat("\nÉtape 4: Estimation combinée...\n")
  
  # Jointure des poids
  data_final <- data_decl %>%
    left_join(base_entreprise %>% select(entreprise_id, mois, w_E),
              by = c("entreprise_id", "mois")) %>%
    filter(R == 1)  # Seulement les salaires observés
  
  # Poids total
  data_final$w_total <- data_final$w_E * data_final$w_I
  
  # Troncature
  seuil <- quantile(data_final$w_total, 0.99)
  data_final$w_total <- pmin(data_final$w_total, seuil)
  
  # Estimation
  mu_final <- weighted.mean(data_final$salaire, w = data_final$w_total)
  
  # Variance (bootstrap ou approximation)
  variance_approx <- weighted.mean((data_final$salaire - mu_final)^2,
                                   w = data_final$w_total) / nrow(data_final)
  
  ic_lower <- mu_final - 1.96 * sqrt(variance_approx)
  ic_upper <- mu_final + 1.96 * sqrt(variance_approx)
  
  resultats <- list(
    salaire_moyen = mu_final,
    erreur_type = sqrt(variance_approx),
    ic_95 = c(ic_lower, ic_upper),
    n_entreprises = n_distinct(data_final$entreprise_id),
    n_salaries = nrow(data_final),
    taux_declaration_entreprise = mean(base_entreprise$R_jt),
    taux_declaration_individu = mean(data_decl$R)
  )
  
  cat("\n--- Résultats CAS 3 ---\n")
  cat("Salaire moyen estimé:", round(mu_final, 2), "\n")
  cat("Erreur-type:", round(sqrt(variance_approx), 2), "\n")
  cat("IC 95%: [", round(ic_lower, 2), ",", round(ic_upper, 2), "]\n")
  cat("Taux déclaration entreprise:", round(mean(base_entreprise$R_jt) * 100, 1), "%\n")
  cat("Taux déclaration individuelle:", round(mean(data_decl$R) * 100, 1), "%\n")
  
  return(resultats)
}

################################################################################
# EXEMPLE D'UTILISATION AVEC DONNÉES SIMULÉES
################################################################################

# Simulation de données pour CAS 1
set.seed(123)
n <- 5000

data_cas1 <- data.frame(
  age = rnorm(n, 40, 10),
  sexe = sample(c("H", "F"), n, replace = TRUE),
  profession = sample(c("Cadre", "Technicien", "Employé", "Ouvrier"), n, replace = TRUE),
  anciennete = rexp(n, 1/10),
  secteur = sample(c("Industrie", "Services", "Commerce"), n, replace = TRUE)
)

# Salaire simulé
data_cas1$salaire <- exp(8 + 0.02 * data_cas1$age + 
                          0.3 * (data_cas1$sexe == "H") +
                          0.4 * (data_cas1$profession == "Cadre") +
                          rnorm(n, 0, 0.3))

# Probabilité de réponse (mécanisme MAR)
logit_R <- -1 + 0.02 * data_cas1$age - 0.5 * (data_cas1$sexe == "F")
data_cas1$R <- rbinom(n, 1, plogis(logit_R))

cat("\n################################################################################\n")
cat("DÉMONSTRATION AVEC DONNÉES SIMULÉES\n")
cat("################################################################################\n")

# Application CAS 1
resultats_cas1 <- estimation_cas1_individuel(data_cas1)

cat("\n\nPour utiliser avec vos propres données:\n")
cat("1. Préparez vos données avec les colonnes nécessaires\n")
cat("2. Appelez la fonction appropriée:\n")
cat("   - estimation_cas1_individuel(data) pour déclaration individuelle\n")
cat("   - estimation_cas2_employeur(data) pour déclaration employeur complète\n")
cat("   - estimation_cas3_partielle(data) pour déclaration partielle\n")

