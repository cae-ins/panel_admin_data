-- ============================================================
-- superset_views.sql
-- Panel Admin - Salaires Fonction Publique Ivoirienne
-- Vues PostgreSQL alignees sur la methodologie ANSTAT/CAE
--
-- Dependances : table `panel` chargee depuis
--   gold/panel_admin/base_avec_statut_corrigee.parquet
-- ============================================================

-- ------------------------------------------------------------
-- Nettoyage
-- ------------------------------------------------------------
DROP VIEW IF EXISTS
    v_ecart_salarial_hf,
    v_taux_multipostes,
    v_multipostes_grade,
    v_indicateurs_annuels,
    v_indicateurs_bareme_sexe,
    v_indicateurs_bareme_grade,
    v_indicateurs_bareme,
    v_indicateurs_sexe_grade,
    v_indicateurs_grade,
    v_indicateurs_citp_gg,
    v_indicateurs_citp,
    v_salaire_moyen_sexe,
    v_indicateurs_global,
    v_panel_base
CASCADE;

DROP MATERIALIZED VIEW IF EXISTS mv_panel_base CASCADE;

-- ------------------------------------------------------------
-- 0. Vue de base methodologique
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_panel_base AS
SELECT
    matricule,
    COALESCE(
        NULLIF("SEXE_STD", ''),
        CASE
            WHEN UPPER(COALESCE(sexe, '')) IN ('MASCULIN','M','H','HOMME','MALE','1') THEN 'Homme'
            WHEN UPPER(COALESCE(sexe, '')) IN ('FEMININ','F','FEMME','FEMALE','2') THEN 'Femme'
            ELSE 'Inconnu'
        END
    ) AS sexe,
    "GRADE" AS grade_principal,
    CASE
        WHEN "GRADE" IN ('A3','A4','A5','A6','A7') THEN 'Categorie A'
        WHEN "GRADE" IN ('B1','B3')                THEN 'Categorie B'
        WHEN "GRADE" IN ('C1','C2','C3')           THEN 'Categorie C'
        WHEN "GRADE" IN ('D1','D2')                THEN 'Categorie D'
        ELSE 'Autre'
    END AS categorie_grade,
    CASE
        WHEN TRIM(COALESCE("Code_CITP", '')) ~ '^[0-9]'
        THEN LEFT(TRIM("Code_CITP"), 1)::integer
        ELSE NULL
    END AS citp_grand_groupe,
    NULLIF(TRIM(COALESCE("Code_CITP", '')), '') AS code_citp,
    NULLIF(TRIM(COALESCE("Metier_CITP", '')), '') AS metier_citp,
    annee,
    mois,
    TO_DATE(annee::text || '-' || LPAD(mois::text, 2, '0') || '-01', 'YYYY-MM-DD') AS date_mois,
    organisme,
    emploi,
    COALESCE(NULLIF(bareme, ''), 'Bareme non renseigne') AS bareme,
    COALESCE(NULLIF(bareme_detail, ''), NULLIF(bareme, ''), 'Bareme non renseigne') AS bareme_detail,
    COALESCE("NB_POSTES", 1) AS nb_postes,
    montant_brut,
    montant_net,
    montant_brut_sans_ind_sans_alloc,
    montant_brut_total,
    "Statut" AS statut_bareme,
    "Statut_Total" AS statut_total_bareme
FROM panel
WHERE annee BETWEEN 2015 AND 2099
  AND COALESCE("CODE_ORGANISME"::text, '') <> '59'
  AND "GRADE" IN ('A3','A4','A5','A6','A7','B1','B3','C1','C2','C3','D1','D2');

CREATE MATERIALIZED VIEW mv_panel_base AS SELECT * FROM v_panel_base;

CREATE INDEX idx_mv_annee_mois       ON mv_panel_base(annee, mois);
CREATE INDEX idx_mv_grade            ON mv_panel_base(grade_principal);
CREATE INDEX idx_mv_bareme           ON mv_panel_base(bareme);
CREATE INDEX idx_mv_date             ON mv_panel_base(date_mois);
CREATE INDEX idx_mv_sexe             ON mv_panel_base(sexe);
CREATE INDEX idx_mv_citp             ON mv_panel_base(citp_grand_groupe);
CREATE INDEX idx_mv_statut_m1        ON mv_panel_base(statut_bareme);
CREATE INDEX idx_mv_statut_m2        ON mv_panel_base(statut_total_bareme);
CREATE INDEX idx_mv_revenu_m1        ON mv_panel_base(montant_brut_sans_ind_sans_alloc);
CREATE INDEX idx_mv_revenu_m2        ON mv_panel_base(montant_brut_total);

-- ------------------------------------------------------------
-- 1. M1 - salaire strict par sexe
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_salaire_moyen_sexe AS
SELECT
    annee, mois, date_mois, sexe,
    COUNT(*) AS effectif,
    ROUND(AVG(montant_brut_sans_ind_sans_alloc)::numeric, 0) AS salaire_moyen,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY montant_brut_sans_ind_sans_alloc)::numeric, 0) AS salaire_median,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY montant_brut_sans_ind_sans_alloc)::numeric, 0) AS salaire_p25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY montant_brut_sans_ind_sans_alloc)::numeric, 0) AS salaire_p75
FROM mv_panel_base
WHERE statut_bareme = 'Conforme au barème'
  AND sexe IN ('Homme', 'Femme')
  AND montant_brut_sans_ind_sans_alloc IS NOT NULL
GROUP BY annee, mois, date_mois, sexe
ORDER BY annee, mois, sexe;

-- ------------------------------------------------------------
-- 2. M1 - indicateurs globaux mensuels
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_indicateurs_global AS
SELECT
    annee, mois, date_mois,
    COUNT(*) AS effectif,
    ROUND(AVG(montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_moyen,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_median,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_p25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_p75,
    ROUND(MIN(montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_min,
    ROUND(MAX(montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_max
FROM mv_panel_base
WHERE statut_bareme = 'Conforme au barème'
  AND montant_brut_sans_ind_sans_alloc IS NOT NULL
GROUP BY annee, mois, date_mois
ORDER BY annee, mois;

-- ------------------------------------------------------------
-- 3. M1 - par CITP
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_indicateurs_citp AS
SELECT
    annee, mois, date_mois,
    citp_grand_groupe, code_citp, metier_citp,
    COUNT(*) AS effectif,
    ROUND(AVG(montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_moyen,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_median,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_p25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_p75,
    ROUND(MIN(montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_min,
    ROUND(MAX(montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_max
FROM mv_panel_base
WHERE statut_bareme = 'Conforme au barème'
  AND citp_grand_groupe IS NOT NULL
  AND montant_brut_sans_ind_sans_alloc IS NOT NULL
GROUP BY annee, mois, date_mois, citp_grand_groupe, code_citp, metier_citp
ORDER BY annee, mois, citp_grand_groupe;

CREATE OR REPLACE VIEW v_indicateurs_citp_gg AS
SELECT
    annee, mois, date_mois,
    citp_grand_groupe,
    CASE citp_grand_groupe
        WHEN 1 THEN '1 - Directeurs et cadres'
        WHEN 2 THEN '2 - Professions intellectuelles'
        WHEN 3 THEN '3 - Professions intermediaires'
        WHEN 4 THEN '4 - Employes administratifs'
        WHEN 5 THEN '5 - Services et ventes'
        WHEN 6 THEN '6 - Agriculture'
        WHEN 7 THEN '7 - Metiers qualifies'
        WHEN 8 THEN '8 - Operateurs machines'
        WHEN 9 THEN '9 - Professions elementaires'
        ELSE 'Non classe'
    END AS libelle_citp,
    COUNT(*) AS effectif,
    ROUND(AVG(montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_moyen,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_median,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_p25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_p75
FROM mv_panel_base
WHERE statut_bareme = 'Conforme au barème'
  AND citp_grand_groupe BETWEEN 1 AND 9
  AND montant_brut_sans_ind_sans_alloc IS NOT NULL
GROUP BY annee, mois, date_mois, citp_grand_groupe
ORDER BY annee, mois, citp_grand_groupe;

-- ------------------------------------------------------------
-- 4. M1 - par grade et sexe x grade
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_indicateurs_grade AS
SELECT
    annee, mois, date_mois, grade_principal,
    COUNT(*) AS effectif,
    ROUND(AVG(montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_moyen,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_median,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_p25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_p75,
    ROUND(MIN(montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_min,
    ROUND(MAX(montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_max
FROM mv_panel_base
WHERE statut_bareme = 'Conforme au barème'
  AND montant_brut_sans_ind_sans_alloc IS NOT NULL
GROUP BY annee, mois, date_mois, grade_principal
ORDER BY annee, mois, grade_principal;

CREATE OR REPLACE VIEW v_indicateurs_sexe_grade AS
SELECT
    annee, mois, date_mois, sexe, grade_principal,
    COUNT(*) AS effectif,
    ROUND(AVG(montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_moyen,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_median,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_p25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_p75,
    ROUND(MIN(montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_min,
    ROUND(MAX(montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_max
FROM mv_panel_base
WHERE statut_bareme = 'Conforme au barème'
  AND sexe IN ('Homme','Femme')
  AND montant_brut_sans_ind_sans_alloc IS NOT NULL
GROUP BY annee, mois, date_mois, sexe, grade_principal
ORDER BY annee, mois, grade_principal, sexe;

CREATE OR REPLACE VIEW v_ecart_salarial_hf AS
SELECT
    annee, mois, date_mois, grade_principal,
    MAX(CASE WHEN sexe = 'Homme' THEN revenu_moyen END) AS salaire_moyen_homme,
    MAX(CASE WHEN sexe = 'Femme' THEN revenu_moyen END) AS salaire_moyen_femme,
    MAX(CASE WHEN sexe = 'Homme' THEN effectif END) AS effectif_homme,
    MAX(CASE WHEN sexe = 'Femme' THEN effectif END) AS effectif_femme,
    ROUND(
        (MAX(CASE WHEN sexe = 'Femme' THEN revenu_moyen END)
       - MAX(CASE WHEN sexe = 'Homme' THEN revenu_moyen END))
      / NULLIF(MAX(CASE WHEN sexe = 'Homme' THEN revenu_moyen END), 0) * 100
    , 1) AS ecart_pct_hf
FROM v_indicateurs_sexe_grade
GROUP BY annee, mois, date_mois, grade_principal
ORDER BY annee, mois, grade_principal;

-- ------------------------------------------------------------
-- 5. M2 - salaire brut total par bareme
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_indicateurs_bareme AS
SELECT
    annee, mois, date_mois, bareme,
    COUNT(*) AS effectif,
    ROUND(AVG(montant_brut_total)::numeric, 0) AS revenu_moyen,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY montant_brut_total)::numeric, 0) AS revenu_median,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY montant_brut_total)::numeric, 0) AS revenu_p25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY montant_brut_total)::numeric, 0) AS revenu_p75,
    ROUND(MIN(montant_brut_total)::numeric, 0) AS revenu_min,
    ROUND(MAX(montant_brut_total)::numeric, 0) AS revenu_max
FROM mv_panel_base
WHERE statut_total_bareme = 'Conforme au barème'
  AND montant_brut_total IS NOT NULL
GROUP BY annee, mois, date_mois, bareme
ORDER BY annee, mois, bareme;

CREATE OR REPLACE VIEW v_indicateurs_bareme_grade AS
SELECT
    annee, mois, date_mois, bareme, grade_principal,
    COUNT(*) AS effectif,
    ROUND(AVG(montant_brut_total)::numeric, 0) AS revenu_moyen,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY montant_brut_total)::numeric, 0) AS revenu_median,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY montant_brut_total)::numeric, 0) AS revenu_p25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY montant_brut_total)::numeric, 0) AS revenu_p75,
    ROUND(MIN(montant_brut_total)::numeric, 0) AS revenu_min,
    ROUND(MAX(montant_brut_total)::numeric, 0) AS revenu_max
FROM mv_panel_base
WHERE statut_total_bareme = 'Conforme au barème'
  AND montant_brut_total IS NOT NULL
GROUP BY annee, mois, date_mois, bareme, grade_principal
ORDER BY annee, mois, bareme, grade_principal;

CREATE OR REPLACE VIEW v_indicateurs_bareme_sexe AS
SELECT
    annee, mois, date_mois, bareme, sexe,
    COUNT(*) AS effectif,
    ROUND(AVG(montant_brut_total)::numeric, 0) AS revenu_moyen,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY montant_brut_total)::numeric, 0) AS revenu_median,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY montant_brut_total)::numeric, 0) AS revenu_p25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY montant_brut_total)::numeric, 0) AS revenu_p75,
    ROUND(MIN(montant_brut_total)::numeric, 0) AS revenu_min,
    ROUND(MAX(montant_brut_total)::numeric, 0) AS revenu_max
FROM mv_panel_base
WHERE statut_total_bareme = 'Conforme au barème'
  AND sexe IN ('Homme','Femme')
  AND montant_brut_total IS NOT NULL
GROUP BY annee, mois, date_mois, bareme, sexe
ORDER BY annee, mois, bareme, sexe;

-- ------------------------------------------------------------
-- 6. M1 - multi-postes
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_multipostes_grade AS
SELECT
    annee, mois, date_mois, grade_principal, nb_postes,
    COUNT(*) AS effectif,
    ROUND(AVG(montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_moyen,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_median,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_p25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY montant_brut_sans_ind_sans_alloc)::numeric, 0) AS revenu_p75
FROM mv_panel_base
WHERE statut_bareme = 'Conforme au barème'
  AND montant_brut_sans_ind_sans_alloc IS NOT NULL
GROUP BY annee, mois, date_mois, grade_principal, nb_postes
ORDER BY annee, mois, grade_principal, nb_postes;

CREATE OR REPLACE VIEW v_taux_multipostes AS
SELECT
    annee, mois, date_mois, grade_principal,
    COUNT(*) AS total_agents,
    SUM(CASE WHEN nb_postes > 1 THEN 1 ELSE 0 END) AS agents_multipostes,
    ROUND(
        SUM(CASE WHEN nb_postes > 1 THEN 1 ELSE 0 END) * 100.0
      / NULLIF(COUNT(*), 0)
    , 1) AS taux_multipostes_pct
FROM mv_panel_base
WHERE statut_bareme = 'Conforme au barème'
GROUP BY annee, mois, date_mois, grade_principal
ORDER BY annee, mois, grade_principal;

-- ------------------------------------------------------------
-- 7. Indicateurs annuels M2
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_indicateurs_annuels AS
SELECT
    annee, grade_principal, bareme, sexe,
    COUNT(DISTINCT matricule) AS nb_agents_distincts,
    COUNT(*) AS nb_observations,
    ROUND(AVG(montant_brut_total)::numeric, 0) AS revenu_moyen,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY montant_brut_total)::numeric, 0) AS revenu_median,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY montant_brut_total)::numeric, 0) AS revenu_p25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY montant_brut_total)::numeric, 0) AS revenu_p75,
    ROUND(SUM(montant_brut_total)::numeric, 0) AS masse_salariale_brute
FROM mv_panel_base
WHERE statut_total_bareme = 'Conforme au barème'
  AND montant_brut_total IS NOT NULL
GROUP BY annee, grade_principal, bareme, sexe
ORDER BY annee, grade_principal, bareme, sexe;
