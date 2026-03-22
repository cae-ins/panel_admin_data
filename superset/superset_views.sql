-- ============================================================
-- superset_views.sql
-- Panel Admin — Salaires Fonction Publique Ivoirienne
-- Vues PostgreSQL pour dashboard Superset (2015-2025)
--
-- Dépendances : table `panel` dans datalab_db
-- Exécution   : psql -U datalab -d datalab_db -f superset_views.sql
-- Refresh     : voir script refresh_superset.sh
-- ============================================================

-- ------------------------------------------------------------
-- NETTOYAGE (ordre inverse des dépendances)
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
    v_indicateurs_global,
    v_panel_base
CASCADE;

DROP MATERIALIZED VIEW IF EXISTS mv_panel_base CASCADE;

-- ------------------------------------------------------------
-- 0. VUE DE BASE
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_panel_base AS
SELECT
    matricule,
    CASE
        WHEN UPPER(sexe) LIKE 'MASC%' THEN 'Homme'
        WHEN UPPER(sexe) LIKE 'FEM%'  THEN 'Femme'
        ELSE 'Autre'
    END AS sexe,
    "GRADE_1" AS grade_principal,
    CASE
        WHEN "GRADE_1" IN ('A3','A4','A5','A6','A7') THEN 'Catégorie A'
        WHEN "GRADE_1" IN ('B1','B3')                THEN 'Catégorie B'
        WHEN "GRADE_1" IN ('C1','C2','C3')           THEN 'Catégorie C'
        WHEN "GRADE_1" IN ('D1','D2')                THEN 'Catégorie D'
        WHEN "GRADE_1" = 'NF'                        THEN 'Non Fonctionnaire'
        ELSE 'Autre'
    END AS categorie_grade,
    CASE
        WHEN TRIM(COALESCE("Code_CITP", '')) ~ '^[0-9]'
        THEN LEFT(TRIM("Code_CITP"), 1)::integer
        ELSE NULL
    END AS citp_grand_groupe,
    NULLIF(TRIM(COALESCE("Code_CITP", '')), '')   AS code_citp,
    NULLIF(TRIM(COALESCE("Metier_CITP", '')), '') AS metier_citp,
    annee,
    mois,
    TO_DATE(annee::text || '-' || LPAD(mois::text,2,'0') || '-01', 'YYYY-MM-DD') AS date_mois,
    montant_brut, montant_net, retenue_pension, impot, charge_patronale,
    organisme, emploi,
    CASE
        WHEN UPPER(organisme) LIKE '%AFFAIRES ETRANG%'
             OR UPPER(emploi) LIKE '%AMBASSAD%'
             OR UPPER(emploi) LIKE '%CONSUL%'                      THEN 'CORPS DIPL'
        WHEN emploi = 'Corps Préfectoral'
             OR UPPER(emploi) LIKE '%GARDE DE SOUS-PREF%'
             OR UPPER(organisme) LIKE '%ADMINISTRATION DU TERRIT%' THEN 'CORPS PREF'
        WHEN UPPER(organisme) LIKE '%JUSTICE%'
             OR UPPER(organisme) LIKE '%TRIBUNAL%'
             OR UPPER(organisme) LIKE '%COUR D%APPEL%'
             OR UPPER(emploi)    LIKE '%MAGISTR%'
             OR UPPER(emploi)    LIKE '%JUGE%'
             OR UPPER(emploi)    LIKE '%PROCUREUR%'                THEN 'MAGISTRAT'
        WHEN UPPER(emploi) LIKE '%GREFF%'                          THEN 'GREFFE'
        WHEN UPPER(organisme) LIKE '%ENSEIGNEMENT SUPER%'
             OR UPPER(organisme) LIKE '%UNIVERSITE%'
             OR UPPER(emploi)    LIKE '%MAITRE DE CONF%'
             OR UPPER(emploi)    LIKE '%PROFESSEUR TITULAR%'       THEN 'ENS SUP'
        WHEN UPPER(organisme) LIKE '%ENSEIGNEMENT%'
             OR UPPER(organisme) LIKE '%EDUCATION%'
             OR UPPER(emploi)    LIKE '%INSTITUTEUR%'
             OR UPPER(emploi)    LIKE '%PROFESSEUR%'
             OR UPPER(emploi)    LIKE '%EDUCATEUR%'                THEN 'ENS SEC & PRIM'
        WHEN (UPPER(emploi) LIKE '%MEDECIN%'
             OR UPPER(emploi)  LIKE '%PHARMACIEN%'
             OR UPPER(emploi)  LIKE '%CHIRURGIEN%'
             OR UPPER(emploi)  LIKE '%DENTISTE%')
             AND "GRADE_1" IN ('A4','A5','A6','A7')                THEN 'Cadre-Sup-Santé'
        WHEN UPPER(organisme) LIKE '%SANTE%'
             OR UPPER(emploi)    LIKE '%INFIRMI%'
             OR UPPER(emploi)    LIKE '%SAGE-FEMME%'
             OR UPPER(emploi)    LIKE '%AIDE SOIGNANT%'            THEN 'Tech-Santé'
        WHEN UPPER(organisme) LIKE '%POLICE%'
             OR UPPER(emploi)    LIKE '%POLICIER%'
             OR UPPER(emploi)    LIKE '%COMMISSAIRE%'              THEN 'police'
        WHEN "GRADE_1" = 'NF'                                      THEN 'Non Fonctionnaire'
        ELSE 'Barême Général'
    END AS bareme
FROM panel
WHERE montant_brut > 0
  AND annee BETWEEN 2015 AND 2099
  AND "GRADE_1" IS NOT NULL
  AND "GRADE_1" <> '';

-- ------------------------------------------------------------
-- 0b. VUE MATÉRIALISÉE (performances Superset)
-- ------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_panel_base AS SELECT * FROM v_panel_base;

CREATE INDEX idx_mv_annee_mois  ON mv_panel_base(annee, mois);
CREATE INDEX idx_mv_grade       ON mv_panel_base(grade_principal);
CREATE INDEX idx_mv_bareme      ON mv_panel_base(bareme);
CREATE INDEX idx_mv_date        ON mv_panel_base(date_mois);
CREATE INDEX idx_mv_sexe        ON mv_panel_base(sexe);
CREATE INDEX idx_mv_citp        ON mv_panel_base(citp_grand_groupe);

-- ------------------------------------------------------------
-- 1. INDICATEURS GLOBAUX MENSUELS
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_indicateurs_global AS
SELECT
    annee, mois, date_mois,
    COUNT(*)                                                                      AS effectif,
    ROUND(AVG(montant_brut)::numeric, 0)                                          AS revenu_moyen,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0)  AS revenu_median,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0) AS revenu_p25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0) AS revenu_p75,
    ROUND(MIN(montant_brut)::numeric, 0)                                          AS revenu_min,
    ROUND(MAX(montant_brut)::numeric, 0)                                          AS revenu_max
FROM mv_panel_base
GROUP BY annee, mois, date_mois
ORDER BY annee, mois;

-- ------------------------------------------------------------
-- 2. PAR GRAND GROUPE CITP
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_indicateurs_citp AS
SELECT
    annee, mois, date_mois,
    citp_grand_groupe, code_citp, metier_citp,
    COUNT(*)                                                                      AS effectif,
    ROUND(AVG(montant_brut)::numeric, 0)                                          AS revenu_moyen,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0)  AS revenu_median,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0) AS revenu_p25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0) AS revenu_p75,
    ROUND(MIN(montant_brut)::numeric, 0)                                          AS revenu_min,
    ROUND(MAX(montant_brut)::numeric, 0)                                          AS revenu_max
FROM mv_panel_base
WHERE citp_grand_groupe IS NOT NULL
GROUP BY annee, mois, date_mois, citp_grand_groupe, code_citp, metier_citp
ORDER BY annee, mois, citp_grand_groupe;

CREATE OR REPLACE VIEW v_indicateurs_citp_gg AS
SELECT
    annee, mois, date_mois,
    citp_grand_groupe,
    CASE citp_grand_groupe
        WHEN 1 THEN '1 - Directeurs & cadres'
        WHEN 2 THEN '2 - Professions intellectuelles'
        WHEN 3 THEN '3 - Professions intermédiaires'
        WHEN 4 THEN '4 - Employés administratifs'
        WHEN 5 THEN '5 - Services & ventes'
        WHEN 6 THEN '6 - Agriculture'
        WHEN 7 THEN '7 - Métiers qualifiés'
        WHEN 8 THEN '8 - Opérateurs machines'
        WHEN 9 THEN '9 - Professions élémentaires'
        ELSE 'Non classé'
    END AS libelle_citp,
    COUNT(*)                                                                      AS effectif,
    ROUND(AVG(montant_brut)::numeric, 0)                                          AS revenu_moyen,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0)  AS revenu_median,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0) AS revenu_p25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0) AS revenu_p75
FROM mv_panel_base
WHERE citp_grand_groupe BETWEEN 1 AND 9
GROUP BY annee, mois, date_mois, citp_grand_groupe
ORDER BY annee, mois, citp_grand_groupe;

-- ------------------------------------------------------------
-- 3. PAR GRADE
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_indicateurs_grade AS
SELECT
    annee, mois, date_mois, grade_principal,
    COUNT(*)                                                                      AS effectif,
    ROUND(AVG(montant_brut)::numeric, 0)                                          AS revenu_moyen,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0)  AS revenu_median,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0) AS revenu_p25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0) AS revenu_p75,
    ROUND(MIN(montant_brut)::numeric, 0)                                          AS revenu_min,
    ROUND(MAX(montant_brut)::numeric, 0)                                          AS revenu_max
FROM mv_panel_base
WHERE grade_principal IN ('A3','A4','A5','A6','A7','B1','B3','C1','C2','C3','D1','D2')
GROUP BY annee, mois, date_mois, grade_principal
ORDER BY annee, mois, grade_principal;

-- ------------------------------------------------------------
-- 4. PAR SEXE × GRADE
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_indicateurs_sexe_grade AS
SELECT
    annee, mois, date_mois, sexe, grade_principal,
    COUNT(*)                                                                      AS effectif,
    ROUND(AVG(montant_brut)::numeric, 0)                                          AS revenu_moyen,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0)  AS revenu_median,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0) AS revenu_p25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0) AS revenu_p75,
    ROUND(MIN(montant_brut)::numeric, 0)                                          AS revenu_min,
    ROUND(MAX(montant_brut)::numeric, 0)                                          AS revenu_max
FROM mv_panel_base
WHERE grade_principal IN ('A3','A4','A5','A6','A7','B1','B3','C1','C2','C3','D1','D2')
  AND sexe IN ('Homme','Femme')
GROUP BY annee, mois, date_mois, sexe, grade_principal
ORDER BY annee, mois, grade_principal, sexe;

CREATE OR REPLACE VIEW v_ecart_salarial_hf AS
SELECT
    annee, mois, date_mois, grade_principal,
    MAX(CASE WHEN sexe = 'Homme' THEN revenu_moyen END) AS salaire_moyen_homme,
    MAX(CASE WHEN sexe = 'Femme' THEN revenu_moyen END) AS salaire_moyen_femme,
    MAX(CASE WHEN sexe = 'Homme' THEN effectif END)     AS effectif_homme,
    MAX(CASE WHEN sexe = 'Femme' THEN effectif END)     AS effectif_femme,
    ROUND(
        (MAX(CASE WHEN sexe = 'Femme' THEN revenu_moyen END)
       - MAX(CASE WHEN sexe = 'Homme' THEN revenu_moyen END))
      / NULLIF(MAX(CASE WHEN sexe = 'Homme' THEN revenu_moyen END), 0) * 100
    , 1) AS ecart_pct_hf
FROM v_indicateurs_sexe_grade
GROUP BY annee, mois, date_mois, grade_principal
ORDER BY annee, mois, grade_principal;

-- ------------------------------------------------------------
-- 5. PAR BARÈME
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_indicateurs_bareme AS
SELECT
    annee, mois, date_mois, bareme,
    COUNT(*)                                                                      AS effectif,
    ROUND(AVG(montant_brut)::numeric, 0)                                          AS revenu_moyen,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0)  AS revenu_median,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0) AS revenu_p25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0) AS revenu_p75,
    ROUND(MIN(montant_brut)::numeric, 0)                                          AS revenu_min,
    ROUND(MAX(montant_brut)::numeric, 0)                                          AS revenu_max
FROM mv_panel_base
GROUP BY annee, mois, date_mois, bareme
ORDER BY annee, mois, bareme;

CREATE OR REPLACE VIEW v_indicateurs_bareme_grade AS
SELECT
    annee, mois, date_mois, bareme, grade_principal,
    COUNT(*)                                                                      AS effectif,
    ROUND(AVG(montant_brut)::numeric, 0)                                          AS revenu_moyen,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0)  AS revenu_median,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0) AS revenu_p25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0) AS revenu_p75
FROM mv_panel_base
WHERE grade_principal IN ('A3','A4','A5','A6','A7','B1','B3','C1','C2','C3','D1','D2')
GROUP BY annee, mois, date_mois, bareme, grade_principal
ORDER BY annee, mois, bareme, grade_principal;

CREATE OR REPLACE VIEW v_indicateurs_bareme_sexe AS
SELECT
    annee, mois, date_mois, bareme, sexe,
    COUNT(*)                                                                      AS effectif,
    ROUND(AVG(montant_brut)::numeric, 0)                                          AS revenu_moyen,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0)  AS revenu_median,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0) AS revenu_p25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0) AS revenu_p75
FROM mv_panel_base
WHERE sexe IN ('Homme','Femme')
GROUP BY annee, mois, date_mois, bareme, sexe
ORDER BY annee, mois, bareme, sexe;

-- ------------------------------------------------------------
-- 6. MULTI-POSTES
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_multipostes_grade AS
SELECT
    annee, mois, date_mois, grade_principal, nb_postes,
    COUNT(*) AS effectif
FROM (
    SELECT annee, mois, date_mois, grade_principal, matricule, COUNT(*) AS nb_postes
    FROM mv_panel_base
    GROUP BY annee, mois, date_mois, grade_principal, matricule
) t
GROUP BY annee, mois, date_mois, grade_principal, nb_postes
ORDER BY annee, mois, grade_principal, nb_postes;

CREATE OR REPLACE VIEW v_taux_multipostes AS
SELECT
    annee, mois, date_mois, grade_principal,
    COUNT(DISTINCT matricule)                                   AS total_agents,
    SUM(CASE WHEN nb_postes > 1 THEN 1 ELSE 0 END)             AS agents_multipostes,
    ROUND(
        SUM(CASE WHEN nb_postes > 1 THEN 1 ELSE 0 END) * 100.0
      / NULLIF(COUNT(DISTINCT matricule), 0)
    , 1) AS taux_multipostes_pct
FROM (
    SELECT annee, mois, date_mois, grade_principal, matricule, COUNT(*) AS nb_postes
    FROM mv_panel_base
    GROUP BY annee, mois, date_mois, grade_principal, matricule
) t
WHERE grade_principal IN ('A3','A4','A5','A6','A7','B1','B3','C1','C2','C3','D1','D2')
GROUP BY annee, mois, date_mois, grade_principal
ORDER BY annee, mois, grade_principal;

-- ------------------------------------------------------------
-- 7. INDICATEURS ANNUELS (KPIs longue période)
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_indicateurs_annuels AS
SELECT
    annee, grade_principal, bareme, sexe,
    COUNT(DISTINCT matricule)                                                     AS nb_agents_distincts,
    COUNT(*)                                                                      AS nb_observations,
    ROUND(AVG(montant_brut)::numeric, 0)                                          AS revenu_moyen,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0)  AS revenu_median,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0) AS revenu_p25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY montant_brut)::numeric, 0) AS revenu_p75,
    ROUND(SUM(montant_brut)::numeric, 0)                                          AS masse_salariale_brute
FROM mv_panel_base
GROUP BY annee, grade_principal, bareme, sexe
ORDER BY annee, grade_principal;
