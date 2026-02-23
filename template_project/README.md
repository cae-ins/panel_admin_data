# Proof of Concept — Panel Administratif 2015-2025

## Objectif

Ce dossier est le **projet exemple de référence** de la Data Platform ANSTAT.
Il reproduit intégralement le projet `panel_admin_data` (31 871 913 lignes × 425 colonnes,
~120 fichiers Excel mensuels 2015-2025) en utilisant les templates et l'infrastructure
de la Data Platform.

Il montre concrètement comment **n'importe quel projet ANSTAT** s'intègre dans la plateforme :
stockage sur MinIO, versionnement Iceberg/Nessie, traitement en R/sparklyr, production via Spark Operator.

---

## Correspondance avec les scripts originaux

| Script PoC (Data Platform) | Script original (`panel_admin_data/Script_final_R/`) |
|----------------------------|------------------------------------------------------|
| `00_upload_sources_minio.R` | *(nouveau — dépôt sur staging)* |
| `01_pre_analyse.R` | `01_pre_analyse_colonnes_2015_2025.R` |
| `02_staging_to_bronze.R` | `02_consolidation_complete_2015_2025.R` (partie ingestion) |
| `03_bronze_to_silver.R` | `02_consolidation_complete_2015_2025.R` (partie enrichissement) |
| `04_validation_silver.R` | `03_post_validation_base_finale.R` |
| `05_silver_to_gold.R` | *(nouveau — agrégations Gold)* |

---

## Ce que démontre ce PoC

1. **Stockage centralisé** : les ~120 fichiers Excel mensuels ne sont plus sur des disques locaux mais dans `staging/panel_admin/` sur MinIO, accessibles par toute l'équipe.

2. **Logique métier préservée** : toutes les fonctions R originales (`mapper_colonnes()`, `normaliser_pour_matching()`, `enrichir_par_codes()`, etc.) sont réutilisées à l'identique.

3. **Versionnement des données** : chaque transformation Bronze → Silver → Gold est versionnée par Nessie. On peut revenir à n'importe quelle version passée.

4. **Séparation des couches** :
   - **Bronze** : données brutes telles qu'extraites des Excel, types String, toutes périodes combinées
   - **Silver** : colonnes standardisées (`mapper_colonnes`), montants en numérique, situations filtrées, enrichi avec les codes ANSTAT
   - **Gold** : agrégations prêtes pour tableaux de bord (masse salariale par période/organisme, effectifs)

5. **Reproductibilité** : n'importe quel membre de l'équipe peut reproduire la base consolidée en exécutant les scripts dans l'ordre, depuis n'importe quelle machine ayant accès à la plateforme.

---

## Ordre d'exécution

```
00_upload_sources_minio.R    ← une seule fois (dépôt des sources)
        │
        ▼
01_pre_analyse.R             ← optionnel (analyse exploratoire des colonnes)
        │
        ▼
02_staging_to_bronze.R       ← ingestion Excel → Bronze Iceberg
        │
        ▼
03_bronze_to_silver.R        ← transformation → Silver Iceberg
        │
        ▼
04_validation_silver.R       ← contrôle qualité Silver
        │
        ▼
05_silver_to_gold.R          ← agrégations → Gold Iceberg
```

---

## Données sources attendues sur MinIO

```
staging/
  panel_admin/
    fichiers_mensuels/
      2015_01.xlsx
      2015_02.xlsx
      ...
      2025_12.xlsx
    references/
      FICHIER_ANSTAT_CODE_2025.xlsx   ← tables de codes (organismes, grades, emplois...)
```

---

## Tables produites

| Table | Couche | Description |
|-------|--------|-------------|
| `nessie.bronze.panel_admin_solde_mensuel` | Bronze | Toutes périodes, colonnes brutes, tout String |
| `nessie.silver.panel_admin_solde_mensuel` | Silver | Colonnes standards, types corrects, codes enrichis |
| `nessie.gold.panel_admin_masse_salariale` | Gold | Masse salariale par période × organisme |
| `nessie.gold.panel_admin_effectifs` | Gold | Effectifs et distribution par période |
