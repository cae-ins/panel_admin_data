# 📊 Panel Admin — Pipeline de Traitement de la Masse Salariale

Pipeline de consolidation, normalisation et analyse de la masse salariale administrative des fonctionnaires (2015–2025).

---

## Table des matières

- [Vue d'ensemble](#vue-densemble)
- [Architecture](#architecture)
- [Prérequis](#prérequis)
- [Configuration](#configuration)
- [Pipeline — Description des étapes](#pipeline--description-des-étapes)
  - [Étape 00 : Upload Staging](#étape-00--upload-staging)
  - [Étape 01 : Pré-analyse](#étape-01--pré-analyse)
  - [Étape 02 : Staging → Bronze](#étape-02--staging--bronze)
  - [Étape 03 : Bronze → Silver](#étape-03--bronze--silver)
  - [Étape 03b : Imputation des grades](#étape-03b--imputation-des-grades)
  - [Étape 03c : Imputation des salaires](#étape-03c--imputation-des-salaires)
  - [Étape 04 : Validation Silver](#étape-04--validation-silver)
  - [Étape 05 : Silver → Gold](#étape-05--silver--gold)
  - [Étape 06 : Compilation du panel](#étape-06--compilation-du-panel)
  - [Étape 07 : Calcul des indicateurs](#étape-07--calcul-des-indicateurs)
  - [Étape 08 : Fichier Excel avec sommaire](#étape-08--fichier-excel-avec-sommaire)
  - [Étape 09 : Chargement PostgreSQL](#étape-09--chargement-postgresql)
- [Orchestrateur](#orchestrateur)
- [Données de référence](#données-de-référence)
- [Sorties produites](#sorties-produites)
- [Dépannage](#dépannage)

---

## Vue d'ensemble

Ce pipeline transforme des fichiers Excel mensuels de paie en un panel analytique complet. Il suit une architecture **Médaillon** (Staging → Bronze → Silver → Gold) avec des étapes d'enrichissement, d'imputation et de calcul statistique.

```
Fichiers Excel mensuels (paie)
        │
        ▼
┌──────────────────┐
│  00 Upload       │  Fichiers Excel → S3 staging
│     Staging      │
└────────┬─────────┘
         ▼
┌──────────────────┐
│  01 Pré-analyse  │  Analyse structure colonnes, détection variations
└────────┬─────────┘
         ▼
┌──────────────────┐
│  02 Staging →    │  Excel → Parquet brut (une colonne = un code)
│     Bronze       │
└────────┬─────────┘
         ▼
┌──────────────────┐
│  03 Bronze →     │  Mapping colonnes, normalisation, enrichissement ANSTAT
│     Silver       │
├──────────────────┤
│  03b Imputation  │  Grade modal par cascade emploi (P1/P2/P3)
│     grades       │
├──────────────────┤
│  03c Imputation  │  LOCF + médiane cascade pour salaires manquants
│     salaires     │
└────────┬─────────┘
         ▼
┌──────────────────┐
│  04 Validation   │  Contrôles qualité (complétude, doublons, cohérence)
│     Silver       │
└────────┬─────────┘
         ▼
┌──────────────────┐
│  05 Silver →     │  Agrégations masse salariale + effectifs
│     Gold         │
└────────┬─────────┘
         ▼
┌──────────────────┐
│  06 Compilation  │  Panel individu × période (déduplication, tri)
│     panel        │
└────────┬─────────┘
         ▼
┌──────────────────┐
│  07 Indicateurs  │  Stats : grade, sexe, CITP, barème, multi-postes
│     salaires     │
├──────────────────┤
│  07b Analyse     │  Extraction et analyse des agents hors barème
│     hors barème  │
└────────┬─────────┘
         ▼
┌──────────────────┐
│  08 Excel avec   │  Fichier navigable avec sommaire structuré
│     sommaire     │
└────────┬─────────┘
         ▼
┌──────────────────┐
│  09 Chargement   │  Panel Gold → PostgreSQL (alimente Superset)
│     PostgreSQL   │
└──────────────────┘
```

---

## Architecture

### Architecture Médaillon

| Couche      | Bucket S3  | Format  | Description |
|-------------|-----------|---------|-------------|
| **Staging** | `staging` | Excel   | Fichiers bruts tels que reçus + exports finaux |
| **Bronze**  | `bronze`  | Parquet | Données brutes converties, une colonne par code |
| **Silver**  | `silver`  | Parquet | Données normalisées, enrichies, imputées |
| **Gold**    | `gold`    | Parquet | Données agrégées, panel final, indicateurs |

### Structure des fichiers

```
panel_admin_data/
├── .env                          # Variables d'environnement (S3, PostgreSQL)
├── orchestrateur.py              # Lance le pipeline complet ou partiel
├── 00_upload_staging.py          # Upload des fichiers source
├── 01_pre_analyse.py             # Analyse structure des fichiers
├── 02_staging_to_bronze.py       # Excel → Parquet Bronze
├── 03_bronze_to_silver.py        # Normalisation → Silver
├── 03b_imputer_grades_silver.py  # Imputation des grades
├── 03c_imputer_salaires_silver.py# Imputation des salaires
├── 04_validation_silver.py       # Contrôles qualité
├── 05_silver_to_gold.py          # Agrégations → Gold
├── 06_compiler_panel.py          # Panel individu × période
├── 07_calcul_indicateur.py       # Calcul des indicateurs statistiques
├── 07b_analyse_hors_bareme.py    # Analyse des agents hors barème
├── 08_creation_fichier_excel_avec_sommaire.py  # Excel final navigable
├── 09_charger_panel_pg.py        # Chargement PostgreSQL
├── src/                          # Modules utilitaires
│   └── version_python_dp_etoile/ # Version Python du data pipeline étoile
└── README.md                     # Ce fichier
```

---

## Prérequis

### Dépendances Python

```bash
pip install polars fastexcel boto3 python-dotenv openpyxl psycopg2-binary duckdb
```

### Infrastructure

- **MinIO / S3** : stockage objet compatible S3 pour les couches Staging/Bronze/Silver/Gold
- **PostgreSQL** : base cible pour le dashboard Superset (étape 09)
- **Apache Superset** : visualisation des données (optionnel)

---

## Configuration

Créer un fichier `.env` à la racine du projet :

```env
# MinIO / S3
MINIO_ENDPOINT
MINIO_ACCESS_KEY
MINIO_SECRET_KEY

# Buckets
BUCKET_STAGING=staging
BUCKET_BRONZE=bronze
BUCKET_SILVER=silver
BUCKET_GOLD=gold

# PostgreSQL (étape 09)
PG_HOST=localhost
PG_PORT=5432
PG_DB=datalab_db
PG_USER=datalab
PG_PASSWORD=****
```

---

## Pipeline — Description des étapes

### Étape 00 : Upload Staging

**Script** : `00_upload_staging.py`

Charge les fichiers Excel mensuels de paie dans le bucket S3 `staging/panel_admin/`.

- **Entrée** : Fichiers Excel locaux (un par mois, format `MMYYYY.xlsx`)
- **Sortie** : `s3://staging/panel_admin/*.xlsx`

---

### Étape 01 : Pré-analyse

**Script** : `01_pre_analyse.py`

Analyse la structure des fichiers Excel en staging : détecte les colonnes F1/F2 (libellés/valeurs), les codes numériques, et les variations de structure entre les périodes.

- **Entrée** : `s3://staging/panel_admin/*.xlsx`
- **Sortie** : Rapport console (diagnostic des colonnes)

---

### Étape 02 : Staging → Bronze

**Script** : `02_staging_to_bronze.py`

Convertit les fichiers Excel mensuels en fichiers Parquet Bronze. Chaque colonne du fichier source est conservée telle quelle.

- **Entrée** : `s3://staging/panel_admin/*.xlsx`
- **Sortie** : `s3://bronze/panel_admin/YYYY.parquet`

---

### Étape 03 : Bronze → Silver

**Script** : `03_bronze_to_silver.py`

Transformations principales :
- Mapping et renommage des colonnes (codes F1 → noms lisibles)
- Normalisation des situations administratives
- Enrichissement ANSTAT (classification des emplois)
- Exclusion de l'organisme 59
- Suivi du taux de matching par période

- **Entrée** : `s3://bronze/panel_admin/YYYY.parquet`
- **Sortie** : `s3://silver/panel_admin/YYYY.parquet`

---

### Étape 03b : Imputation des grades

**Script** : `03b_imputer_grades_silver.py`

Impute les grades manquants par cascade basée sur l'emploi :
1. **P1** : Grade modal par emploi × mois (même période)
2. **P2** : Grade modal par emploi × année
3. **P3** : Grade modal par emploi (toutes périodes confondues)

Colonnes ajoutées :
- `GRADE` : grade final (observé ou imputé)
- `GRADE_SOURCE` : `OBSERVE` | `IMPUTE` | `NF_NON_IMPUTABLE`

- **Entrée/Sortie** : `s3://silver/panel_admin/YYYY.parquet` (mis à jour en place)

---

### Étape 03c : Imputation des salaires

**Script** : `03c_imputer_salaires_silver.py`

Impute les salaires manquants par cascade :
1. **LOCF** : Dernier salaire connu de l'individu (forward fill + backward fill)
2. **Médiane L1** : `CODE_EMPLOI × GRADE × CODE_ORGANISME × mois_annee`
3. **Médiane L2** : `CODE_EMPLOI × GRADE × mois_annee`
4. **Médiane L3** : `GRADE × CODE_ORGANISME × mois_annee`
5. **Médiane L4** : `GRADE × mois_annee`

Colonnes ajoutées :
- `montant_brut_SOURCE` : `OBSERVE` | `LOCF_FWD` | `LOCF_BWD` | `IMPUTE_L1..L4` | `NON_IMPUTABLE`
- `montant_net_SOURCE` : idem

Architecture mémoire optimisée (streaming fichier par fichier, ~80% de réduction du pic mémoire).

- **Entrée/Sortie** : `s3://silver/panel_admin/YYYY.parquet` (mis à jour en place)

---

### Étape 04 : Validation Silver

**Script** : `04_validation_silver.py` *(optionnelle)*

Contrôles qualité sur les tables Silver :
1. Complétude des colonnes clés
2. Variations suspectes de NA entre années
3. Doublons `matricule × mois_annee`
4. Cohérence des montants (net > brut)
5. Distribution des situations administratives

- **Entrée** : `s3://silver/panel_admin/YYYY.parquet`
- **Sortie** : Rapport console

---

### Étape 05 : Silver → Gold

**Script** : `05_silver_to_gold.py`

Produit les tables Gold agrégées :

| Table Gold | Contenu |
|-----------|---------|
| `masse_salariale.parquet` | Masse salariale brute et nette par période × organisme |
| `effectifs.parquet` | Effectifs et agents uniques par période |

Exports CSV également produits dans `staging/panel_admin/exports_gold/`.

- **Entrée** : `s3://silver/panel_admin/YYYY.parquet`
- **Sortie** : `s3://gold/panel_admin/*.parquet` + `s3://staging/panel_admin/exports_gold/*.csv`

---

### Étape 06 : Compilation du panel

**Script** : `06_compiler_panel.py`

Construit le panel final : **une ligne = un individu × une période**. Inclut :

| Catégorie | Colonnes |
|-----------|----------|
| Identité | `cle_unique`, `matricule`, `nom`, `sexe`, `date_naissance` |
| Position admin. | `situation_normalisee`, `CODE_ORGANISME`, `organisme`, `CODE_EMPLOI`, `emploi`, `CODE_GRADE`, etc. |
| Grade normalisé | `GRADE`, `GRADE_SOURCE` |
| Temporel | `mois_annee`, `annee`, `mois` |
| Salaires | `montant_brut`, `montant_net`, `retenue_pension`, `impot`, `charge_patronale` |
| Traçabilité | `montant_brut_SOURCE`, `montant_net_SOURCE` |
| Multi-postes | `NB_POSTES`, `montant_brut_bareme`, `montant_brut_total` |

- **Entrée** : `s3://silver/panel_admin/YYYY.parquet`
- **Sortie** : `s3://gold/panel_admin/panel_YYYY.parquet` + `panel_complet.parquet`

---

### Étape 07 : Calcul des indicateurs

**Script** : `07_calcul_indicateur.py`

C'est le **moteur statistique** du pipeline. Il produit l'ensemble des indicateurs salariaux à partir du panel Gold.

#### Paramètres configurables

| Paramètre | Défaut | Description |
|-----------|--------|-------------|
| `MODE_SALAIRE_BRUT` | `True` | `True` = agrégation par `cle_unique` (salaire brut) / `False` = par `matricule` (revenu salarial) |
| `PERIODE_RECENTE` | `True` | `True` = 2024–2025 / `False` = 2015 → dernière année |
| `INCLURE_ZEROS` | `True` | `True` = inclure les salaires nuls / `False` = salaire > 0 uniquement |
| `SEUIL_WINS` | `0.01` | Seuil de winsorisation (P1/P99). `None` pour désactiver |

#### Processus détaillé

1. **Chargement** — Lit les fichiers `panel_YYYY.parquet` depuis Gold, exclut l'organisme 59
2. **Vérification** — Contrôle la présence de `GRADE` et `GRADE_SOURCE`
3. **Extraction dates** — Parse `mois_annee` (MMYYYY) → `ANNEE`, `MOIS_NUM`
4. **Salaire brut** — Utilise `montant_brut_bareme` (postes 124/228/271/421/425)
5. **Standardisation sexe** — Normalise en `Homme` / `Femme`
6. **Multi-postes** — Détecte les agents ayant plusieurs postes par période
7. **Filtrage grades** — Retient uniquement les grades valides (A1–A7, B1–B6, C1–C5, D1–D3)
8. **Fuzzy join barème** — Joint avec `CORRESPONDANCE_GRILLE_EMPLOI.xlsx` pour associer chaque agent à un secteur (Éducation, Santé, Police, Barème Général…)
9. **Statut barème** — Compare chaque salaire aux bornes min/max du référentiel ANSTAT 2025 par cluster (secteur × grade) → `Conforme` / `Inférieur` / `Supérieur`. **Seuls les conformes sont retenus pour les indicateurs**
10. **Agrégation** — Agrège par `cle_unique` ou `matricule` selon le mode
11. **Winsorisation** — Écrête les valeurs extrêmes de `montant_brut_total` (P1/P99)
12. **Calcul indicateurs** — Effectif, moyenne, médiane, min, max, P25, P75
13. **MODELE_ANSTAT** — Synthèse min/moy/max par secteur pour 2024 et 2025
14. **Export Excel** — Fichier multi-feuilles vers S3 staging
15. **Export barème** — Second Excel + Parquet `base_avec_statut.parquet`

#### Feuilles Excel produites

| Feuille | Contenu |
|---------|---------|
| `REVENU_Grade_Detail` | Stats par année × mois × grade |
| `REVENU_Grade_Sexe` | Stats par grade × sexe |
| `REVENU_CITP_Detail` | Stats par code CITP |
| `REVENU_Bareme_Detail` | Stats par barème (secteur) |
| `REVENU_Bareme_Grade` | Stats par barème × grade |
| `REVENU_Bareme_Sexe` | Stats par barème × sexe |
| `montant_brut_bareme_Moyen` | Pivot année × mois × grade (moyenne) |
| `montant_brut_bareme_Median` | Pivot année × mois × grade (médiane) |
| `MULTI_Par_Grade_NbPostes` | Effectifs par grade × nb de postes |
| `MULTI_Distribution` | Distribution mono/multi-postes |
| `MODELE_ANSTAT_2024` | Synthèse formatée par secteur — 2024 |
| `MODELE_ANSTAT_2025` | Synthèse formatée par secteur — 2025 |

#### Fichiers de sortie

| Fichier | Emplacement |
|---------|-------------|
| Indicateurs principaux | `s3://staging/panel_admin/exports_gold/indicateurs_salaire_brut_2024_2025_avec_zeros.xlsx` |
| Indicateurs barème | `s3://staging/panel_admin/exports_gold/indicateurs_bareme_sans_indemnites.xlsx` |
| Base avec statut | `s3://gold/panel_admin/base_avec_statut.parquet` |

- **Entrée** : `s3://gold/panel_admin/panel_YYYY.parquet`
- **Sortie** : voir tableau ci-dessus

---

### Étape 07b : Analyse hors barème

**Script** : `07b_analyse_hors_bareme.py` *(optionnel, à lancer manuellement)*

Extrait et analyse les agents dont le salaire se situe **en dehors** des bornes min/max du référentiel ANSTAT 2025. S'exécute après l'étape 07.

#### Statuts analysés

- **« Inférieur au minimum du barème »** — salaire en dessous du minimum référentiel
- **« Supérieur au maximum du barème »** — salaire au-dessus du maximum référentiel

#### Feuilles Excel produites

| Feuille | Contenu |
|---------|---------|
| `RESUME_Statut` | Effectif, moyenne, médiane, P25/P75, écart-type par statut |
| `HORS_Statut_Grade` | Ventilation par statut × grade |
| `HORS_Statut_Bareme` | Ventilation par statut × barème (secteur) |
| `HORS_Statut_Sexe` | Ventilation par statut × sexe |
| `HORS_Statut_Annee` | Ventilation par statut × année |
| `HORS_Bareme_Grade` | Croisement complet statut × barème × grade |
| `ECART_Resume` | Écart moyen/médian au barème (FCFA et %) |
| `DETAIL_Agents` | Liste détaillée des agents (jusqu'à 10 000 lignes, triés par écart) |

#### Fichiers de sortie

| Fichier | Emplacement |
|---------|-------------|
| Excel analyse | `s3://staging/panel_admin/exports_gold/analyse_hors_bareme.xlsx` |
| Parquet complet | `s3://gold/panel_admin/agents_hors_bareme.parquet` |

- **Entrée** : `s3://gold/panel_admin/base_avec_statut.parquet` (produit par l'étape 07)
- **Sortie** : voir tableau ci-dessus

---

### Étape 08 : Fichier Excel avec sommaire

**Script** : `08_creation_fichier_excel_avec_sommaire.py`

Produit un fichier Excel navigable avec un sommaire structuré à liens cliquables :

| Section | Contenu |
|---------|---------|
| Section 1 | Classification par grand groupe CITP (9 groupes) |
| Section 2 | Classification par grade (A1–D3) |
| Section 3 | Classification par sexe (Homme / Femme) |
| Section 4 | Multi-postes par grade |
| Section 5 | Barème / Grille salariale |
| Annexe | Modèle ANSTAT 2024 / 2025 |

> ⚠️ Les 3 paramètres `MODE_SALAIRE_BRUT`, `PERIODE_RECENTE` et `INCLURE_ZEROS` doivent être identiques à ceux de l'étape 07.

- **Entrée** : `s3://staging/panel_admin/exports_gold/indicateurs_*.xlsx`
- **Sortie** : `s3://staging/panel_admin/exports_gold/indicateurs_*_SOMMAIRE.xlsx`

---

### Étape 09 : Chargement PostgreSQL

**Script** : `09_charger_panel_pg.py`

Charge le panel Gold dans PostgreSQL pour alimenter le dashboard Superset :
1. Télécharge `panel_complet.parquet` depuis MinIO
2. Lit avec Polars
3. Recrée la table `panel` dans PostgreSQL (DROP + COPY)
4. Vérifie le nombre de lignes chargées

- **Entrée** : `s3://gold/panel_admin/panel_complet.parquet`
- **Sortie** : Table `panel` dans PostgreSQL (`datalab_db`)

---

## Orchestrateur

Le fichier `orchestrateur.py` permet de lancer le pipeline de bout en bout ou partiellement.

### Utilisation

```bash
# Pipeline complet (étapes 01 à 09)
python orchestrateur.py

# Reprendre à partir d'une étape
python orchestrateur.py --depuis 03

# Étapes spécifiques uniquement
python orchestrateur.py --etapes 06 07 08

# Afficher le plan sans exécuter
python orchestrateur.py --dry-run

# Combiner les options
python orchestrateur.py --depuis 07 --dry-run
```

### Comportement en cas d'erreur

- **Étape obligatoire** en échec → le pipeline **s'arrête**
- **Étape optionnelle** en échec (ex : 04 Validation) → le pipeline **continue**
- Un résumé final indique les étapes réussies / échouées avec les durées
- Pour reprendre : `python orchestrateur.py --depuis <ID_ECHEC>`

---

## Données de référence

Les fichiers de référence sont stockés dans `s3://staging/panel_admin/references/` :

| Fichier | Usage |
|---------|-------|
| `CORRESPONDANCE_GRILLE_EMPLOI.xlsx` | Table de correspondance emploi → barème/grille salariale + grade de référence. Utilisée pour le fuzzy join barème à l'étape 07 |
| `anstat_2025.txt.xlsx` | Barème ANSTAT 2025 : colonnes salaire de base, résidence, logement, transport, allocation familiale. Sert de référentiel min/max pour le calcul du statut barème |

---

## Sorties produites

### Fichiers Gold (Parquet)

| Fichier | Étape | Description |
|---------|-------|-------------|
| `panel_YYYY.parquet` | 06 | Panel par année |
| `panel_complet.parquet` | 06 | Panel toutes années |
| `masse_salariale.parquet` | 05 | Masses salariales agrégées |
| `effectifs.parquet` | 05 | Effectifs agrégés |
| `base_avec_statut.parquet` | 07 | Panel avec statut barème (toutes catégories) |
| `agents_hors_bareme.parquet` | 07b | Agents hors barème avec écarts calculés |

### Fichiers Excel (Staging)

| Fichier | Étape | Description |
|---------|-------|-------------|
| `indicateurs_salaire_brut_*.xlsx` | 07 | Indicateurs statistiques multi-feuilles |
| `indicateurs_bareme_sans_indemnites.xlsx` | 07 | Indicateurs barème + référentiel |
| `analyse_hors_bareme.xlsx` | 07b | Analyse détaillée des agents hors barème |
| `indicateurs_*_SOMMAIRE.xlsx` | 08 | Fichier Excel navigable avec sommaire |

### Base de données

| Table | Étape | Description |
|-------|-------|-------------|
| `panel` (PostgreSQL) | 09 | Panel complet, alimente Superset |

---

## Dépannage

### Erreurs courantes

| Erreur | Cause | Solution |
|--------|-------|----------|
| `GRADE et/ou GRADE_SOURCE absentes du Gold` | Étape 03b non exécutée avant 07 | Relancer `python orchestrateur.py --depuis 03b` |
| `montant_brut_bareme absent du Gold` | Étape 06 non exécutée/obsolète | Relancer `python orchestrateur.py --depuis 06` |
| `base_avec_statut.parquet introuvable` | Étape 07 non exécutée avant 07b | Exécuter d'abord `python 07_calcul_indicateur.py` |
| `Table barème non disponible` | Fichier `CORRESPONDANCE_GRILLE_EMPLOI.xlsx` absent | Charger le fichier dans `staging/panel_admin/references/` |
| `Fichier anstat_2025.txt.xlsx introuvable` | Référentiel ANSTAT absent | Charger le fichier dans `staging/panel_admin/references/` |
| Matching barème < 20% | Correspondance emplois faible | Vérifier/compléter `CORRESPONDANCE_GRILLE_EMPLOI.xlsx` |

### Relancer une partie du pipeline

```bash
# Après modification du mapping Bronze → Silver
python orchestrateur.py --depuis 03

# Après ajout d'un fichier de référence
python orchestrateur.py --depuis 07

# Régénérer uniquement les exports Excel
python orchestrateur.py --etapes 07 08

# Analyser les agents hors barème (script autonome)
python 07b_analyse_hors_bareme.py
```

---

> **Dernière mise à jour** : Avril 2025
