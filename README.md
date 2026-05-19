# PANEL ADMIN - Estimation des salaires moyens dans l'administration publique

Ce projet implémente un pipeline ETL (Extract, Transform, Load) complet pour le traitement des données de paie administratives sur la période 2015-2025. L'objectif principal est de normaliser les données issues de sources disparates pour permettre une présentation fiable des salaires moyens dans le secteur public en Côte d'Ivoire.

## Méthodologie et Définition du Revenu

Le pipeline adopte une approche rigoureuse pour le calcul des indicateurs de revenus, en distinguant deux visions complémentaires :

### Composition du Revenu de Référence (Méthodologie 1)
Pour l'analyse des niveaux de rémunération standard, le système calcule un revenu de référence basé sur les composantes universelles de la paie. Ce revenu inclut :
- **Salaire de Base** (Code 124)
- **Indemnité de Résidence** (Code 228)
- **Indemnité de Logement** (Code 271)
- **Indemnité de Transport** (Code 425)

**Note importante :** L'allocation familiale (Code 421) est systématiquement **exclue** de ce revenu de référence. Cette exclusion permet d'analyser les salaires moyens sans le biais introduit par la situation familiale individuelle des agents.

### Double Vision du Revenu
- **Méthodologie 1 (Revenu de Référence) :** Analyse basée sur le brut hors indemnités spécifiques et hors allocations familiales.
- **Méthodologie 2 (Coût Global) :** Analyse basée sur le montant brut total incluant toutes les composantes de la paie pour mesurer la charge réelle.

## Architecture du Projet

Le projet adopte une architecture "Medallion" pour garantir la traçabilité et la qualité des données :

- **Staging :** Ingestion des fichiers Excel sources vers un stockage objet (MinIO).
- **Bronze :** Conversion des données brutes au format Parquet.
- **Silver :** Normalisation des situations administratives, mapping des colonnes et enrichissement via les référentiels métiers.
- **Gold :** Agrégation des données par individu et par période, calcul des indicateurs statistiques et production des rapports de synthèse.

## Structure du Code

- **orchestrateur.py** : Point d'entrée unique permettant de piloter l'ensemble des étapes du pipeline.
- **src/pipeline/** : Contient les scripts Python de traitement (00 à 09).
- **src/pipeline/tests/** : Suite de tests unitaires pour la validation des calculs d'imputation et de normalisation.
- **src/pipeline/control_verif/** : Scripts de diagnostic pour le suivi de la qualité des données à chaque étape.
- **src/research/** : Archives des travaux exploratoires et notebooks de développement.
- **docs/** : Documentation technique et manuels méthodologiques.

## Utilisation

### Prérequis
- Python 3.12+
- Accès à une instance MinIO (S3)
- Base de données pour le stockage final

### Installation
```bash
pip install -r src/pipeline/requirements.txt
```

### Exécution du Pipeline
Le pipeline peut être lancé intégralement ou par étapes spécifiques via l'orchestrateur. C'est un processus continu destiné à intégrer les nouvelles périodes de données dès leur disponibilité.

```bash
# Lancement complet
python orchestrateur.py

# Lancement à partir d'une étape précise
python orchestrateur.py --depuis 06

# Lancement d'étapes ciblées
python orchestrateur.py --etapes 06 07 08
```

## Étapes du Pipeline

0. **Upload Staging** : Chargement des sources Excel locales vers MinIO.
1. **Pré-analyse** : Analyse structurelle et détection des variations de colonnes.
2. **Staging -> Bronze** : Traduction technique en formats tabulaires performants.
3. **Bronze -> Silver** : Normalisation des grades, corps et situations.
4. **Validation Silver** : Contrôles de complétude et d'intégrité.
5. **Silver -> Gold** : Calcul de la masse salariale agrégée par organisme.
6. **Compilation Panel** : Construction de l'historique individuel (logique sans allocation familiale).
7. **Calcul Indicateurs** : Analyse des niveaux de revenus moyens par secteur et grade.
8. **Rapport Sommaire** : Génération d'un fichier Excel interactif avec navigation structurée.
9. **Chargement Final** : Synchronisation du panel vers le système de stockage cible.

---
*Projet maintenu par la Cellule d'Analyses Économiques.*
