# PANEL ADMIN - Consolidation et Analyse de la Masse Salariale (2015-2025)

Ce projet implémente un pipeline ETL (Extract, Transform, Load) complet pour le traitement et l'audit des données de paie administratives sur la période 2015-2025. L'objectif principal est de normaliser les données issues de sources disparates pour produire des indicateurs de conformité salariale.

## Méthodologie et Audit de Conformité

Le pipeline intègre une logique d'audit avancée permettant de vérifier la conformité des salaires par rapport aux barèmes officiels de l'ANSTAT.

### Règle "No Allocation Familiale"
Pour l'audit de conformité (Méthodologie 1), le système exclut systématiquement le code poste 421 (Allocation Familiale). Cette approche permet de comparer le salaire de base et les indemnités universelles (Résidence, Logement, Transport) à la grille indiciaire sans le biais introduit par la situation familiale des agents.

### Double Vision du Revenu
- **Méthodologie 1 (Conformité) :** Analyse basée sur le brut hors indemnités spécifiques et hors allocations familiales.
- **Méthodologie 2 (Coût État) :** Analyse basée sur le montant brut total incluant toutes les composantes de la paie.

## Architecture du Projet

Le projet adopte une architecture "Medallion" pour garantir la traçabilité et la qualité des données :

- **Staging :** Ingestion des fichiers Excel sources vers un stockage objet (MinIO).
- **Bronze :** Conversion des données brutes au format Parquet.
- **Silver :** Normalisation des situations administratives, mapping des colonnes et enrichissement via les référentiels ANSTAT.
- **Gold :** Agrégation des données par individu et par période, calcul des indicateurs statistiques et production des rapports finaux.

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
- Base de données PostgreSQL (pour le chargement final)

### Installation
```bash
pip install -r src/pipeline/requirements.txt
```

### Exécution du Pipeline
Le pipeline peut être lancé intégralement ou par étapes spécifiques via l'orchestrateur :

```bash
# Lancement complet
python orchestrateur.py

# Lancement à partir d'une étape précise
python orchestrateur.py --depuis 06

# Lancement d'étapes ciblées
python orchestrateur.py --etapes 06 07 08
```

### Tests et Validation
Il est recommandé de lancer les tests unitaires avant toute mise en production :
```bash
pytest src/pipeline/tests/
```

## Étapes Détaillées du Pipeline

0. **Upload Staging** : Chargement des sources Excel locales vers MinIO.
1. **Pré-analyse** : Analyse structurelle et détection des variations de colonnes.
2. **Staging -> Bronze** : Traduction technique en formats tabulaires performants.
3. **Bronze -> Silver** : Normalisation des grades, corps et situations.
4. **Validation Silver** : Contrôles de complétude et d'intégrité.
5. **Silver -> Gold** : Calcul de la masse salariale agrégée par organisme.
6. **Compilation Panel** : Construction de l'historique individuel (logique No AF).
7. **Calcul Statuts** : Audit de conformité aux bornes min/max du barème.
8. **Rapport Sommaire** : Génération d'un fichier Excel interactif avec navigation structurée.
9. **Chargement Final** : Synchronisation du panel final vers le système de stockage cible.

---
*Projet maintenu par la Cellule d'Analyses Économiques.*
