# PANEL ADMIN — Pipeline de Consolidation 2015-2025

Ce projet automatise le traitement, la normalisation et l'analyse des données de paie administratives sur 10 ans. Il produit des rapports de conformité au barème et alimente un dashboard Superset.

## 🚀 Structure Industrialisée

Le projet a été consolidé pour offrir une structure robuste et une logique métier précise (exclusion de l'allocation familiale pour l'audit).

- **`orchestrateur.py`** : Point d'entrée unique pour piloter les 14 étapes.
- **`src/pipeline/`** : Cœur du projet contenant les scripts industrialisés (00 → 09).
- **`src/pipeline/tests/`** : Tests unitaires pour garantir la justesse des calculs.
- **`src/pipeline/control_verif/`** : Outils de diagnostic de qualité des données.
- **`.env`** : Configuration des accès MinIO et PostgreSQL (non commité).

## 🛠️ Utilisation Rapide

1. **Installer les dépendances :**
   ```bash
   pip install -r src/pipeline/requirements.txt
   ```

2. **Lancer le pipeline complet :**
   ```bash
   python orchestrateur.py
   ```

3. **Lancer des étapes spécifiques :**
   ```bash
   python orchestrateur.py --etapes 06 07 08
   ```

## 📊 Étapes du Pipeline

0.  **Upload Staging** : Ingestion des sources Excel vers MinIO.
1.  **Pré-analyse** : Analyse de la structure des fichiers sources.
2.  **Staging → Bronze** : Conversion en Parquet brut.
3.  **Bronze → Silver** : Normalisation et mapping ANSTAT.
4.  **Validation Silver** : Contrôles qualité automatiques.
5.  **Silver → Gold** : Agrégations par organisme.
6.  **Compilation Panel** : Construction du panel individus (Logique No AF).
7.  **Calcul Statuts** : Audit de conformité au barème 2025.
8.  **Rapport Excel** : Génération du rapport interactif final avec sommaire.
9.  **Chargement PG** : Mise à jour de la base de données PostgreSQL.

## 🧪 Tests et Qualité

Pour lancer les tests unitaires :
```bash
pytest src/pipeline/tests/
```

---
*Projet maintenu par la Cellule d'Analyses Economiques de l'ANSTAT.*
