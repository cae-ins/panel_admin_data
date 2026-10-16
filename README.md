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

## Organisation du dépôt

| Emplacement | Contenu |
|-------------|---------|
| [orchestrateur.py](orchestrateur.py) | Point d'entrée du pipeline et reprise des exécutions |
| [src/pipeline/](src/pipeline/) | Étapes de traitement, modules communs et configuration |
| [src/pipeline/control_verif/](src/pipeline/control_verif/) | Diagnostics des données |
| [src/utils/](src/utils/) | Pseudonymisation, lecture des schémas Excel, création du `.env` |
| [tests/](tests/) et [src/pipeline/tests/](src/pipeline/tests/) | Tests des utilitaires et des fonctions de calcul |
| [superset/](superset/) | Vues PostgreSQL et guide de validation pour Superset |
| [docs/](docs/README.md) | Documentation officielle, guides, communication et archives |
| [src/research/](src/research/) | Notebooks et analyses ponctuelles, hors orchestration |
| [src/R/](src/R/) et [consolidation_solde_2015_2025/](consolidation_solde_2015_2025/) | Travaux R historiques, conservés pour référence |
| [infra/](infra/) et [Dockerfile](Dockerfile) | Configuration de déploiement |

Les analyses ponctuelles dans `src/research/analyses_ponctuelles/` utilisent des
chemins de données locaux à adapter avant exécution. Elles ne font pas partie du
pipeline de production.

Les fichiers `.env`, caches, journaux techniques et fichiers intermédiaires LaTeX
restent locaux. Les rapports Markdown dans `reports/` conservent la trace des
exécutions ; leur publication reste volontaire.

## Utilisation

### Prérequis
- Python 3.12+
- Accès à une instance MinIO (S3)
- Base de données pour le stockage final

### Installation
```bash
pip install -r requirements.txt
```

### Configuration des identifiants (MinIO, PostgreSQL)

Le code ne contient aucun identifiant. Les scripts lisent des variables
d'environnement (`MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`,
`PG_PASSWORD`, etc. ; liste complète dans [.env.example](.env.example)). Chaque
machine (serveur JupyterLab, poste local) fournit ses propres valeurs sous les
mêmes noms. Les scripts ne fournissent pas de valeurs de connexion par défaut.

Deux façons de fournir ces variables :

**Option A : un fichier `.env` sur la machine.** Depuis un terminal JupyterLab
(*File → New → Terminal*), à la racine du dépôt :

```bash
python src/utils/creer_env.py
```

Le script reprend les variables de `.env.example` une par une :

- les variables ordinaires proposent une valeur par défaut entre crochets
  (Entrée pour la garder) ;
- les secrets (`MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `PG_PASSWORD`,
  `ANSTAT_SECRET_KEY`) sont saisis sans affichage, avec confirmation. On peut
  les coller.

Protections :

- aucune commande shell n'est lancée et les secrets ne passent pas en argument :
  ils n'apparaissent pas dans l'historique du shell ;
- une valeur contenant un retour à la ligne est refusée, ce qui empêche d'ajouter
  d'autres variables dans le `.env` ;
- chaque valeur, puis le fichier complet, est relu par python-dotenv avant
  écriture. Si la relecture diffère de la saisie, rien n'est écrit ;
- le `.env` est créé avec les droits `600` (lisible par son seul propriétaire),
  d'un seul coup. Ctrl+C pendant la saisie n'écrit rien ;
- un `.env` existant n'est écrasé qu'après confirmation ;
- l'écran et l'historique de défilement du terminal sont effacés à la fin.
  Fermer ensuite l'onglet du terminal.

Deux types de valeurs sont refusés, et il faut alors changer le mot de passe
côté MinIO ou PostgreSQL :

- une valeur contenant `${` : `load_dotenv` la remplacerait par une autre
  variable ;
- une valeur finissant par `\` : python-dotenv la lit mal quand d'autres lignes
  suivent.

Les étapes `00` à `07a` chargent `.env` avec un chemin relatif
(`load_dotenv(".env")`) : lancer le pipeline **depuis la racine du dépôt**.

**Option B : un Secret Kubernetes injecté dans le pod Jupyter.** Aucun fichier
secret n'est alors présent sur le disque. Le modèle
[infra/panel-admin-secret.yaml](infra/panel-admin-secret.yaml) (valeurs
`change-me` à remplacer hors du dépôt) est déclaré dans le namespace `airflow`.
Un Secret n'est visible que dans son namespace : il faut en créer une copie dans
le namespace de JupyterHub, puis l'injecter dans les pods utilisateurs
(`values.yaml` de JupyterHub) :

```yaml
singleuser:
  extraEnv:
    MINIO_ACCESS_KEY:
      valueFrom: {secretKeyRef: {name: panel-admin-secrets, key: MINIO_ACCESS_KEY}}
    MINIO_SECRET_KEY:
      valueFrom: {secretKeyRef: {name: panel-admin-secrets, key: MINIO_SECRET_KEY}}
    # idem pour MINIO_ENDPOINT, PG_*, etc.
```

`load_dotenv` n'écrase pas une variable déjà définie : les deux options peuvent
coexister, et les variables déjà présentes dans l'environnement l'emportent sur
le `.env`.

Bonnes pratiques :

- ne jamais versionner un `.env` ni un Secret rempli (`.env` et `**/.env` sont
  dans `.gitignore`) ;
- une clé d'accès MinIO par personne ou par usage (*service account* limité aux
  buckets `staging`, `bronze`, `silver`, `gold`), plutôt que le compte root
  partagé. On peut ainsi révoquer un accès sans couper les autres ;
- dans les notebooks, ne jamais taper un mot de passe dans une cellule ni
  afficher `os.environ` : les sorties restent enregistrées dans le `.ipynb` ;
- vérification TLS : pour l'instant elle est désactivée partout
  (`MINIO_VERIFY_SSL=false` dans `.env.example` et dans le Secret). Le client
  commun (`src/pipeline/common/s3_client.py`) lit `MINIO_VERIFY_SSL`, avec `false`
  si la variable est absente. Les étapes `00` à `08` (sauf `07c`),
  `10_correction_gold_trajectoire`, `11` et `plus_age_experience.py` créent leur propre client avec `verify=False` en dur : pour réactiver
  la vérification, il faudra les faire passer par `create_s3_client()`.

### Exécution du Pipeline
Le pipeline peut être lancé intégralement ou par étapes spécifiques via l'orchestrateur. C'est un processus continu destiné à intégrer les nouvelles périodes de données dès leur disponibilité.

```bash
# Lancement complet
python orchestrateur.py

# Lancement à partir d'une étape précise
python orchestrateur.py --depuis 06

# Lancement d'étapes ciblées
python orchestrateur.py --etapes 06 07 08

# Visualiser le plan sans exécuter
python orchestrateur.py --dry-run

# Reprendre après un échec (saute les étapes déjà complétées)
python orchestrateur.py --resume

# Forcer la ré-exécution complète (ignore l'état précédent)
python orchestrateur.py --force
```

### Idempotence

Après chaque étape réussie, l'orchestrateur enregistre la progression dans `reports/.pipeline_state.json`. En cas d'échec, relancer avec `--resume` reprend automatiquement à la première étape non complétée sans re-traiter les données déjà produites. Après un run complet sans erreur, ce fichier est supprimé automatiquement.

| Scénario | Commande |
|----------|----------|
| Échec à l'étape 07, correction appliquée | `python orchestrateur.py --resume` |
| Nouvelles données, tout re-traiter | `python orchestrateur.py` ou `--force` |
| Rejouer seulement certaines étapes | `python orchestrateur.py --etapes 07 08` |

### Rapports de run

À chaque exécution, l'orchestrateur génère automatiquement un rapport Markdown horodaté dans `reports/YYYY-MM/run_YYYYMMDD_HHMMSS.md`. Ce rapport contient le statut et la durée de chaque étape, les paramètres d'exécution et la liste des éventuels échecs.

```
reports/
  2025-05/
    run_20250520_143022.md
```

Ces rapports constituent la trace d'audit des runs. Il est recommandé de les committer après chaque exécution :

```bash
git add reports/
git commit -m "run $(date +%Y-%m-%d) — pipeline complet"
```

### Observabilité

L'orchestrateur et les scripts qui importent le module commun utilisent un logging structuré (`src/pipeline/common/logging_config.py`) configurable via variables d'environnement :

| Variable | Valeur par défaut | Description |
|----------|-------------------|-------------|
| `LOG_LEVEL` | `INFO` | Niveau de log (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `LOG_FORMAT` | `key-value` | Format de sortie (`key-value` ou `json`) |
| `LOG_TO_FILE` | `false` | Activer l'écriture des logs dans un fichier |
| `LOG_DIR` | `logs/` | Répertoire des fichiers de log |

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
9. **Chargement Final** : Synchronisation du panel vers PostgreSQL.
10. **Validation SQL Superset** : Application et contrôle des vues PostgreSQL avec `10_validate_superset_views.py`.

Les scripts `10_correction_gold_trajectoire.py`, `11_analyse_salaire_experience.py`
et `plus_age_experience.py` sont des traitements complémentaires lancés séparément.
Le numéro 10 de l'orchestrateur désigne la validation SQL.

## Vérifications locales

```bash
python orchestrateur.py --dry-run
python -m pytest tests src/pipeline/tests
```

Le dry-run vérifie la présence des scripts sans lancer les traitements. Les tests
du pipeline portent sur des fonctions recopiées dans les fichiers de test ; ils
ne remplacent pas une validation complète sur les données et services externes.

---
Cellule d'Analyses Économiques - ANSTAT
