# Panel Admin Data

Panel longitudinal des salaires de la fonction publique ivoirienne (2015–2025).
Consolide ~120 fichiers Excel mensuels (`MMYYYY.xlsx`) en un panel individu × période
de **~31 millions de lignes**.

---

## Architecture

```
staging/panel_admin/        ← fichiers mensuels MMYYYY.xlsx
        │
        ▼  01→02→03→03b→03c→04→05→06→07→08→09
        │
   Bronze (Parquet MinIO)   ← données brutes
        │
   Silver (Parquet MinIO)   ← nettoyé, imputé (grades + salaires)
        │
   Gold  (Parquet MinIO)    ← agrégations + panel_complet.parquet
        │
        ├──▶ Excel indicateurs (staging/exports_gold/)
        │
        └──▶ PostgreSQL table `panel`
                    │
                    ▼
             Dashboard Superset
```

---

## Pipeline (`src/version_python_dp_etoile/`)

| Étape | Script | Description |
|-------|--------|-------------|
| 00 | `00_upload_staging.py` | Upload des fichiers Excel mensuels vers MinIO staging |
| 01 | `01_pre_analyse.py` | Analyse structure colonnes (F1/F2, variations entre périodes) |
| 02 | `02_staging_to_bronze.py` | Excel → Parquet Bronze |
| 03 | `03_bronze_to_silver.py` | Mapping colonnes, normalisation situations, enrichissement ANSTAT |
| 03b | `03b_imputer_grades_silver.py` | Imputation grades (cascade P1/P2/P3 par emploi) |
| 03c | `03c_imputer_salaires_silver.py` | Imputation salaires (LOCF + médiane cascade) |
| 04 | `04_validation_silver.py` | Contrôles qualité Silver *(optionnel)* |
| 05 | `05_silver_to_gold.py` | Agrégations Gold (masse salariale, effectifs) |
| 06 | `06_compiler_panel.py` | Panel individu × période + `panel_complet.parquet` |
| 07 | `07_calcul_indicateur.py` | Indicateurs salaires (grade, sexe, CITP, multi-postes) |
| 08 | `08_creation_fichier_excel_avec_sommaire.py` | Excel navigable avec sommaire |
| 09 | `09_charger_panel_pg.py` | Chargement `panel_complet.parquet` → PostgreSQL |
| 10 | `10_refresh_superset.sh` | Refresh vue matérialisée Superset (kubectl) |

### Lancer le pipeline manuellement

```bash
cd src/version_python_dp_etoile
python orchestrateur.py                     # pipeline complet (01→09)
python orchestrateur.py --depuis 03         # reprendre à l'étape 03
python orchestrateur.py --etapes 07 08      # étapes spécifiques
python orchestrateur.py --dry-run           # afficher le plan sans exécuter

# Après le pipeline Python :
bash 10_refresh_superset.sh
```

### Variables d'environnement

Copier `.env.example` → `.env` et renseigner les valeurs :

```bash
# MinIO
MINIO_ENDPOINT=http://192.168.1.230:30137   # local
# MINIO_ENDPOINT=http://minio.mon-namespace.svc.cluster.local:80  # JHub
MINIO_ACCESS_KEY=<access_key>
MINIO_SECRET_KEY=<secret_key>

# Buckets
BUCKET_STAGING=staging
BUCKET_BRONZE=bronze
BUCKET_SILVER=silver
BUCKET_GOLD=gold

# PostgreSQL (étape 09)
PG_HOST=postgres-service.postgres.svc.cluster.local
PG_PORT=5432
PG_DB=datalab_db
PG_USER=datalab
PG_PASSWORD=<mot_de_passe>

# Pipeline
MIN_OBS_IMPUTATION=5
```

---

## Orchestration Airflow (production)

Le pipeline est orchestré via un DAG Airflow déclenché automatiquement
dès qu'un nouveau fichier mensuel apparaît dans `staging/panel_admin/`.

- **DAG** : `Data_Plateform/infra/airflow/panel_admin_dag.py`
- **Déclencheur** : `S3KeySensor` sur `staging/panel_admin/MMYYYY.xlsx`
- **Exécution** : `KubernetesPodOperator` (image Docker `panel-admin`)
- **Déploiement** : repo GitHub privé `dag-airflow`

Prérequis avant premier lancement :
```bash
# 1. Builder et pousser l'image Docker
docker build -t registry.datalab.local/panel-admin:latest .
docker push registry.datalab.local/panel-admin:latest

# 2. Appliquer le Secret K8s
kubectl apply -f infra/panel-admin-secret.yaml -n airflow

# 3. Configurer la connexion minio_s3 dans Airflow UI
#    (Admin → Connections, type Amazon S3, endpoint MinIO)

# 4. Pousser le DAG sur le repo dag-airflow
```

---

## Dashboard Superset

Vues PostgreSQL dans `superset/superset_views.sql`.

```bash
# Première installation
psql -U datalab -d datalab_db -f superset/superset_views.sql

# Après chaque nouveau mois (si pipeline manuel)
python src/version_python_dp_etoile/09_charger_panel_pg.py
bash src/version_python_dp_etoile/10_refresh_superset.sh
```

---

## Tests

```bash
cd src/version_python_dp_etoile
python -m pytest tests/ -v
```

---

## Structure du dépôt

```
panel_admin_data/
├── src/version_python_dp_etoile/   ← scripts pipeline (00→10)
│   ├── orchestrateur.py
│   ├── tests/
│   ├── .env                        ← credentials (non commité)
│   └── requirements.txt
├── superset/
│   ├── superset_views.sql          ← vues PostgreSQL Superset
│   └── README.md
├── infra/
│   └── panel-admin-secret.yaml    ← Secret K8s (credentials)
├── docs/                           ← documentation
├── Dockerfile
└── README.md
```
