# Prochaines étapes — 22 mars 2026

## 🔴 Priorité haute

### 1. Mettre en production le pipeline Airflow
Tous les éléments sont prêts, il reste à déployer :
- [ ] Builder l'image Docker et la pousser vers le registry
  ```bash
  docker build -t registry.datalab.local/panel-admin:latest .
  docker push registry.datalab.local/panel-admin:latest
  ```
- [ ] Remplir les credentials dans `infra/panel-admin-secret.yaml` puis appliquer
  ```bash
  kubectl apply -f infra/panel-admin-secret.yaml -n airflow
  ```
- [ ] Configurer la connexion `minio_s3` dans l'UI Airflow
  (Admin → Connections, type Amazon S3, endpoint MinIO cluster)
- [ ] Pousser `Data_Plateform/infra/airflow/panel_admin_dag.py` sur le repo GitHub privé `dag-airflow`
- [ ] Tester le DAG sur un fichier de janvier 2026 (`012026.xlsx`)

---

## 🟡 Priorité moyenne

### 2. Indicateur vétérinaires (étape 07)
Ajouter le nombre de vétérinaires dans la fonction publique comme
nouvel indicateur dans `07_calcul_indicateur.py`.
- Identifier le/les codes emploi ou CITP correspondant aux vétérinaires
- Ajouter une feuille dédiée dans l'Excel de sortie
- Ajouter la vue correspondante dans `superset/superset_views.sql`

### 3. Diagnostic mois de juin manquant pour A4/A5/A6
Les grades A4, A5, A6 n'ont pas de données pour juin dans certaines années.
- Identifier les années concernées
- Déterminer si c'est une absence réelle ou un problème d'imputation
- Corriger dans `03b_imputer_grades_silver.py` ou `03c_imputer_salaires_silver.py`

### 4. Vues Superset — corps vétérinaires
Le mapping barème dans `superset/superset_views.sql` (`v_panel_base`)
ne couvre pas les vétérinaires.
- Ajouter une règle `WHEN` pour les identifier (emploi ou organisme)
- Recréer les vues après modification :
  ```bash
  psql -U datalab -d datalab_db -f superset/superset_views.sql
  ```

---

## 🟢 Priorité basse

### 5. Créer un `.env.example`
Créer `src/version_python_dp_etoile/.env.example` (template sans credentials)
pour faciliter l'onboarding.

### 6. Documenter la structure des fichiers source
Préciser dans la doc la structure attendue des fichiers `MMYYYY.xlsx`
(2 feuilles F1/F2, ligne d'en-tête variable, etc.).
