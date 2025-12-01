# Panel Admin Data — Traitement des données de solde

Projet de traitement, fusion et contrôle qualité des données de solde (paie) utilisé pour construire un panel longitudinal d'observations par matricule.

**Remarque** : ce dépôt contient plusieurs notebooks Jupyter qui réalisent l'extraction depuis un stockage S3/MinIO, le nettoyage, la fusion et des contrôles qualité (notamment sur la variable `MONTANT NET`).

**Contenu**
- **Notebooks** : plusieurs notebooks `.ipynb` (liste et description ci-dessous).
- **Scripts / fonctions** : fonctions et utilitaires pour lecture S3, nettoyage des dates, normalisation de libellés, consolidation de colonnes, et pipeline de contrôle qualité des revenus.

**Dépendances principales**
- Python 3.8+ (recommandé)
- Bibliothèques Python : `pandas`, `numpy`, `boto3`, `botocore`, `pyarrow`, `fastparquet`, `openpyxl`, `s3fs`, `polars` (utilitaire), `xlsxwriter` (optionnel).

Installation rapide (PowerShell) :
```powershell
python -m pip install --upgrade pip
pip install pandas numpy boto3 botocore pyarrow fastparquet openpyxl s3fs polars
```

**Configuration (MinIO / S3)**
- Les notebooks se connectent à un service S3/MinIO — configurez les variables suivantes avant exécution :

  - `MINIO_ENDPOINT` (ex: `http://minio.mon-namespace.svc.cluster.local:80`)
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `BUCKET_NAME` (par défaut utilisé : `admindataanstat` dans les notebooks)

Exemple (PowerShell) pour définir temporairement des variables d'environnement :
```powershell
$env:MINIO_ENDPOINT = 'http://minio.mon-namespace.svc.cluster.local:80'
$env:AWS_ACCESS_KEY_ID = 'VOTRE_ACCESS_KEY'
$env:AWS_SECRET_ACCESS_KEY = 'VOTRE_SECRET_KEY'
```

Remplacez `VOTRE_ACCESS_KEY` et `VOTRE_SECRET_KEY` par vos identifiants. Ne commitez jamais de clés en clair.

**Usage**
- Ouvrez les notebooks avec Jupyter Lab/Notebook ou Visual Studio Code (extension Jupyter).
- Exécutez les cellules séquentiellement. Les notebooks contiennent des cellules d'installation rapide (`!pip install ...`) que vous pouvez ignorer si vous avez déjà installé les dépendances.

Commandes utiles :
```powershell
# Lancer JupyterLab
jupyter lab

# Ouvrir VS Code (depuis le dossier du projet)
code .
```

**Description des notebooks**
- `Data_Fusion_Solde_1.ipynb` :
  - Connexion à MinIO, lecture de fichiers Excel (format `MMYYYY.xlsx`) dans les dossiers `Solde/DONNEES YYYY/`.
  - Détection dynamique de la ligne d'en-tête, lecture des fichiers, ajout de colonnes `PERIODE` et `DATE_COLLECTE`.
  - Construction d'un `panel_solde_df` concaténé, export CSV puis Parquet sur S3.

- `Data_Fusion_Solde_2.ipynb` :
  - Version plus segmentée/robuste pour charger les feuilles Excel, supprimer entêtes parasites et fusionner par périodes (ex. fusion 2015–2016, 2017–2018).
  - Harmonisation des colonnes, sauvegarde de la fusion en Parquet sur S3 (`Solde/FUSION/...`).

- `Jointure_Code_Solde.ipynb` :
  - Chargement du panel (parquet) depuis S3, puis chargement d'un fichier de référence contenant les codes administratifs (`FICHIER_ANSTAT_CODE_2025.xlsx`).
  - Fonctions de normalisation (`normalize_label`), lecture/clean des fichiers référence (`read_reference`) et écriture de feuilles Excel (`write_sheet`).
  - But : associer aux variables textuelles leurs codes standardisés (Fonction, Service, Organisme, Emploi, etc.).

- `Traiment_Data_Solde_1.ipynb` et `Traiment_Data_Solde_2.ipynb` :
  - Bibliothèque d'utilitaires pour le nettoyage et la transformation :
    - Normalisation des textes, harmonisation du sexe, nettoyage et conversion de dates mixtes (y compris numéros de série Excel), détection et consolidation de colonnes répétées, extraction année/mois depuis noms de fichiers, etc.
  - Fonctions de lecture performante depuis S3, consolidation par base (ex: coalescence, somme, concat), et vérifications d'exclusivité.

- `Traitement_Data_Solde_010825.ipynb` :
  - Notebook orienté nettoyage et exploration (extraction matricule/code organisme, décomposition de dates, tabulations descriptives) et export de résultats (Excel/CSV) vers S3.

- `Traitement_Salaire_Solde.ipynb` :
  - Pipeline complet pour la variable `MONTANT NET` :
    - Classe `ControleQualite` pour centraliser résultats et exports,
    - Fonctions de détection d'erreurs non temporelles (valeurs manquantes, zéros, négatifs, net>brut, outliers),
    - Détection d'anomalies temporelles par traitement par chunks (baisses/hausses fortes entre deux collectes),
    - Pipeline `pipeline_revenu_ultra_optimise()` pour enchaîner chargement → exploration → détections → contrôles.

