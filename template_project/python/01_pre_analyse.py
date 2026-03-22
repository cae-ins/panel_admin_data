# ============================================================
# PANEL ADMIN — ÉTAPE 1 : PRÉ-ANALYSE DES COLONNES
# ============================================================
# Lit les en-têtes de tous les fichiers Excel depuis MinIO staging
# (sans télécharger les données, juste les noms de colonnes)
# et produit un rapport des changements de structure année par année.
#
# Utile avant l'ingestion pour anticiper les colonnes à mapper.
# Les rapports sont déposés dans staging/panel_admin/pre_analyse/
#
# Dépendances :
#   pip install boto3 openpyxl pandas python-dotenv
# ============================================================

import os
import io
import csv
import tempfile
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import pandas as pd

load_dotenv(".env")

# --- CONFIGURATION ---

# Endpoint selon environnement : configurer dans .env (local ou JHub)
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

BUCKET          = "staging"
PREFIX_MENSUELS = "panel_admin/fichiers_mensuels"
PREFIX_SORTIE   = "panel_admin/pre_analyse"

# --- CLIENT S3 (MinIO) ---
s3 = boto3.client(
    "s3",
    endpoint_url          = MINIO_ENDPOINT,
    aws_access_key_id     = MINIO_ACCESS_KEY,
    aws_secret_access_key = MINIO_SECRET_KEY,
    config                = Config(signature_version="s3v4"),
    region_name           = "us-east-1",
    verify                = False,
)

print("=" * 70)
print("PRÉ-ANALYSE : COLONNES 2015-2025 (depuis MinIO staging)")
print("=" * 70)
print()

# --- LISTER LES FICHIERS DANS STAGING ---

paginator = s3.get_paginator("list_objects_v2")
pages = paginator.paginate(Bucket=BUCKET, Prefix=PREFIX_MENSUELS)

fichiers_minio = sorted([
    obj["Key"]
    for page in pages
    for obj in page.get("Contents", [])
    if obj["Key"].lower().endswith(".xlsx")
])

print(f"✓ {len(fichiers_minio)} fichiers trouvés dans staging\n")

# --- EXTRACTION DES NOMS DE COLONNES ---

print("Extraction des noms de colonnes...\n")

colonnes_par_periode = {}
erreurs = []

for chemin_objet in fichiers_minio:
    periode = os.path.splitext(os.path.basename(chemin_objet))[0]
    print(f"  {periode}... ", end="", flush=True)

    try:
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name

        s3.download_file(BUCKET, chemin_objet, tmp_path)

        f1 = pd.read_excel(tmp_path, sheet_name=0, nrows=0)
        f2 = pd.read_excel(tmp_path, sheet_name=1, nrows=0)

        colonnes_par_periode[periode] = {
            "f1":    list(f1.columns),
            "f2":    list(f2.columns),
            "nb_f1": len(f1.columns),
            "nb_f2": len(f2.columns),
        }

        print(f"✓ F1:{len(f1.columns)} F2:{len(f2.columns)}")
        os.unlink(tmp_path)

    except Exception as e:
        print(f"✗ ERREUR: {e}")
        erreurs.append(periode)
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

msg = f"\n✓ {len(colonnes_par_periode)} fichiers analysés"
if erreurs:
    msg += f" | ⚠️  {len(erreurs)} erreurs"
print(msg + "\n")

# --- ANALYSE CHANGEMENTS FEUILLE 1 ---

print("=" * 70)
print("CHANGEMENTS FEUILLE 1 (DONNÉES AGENTS)")
print("=" * 70)
print()

periodes       = list(colonnes_par_periode.keys())
changements_f1 = {}

for i in range(len(periodes) - 1):
    p1    = periodes[i]
    p2    = periodes[i + 1]
    cols1 = set(colonnes_par_periode[p1]["f1"])
    cols2 = set(colonnes_par_periode[p2]["f1"])

    disparues = cols1 - cols2
    apparues  = cols2 - cols1

    if disparues or apparues:
        print(f"{p1} → {p2} :")
        if disparues:
            print(f"  ❌ DISPARUES ({len(disparues)}) :")
            for col in list(disparues)[:10]:
                print(f"     • {col}")
            if len(disparues) > 10:
                print(f"     ... et {len(disparues) - 10} autres")
        if apparues:
            print(f"  ✅ APPARUES ({len(apparues)}) :")
            for col in list(apparues)[:10]:
                print(f"     • {col}")
            if len(apparues) > 10:
                print(f"     ... et {len(apparues) - 10} autres")
        print()
        changements_f1[f"{p1}_{p2}"] = {
            "periode_avant": p1, "periode_apres": p2,
            "disparues": list(disparues), "apparues": list(apparues),
        }

if not changements_f1:
    print("✓ Aucun changement détecté\n")

# --- STATISTIQUES GLOBALES ---

print("=" * 70)
print("STATISTIQUES GLOBALES")
print("=" * 70)
print()

toutes_cols_f1 = set(c for v in colonnes_par_periode.values() for c in v["f1"])
toutes_cols_f2 = set(c for v in colonnes_par_periode.values() for c in v["f2"])
tous_codes_f2  = {c for c in toutes_cols_f2 if str(c).isdigit()}

nb_cols_f1 = [v["nb_f1"] for v in colonnes_par_periode.values()]
nb_cols_f2 = [v["nb_f2"] for v in colonnes_par_periode.values()]

print(f"Colonnes F1 uniques (toutes périodes) : {len(toutes_cols_f1)}")
print(f"Colonnes F2 uniques (toutes périodes) : {len(toutes_cols_f2)}")
print(f"Codes numériques F2 uniques : {len(tous_codes_f2)}")
print(f"\nF1 — colonnes / fichier : min={min(nb_cols_f1)}, max={max(nb_cols_f1)}, moy={sum(nb_cols_f1)/len(nb_cols_f1):.1f}")
print(f"F2 — colonnes / fichier : min={min(nb_cols_f2)}, max={max(nb_cols_f2)}, moy={sum(nb_cols_f2)/len(nb_cols_f2):.1f}")

# --- SAUVEGARDE DES RAPPORTS VERS STAGING ---

print()
print("=" * 70)
print("SAUVEGARDE DES RAPPORTS")
print("=" * 70)
print()

def sauvegarder_rapport(df, nom_fichier):
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    s3.put_object(
        Bucket = BUCKET,
        Key    = f"{PREFIX_SORTIE}/{nom_fichier}",
        Body   = buf.getvalue().encode("utf-8"),
    )
    print(f"✓ {nom_fichier}")

# Rapport 1 : colonnes F1 uniques
sauvegarder_rapport(
    pd.DataFrame({"colonne": sorted(toutes_cols_f1)}),
    "colonnes_f1_uniques.csv"
)

# Rapport 2 : stats par période
sauvegarder_rapport(
    pd.DataFrame({"periode": periodes, "nb_cols_f1": nb_cols_f1, "nb_cols_f2": nb_cols_f2}),
    "stats_par_periode.csv"
)

# Rapport 3 : changements F1
if changements_f1:
    lignes = []
    for chg in changements_f1.values():
        for col in chg["disparues"]:
            lignes.append({"p_avant": chg["periode_avant"], "p_apres": chg["periode_apres"],
                           "type": "DISPARUE", "colonne": col})
        for col in chg["apparues"]:
            lignes.append({"p_avant": chg["periode_avant"], "p_apres": chg["periode_apres"],
                           "type": "APPARUE", "colonne": col})
    sauvegarder_rapport(pd.DataFrame(lignes), "changements_f1.csv")

print("\n✓ PRÉ-ANALYSE TERMINÉE")
print(f"Rapports disponibles dans : s3://{BUCKET}/{PREFIX_SORTIE}/\n")
print("Prochaine étape : exécuter 02_staging_to_bronze.py")
