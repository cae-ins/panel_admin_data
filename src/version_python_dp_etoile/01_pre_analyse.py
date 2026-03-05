# ============================================================
# PANEL ADMIN — ÉTAPE 1 : PRÉ-ANALYSE DES COLONNES
# ============================================================
# Lit les en-têtes de tous les fichiers Excel depuis MinIO staging
# et produit un rapport des changements de structure année par année.
#
# Analyses réalisées :
#   - Changements de colonnes F1 entre périodes consécutives
#   - Changements de colonnes F2 (codes numériques) entre périodes
#   - Statistiques globales (min, max, moyenne du nb colonnes)
#   - Min / max des codes numériques F2 (toutes périodes)
#
# Dépendances :
#   pip install polars fastexcel boto3 python-dotenv
# ============================================================

import os
import io
import tempfile
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import polars as pl

load_dotenv(".env")

# --- CONFIGURATION ---

# LORSQU'ON TRAVAILLE DEPUIS SA MACHINE LOCAL
MINIO_ENDPOINT   = "http://192.168.1.230:30137"
MINIO_ACCESS_KEY = "datalab-team"
MINIO_SECRET_KEY = "minio-datalabteam123"

# LORSQU'ON TRAVAILLE SUR JHUB
# MINIO_ENDPOINT   = "http://minio.mon-namespace.svc.cluster.local:80"
# MINIO_ACCESS_KEY = "datalab-team"
# MINIO_SECRET_KEY = "minio-datalabteam123"

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


def sauvegarder_rapport(df: pl.DataFrame, nom_fichier: str) -> None:
    csv_bytes = df.write_csv().encode("utf-8")
    s3.put_object(
        Bucket=BUCKET,
        Key=f"{PREFIX_SORTIE}/{nom_fichier}",
        Body=csv_bytes,
    )
    print(f"✓ {nom_fichier}")


print("=" * 70)
print("PRÉ-ANALYSE : COLONNES 2015-2025 (depuis MinIO staging)")
print("=" * 70)
print()

# --- LISTER LES FICHIERS DANS STAGING ---
paginator = s3.get_paginator("list_objects_v2")
pages     = paginator.paginate(Bucket=BUCKET, Prefix=PREFIX_MENSUELS)

fichiers_minio = sorted([
    obj["Key"]
    for page in pages
    for obj in page.get("Contents", [])
    if obj["Key"].lower().endswith(".xlsx")
])

print(f"✓ {len(fichiers_minio)} fichiers trouvés dans staging\n")

# --- EXTRACTION DES NOMS DE COLONNES ---
print("Extraction des noms de colonnes...\n")

colonnes_par_periode: dict = {}
erreurs: list = []

for chemin_objet in fichiers_minio:
    periode = os.path.splitext(os.path.basename(chemin_objet))[0]
    print(f"  {periode}... ", end="", flush=True)

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name
        s3.download_file(BUCKET, chemin_objet, tmp_path)

        # FIX : read_options (pas engine_options) pour limiter la lecture aux en-têtes
        f1 = pl.read_excel(tmp_path, sheet_id=1, read_options={"n_rows": 0})
        f2 = pl.read_excel(tmp_path, sheet_id=2, read_options={"n_rows": 0})

        # Extraire les codes numériques de F2
        codes_num_f2 = [c for c in f2.columns if str(c).strip().isdigit()]

        colonnes_par_periode[periode] = {
            "f1":          f1.columns,
            "f2":          f2.columns,
            "codes_num_f2": codes_num_f2,
            "nb_f1":       len(f1.columns),
            "nb_f2":       len(f2.columns),
            "nb_codes_f2": len(codes_num_f2),
        }
        print(f"✓  F1:{len(f1.columns)} cols  F2:{len(f2.columns)} cols  "
              f"(dont {len(codes_num_f2)} codes numériques)")
        os.unlink(tmp_path)

    except Exception as e:
        print(f"✗ ERREUR : {e}")
        erreurs.append(periode)
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

msg = f"\n✓ {len(colonnes_par_periode)} fichiers analysés"
if erreurs:
    msg += f" | ⚠️  {len(erreurs)} erreur(s) : {', '.join(erreurs)}"
print(msg + "\n")

periodes = list(colonnes_par_periode.keys())

# ============================================================
# CHANGEMENTS FEUILLE 1
# ============================================================

print("=" * 70)
print("CHANGEMENTS FEUILLE 1 (DONNÉES AGENTS)")
print("=" * 70)
print()

changements_f1: dict = {}

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
            for col in sorted(disparues)[:10]:
                print(f"     • {col}")
            if len(disparues) > 10:
                print(f"     ... et {len(disparues) - 10} autres")
        if apparues:
            print(f"  ✅ APPARUES ({len(apparues)}) :")
            for col in sorted(apparues)[:10]:
                print(f"     • {col}")
            if len(apparues) > 10:
                print(f"     ... et {len(apparues) - 10} autres")
        print()
        changements_f1[f"{p1}_{p2}"] = {
            "periode_avant": p1,
            "periode_apres": p2,
            "disparues":     sorted(disparues),
            "apparues":      sorted(apparues),
        }

if not changements_f1:
    print("✓ Aucun changement détecté\n")

# ============================================================
# CHANGEMENTS FEUILLE 2 — CODES NUMÉRIQUES
# ============================================================

print("=" * 70)
print("CHANGEMENTS FEUILLE 2 (CODES NUMÉRIQUES)")
print("=" * 70)
print()

changements_f2: dict = {}

for i in range(len(periodes) - 1):
    p1      = periodes[i]
    p2      = periodes[i + 1]
    codes1  = set(colonnes_par_periode[p1]["codes_num_f2"])
    codes2  = set(colonnes_par_periode[p2]["codes_num_f2"])

    disparues = codes1 - codes2
    apparues  = codes2 - codes1

    if disparues or apparues:
        print(f"{p1} → {p2} :")
        if disparues:
            disparus_tries = sorted(disparues, key=int)
            print(f"  ❌ CODES DISPARUS ({len(disparues)}) : "
                  f"{', '.join(disparus_tries[:20])}"
                  f"{'...' if len(disparues) > 20 else ''}")
        if apparues:
            apparus_tries = sorted(apparues, key=int)
            print(f"  ✅ CODES APPARUS  ({len(apparues)}) : "
                  f"{', '.join(apparus_tries[:20])}"
                  f"{'...' if len(apparues) > 20 else ''}")
        print()
        changements_f2[f"{p1}_{p2}"] = {
            "periode_avant": p1,
            "periode_apres": p2,
            "disparues":     sorted(disparues, key=int),
            "apparues":      sorted(apparues,  key=int),
        }

if not changements_f2:
    print("✓ Aucun changement majeur détecté\n")

# ============================================================
# STATISTIQUES GLOBALES
# ============================================================

print("=" * 70)
print("STATISTIQUES GLOBALES")
print("=" * 70)
print()

toutes_cols_f1  = {c for v in colonnes_par_periode.values() for c in v["f1"]}
toutes_cols_f2  = {c for v in colonnes_par_periode.values() for c in v["f2"]}
tous_codes_f2   = sorted(
    {c for v in colonnes_par_periode.values() for c in v["codes_num_f2"]},
    key=int,
)

nb_cols_f1  = [v["nb_f1"]       for v in colonnes_par_periode.values()]
nb_cols_f2  = [v["nb_f2"]       for v in colonnes_par_periode.values()]
nb_codes_f2 = [v["nb_codes_f2"] for v in colonnes_par_periode.values()]

print(f"Colonnes F1 uniques (toutes périodes) : {len(toutes_cols_f1)}")
print(f"Colonnes F2 uniques (toutes périodes) : {len(toutes_cols_f2)}")
print(f"Codes numériques F2 uniques           : {len(tous_codes_f2)}")

if tous_codes_f2:
    print(f"  Min code F2 : {min(tous_codes_f2, key=int)}")
    print(f"  Max code F2 : {max(tous_codes_f2, key=int)}")

print(f"\nF1 — colonnes/fichier : "
      f"min={min(nb_cols_f1)}, max={max(nb_cols_f1)}, "
      f"moy={sum(nb_cols_f1)/len(nb_cols_f1):.1f}")
print(f"F2 — colonnes/fichier : "
      f"min={min(nb_cols_f2)}, max={max(nb_cols_f2)}, "
      f"moy={sum(nb_cols_f2)/len(nb_cols_f2):.1f}")
print(f"F2 — codes num/fichier : "
      f"min={min(nb_codes_f2)}, max={max(nb_codes_f2)}, "
      f"moy={sum(nb_codes_f2)/len(nb_codes_f2):.1f}")

# ============================================================
# SAUVEGARDE DES RAPPORTS
# ============================================================

print()
print("=" * 70)
print("SAUVEGARDE DES RAPPORTS")
print("=" * 70)
print()

# Rapport 1 : colonnes F1 uniques
sauvegarder_rapport(
    pl.DataFrame({"colonne": sorted(toutes_cols_f1)}),
    "colonnes_f1_uniques.csv",
)

# Rapport 2 : codes F2 numériques uniques
sauvegarder_rapport(
    pl.DataFrame({"code": tous_codes_f2}),
    "codes_f2_uniques.csv",
)

# Rapport 3 : stats par période
sauvegarder_rapport(
    pl.DataFrame({
        "periode":    periodes,
        "nb_cols_f1": nb_cols_f1,
        "nb_cols_f2": nb_cols_f2,
        "nb_codes_f2": nb_codes_f2,
    }),
    "stats_par_periode.csv",
)

# Rapport 4 : changements F1
if changements_f1:
    lignes = []
    for chg in changements_f1.values():
        for col in chg["disparues"]:
            lignes.append({"p_avant": chg["periode_avant"],
                           "p_apres": chg["periode_apres"],
                           "type": "DISPARUE", "colonne": col})
        for col in chg["apparues"]:
            lignes.append({"p_avant": chg["periode_avant"],
                           "p_apres": chg["periode_apres"],
                           "type": "APPARUE", "colonne": col})
    sauvegarder_rapport(pl.DataFrame(lignes), "changements_f1.csv")

# Rapport 5 : changements codes F2 numériques
if changements_f2:
    lignes = []
    for chg in changements_f2.values():
        for code in chg["disparues"]:
            lignes.append({"p_avant": chg["periode_avant"],
                           "p_apres": chg["periode_apres"],
                           "type": "DISPARU", "code": code})
        for code in chg["apparues"]:
            lignes.append({"p_avant": chg["periode_avant"],
                           "p_apres": chg["periode_apres"],
                           "type": "APPARU", "code": code})
    sauvegarder_rapport(pl.DataFrame(lignes), "changements_codes_f2.csv")

print(f"\n✓ PRÉ-ANALYSE TERMINÉE")
print(f"Rapports disponibles dans : s3://{BUCKET}/{PREFIX_SORTIE}/\n")
print("Prochaine étape : exécuter 02_staging_to_bronze.py")