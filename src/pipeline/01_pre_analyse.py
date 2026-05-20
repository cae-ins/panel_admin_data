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
from common.s3_client import create_s3_client
from common.logging_config import setup_pipeline_logging
import polars as pl

# --- CONFIGURATION ---

BUCKET          = os.getenv("BUCKET_STAGING", "staging")
PREFIX_MENSUELS = "panel_admin/fichiers_mensuels"
PREFIX_SORTIE   = "panel_admin/pre_analyse"

# --- CLIENT S3 (MinIO) ---
s3 = create_s3_client()
logger, _ = setup_pipeline_logging("01_pre_analyse")


def sauvegarder_rapport(df: pl.DataFrame, nom_fichier: str) -> None:
    csv_bytes = df.write_csv().encode("utf-8")
    s3.put_object(
        Bucket=BUCKET,
        Key=f"{PREFIX_SORTIE}/{nom_fichier}",
        Body=csv_bytes,
    )
    logger.info(f"✓ {nom_fichier}")


logger.info("=" * 70)
logger.info("PRÉ-ANALYSE : COLONNES 2015-2025 (depuis MinIO staging)")
logger.info("=" * 70)

# --- LISTER LES FICHIERS DANS STAGING ---
paginator = s3.get_paginator("list_objects_v2")
pages     = paginator.paginate(Bucket=BUCKET, Prefix=PREFIX_MENSUELS)

fichiers_minio = sorted([
    obj["Key"]
    for page in pages
    for obj in page.get("Contents", [])
    if obj["Key"].lower().endswith(".xlsx")
])

logger.info(f"✓ {len(fichiers_minio)} fichiers trouvés dans staging")

# --- EXTRACTION DES NOMS DE COLONNES ---
logger.info("Extraction des noms de colonnes...")

colonnes_par_periode: dict = {}
erreurs: list = []

for chemin_objet in fichiers_minio:
    periode = os.path.splitext(os.path.basename(chemin_objet))[0]

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
        logger.info(f"{periode} ✓  F1:{len(f1.columns)} cols  F2:{len(f2.columns)} cols  "
                    f"(dont {len(codes_num_f2)} codes numériques)")
        os.unlink(tmp_path)

    except Exception as e:
        logger.error(f"{periode} ✗ ERREUR : {e}")
        erreurs.append(periode)
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

msg = f"✓ {len(colonnes_par_periode)} fichiers analysés"
if erreurs:
    msg += f" | ⚠️  {len(erreurs)} erreur(s) : {', '.join(erreurs)}"
logger.info(msg)

periodes = list(colonnes_par_periode.keys())

# ============================================================
# CHANGEMENTS FEUILLE 1
# ============================================================

logger.info("=" * 70)
logger.info("CHANGEMENTS FEUILLE 1 (DONNÉES AGENTS)")
logger.info("=" * 70)

changements_f1: dict = {}

for i in range(len(periodes) - 1):
    p1    = periodes[i]
    p2    = periodes[i + 1]
    cols1 = set(colonnes_par_periode[p1]["f1"])
    cols2 = set(colonnes_par_periode[p2]["f1"])

    disparues = cols1 - cols2
    apparues  = cols2 - cols1

    if disparues or apparues:
        logger.info(f"{p1} → {p2} :")
        if disparues:
            logger.info(f"  ❌ DISPARUES ({len(disparues)}) :")
            for col in sorted(disparues)[:10]:
                logger.info(f"     • {col}")
            if len(disparues) > 10:
                logger.info(f"     ... et {len(disparues) - 10} autres")
        if apparues:
            logger.info(f"  ✅ APPARUES ({len(apparues)}) :")
            for col in sorted(apparues)[:10]:
                logger.info(f"     • {col}")
            if len(apparues) > 10:
                logger.info(f"     ... et {len(apparues) - 10} autres")
        changements_f1[f"{p1}_{p2}"] = {
            "periode_avant": p1,
            "periode_apres": p2,
            "disparues":     sorted(disparues),
            "apparues":      sorted(apparues),
        }

if not changements_f1:
    logger.info("✓ Aucun changement détecté")

# ============================================================
# CHANGEMENTS FEUILLE 2 — CODES NUMÉRIQUES
# ============================================================

logger.info("=" * 70)
logger.info("CHANGEMENTS FEUILLE 2 (CODES NUMÉRIQUES)")
logger.info("=" * 70)

changements_f2: dict = {}

for i in range(len(periodes) - 1):
    p1      = periodes[i]
    p2      = periodes[i + 1]
    codes1  = set(colonnes_par_periode[p1]["codes_num_f2"])
    codes2  = set(colonnes_par_periode[p2]["codes_num_f2"])

    disparues = codes1 - codes2
    apparues  = codes2 - codes1

    if disparues or apparues:
        logger.info(f"{p1} → {p2} :")
        if disparues:
            disparus_tries = sorted(disparues, key=int)
            logger.info(f"  ❌ CODES DISPARUS ({len(disparues)}) : "
                        f"{', '.join(disparus_tries[:20])}"
                        f"{'...' if len(disparues) > 20 else ''}")
        if apparues:
            apparus_tries = sorted(apparues, key=int)
            logger.info(f"  ✅ CODES APPARUS  ({len(apparues)}) : "
                        f"{', '.join(apparus_tries[:20])}"
                        f"{'...' if len(apparues) > 20 else ''}")
        changements_f2[f"{p1}_{p2}"] = {
            "periode_avant": p1,
            "periode_apres": p2,
            "disparues":     sorted(disparues, key=int),
            "apparues":      sorted(apparues,  key=int),
        }

if not changements_f2:
    logger.info("✓ Aucun changement majeur détecté")

# ============================================================
# STATISTIQUES GLOBALES
# ============================================================

logger.info("=" * 70)
logger.info("STATISTIQUES GLOBALES")
logger.info("=" * 70)

toutes_cols_f1  = {c for v in colonnes_par_periode.values() for c in v["f1"]}
toutes_cols_f2  = {c for v in colonnes_par_periode.values() for c in v["f2"]}
tous_codes_f2   = sorted(
    {c for v in colonnes_par_periode.values() for c in v["codes_num_f2"]},
    key=int,
)

nb_cols_f1  = [v["nb_f1"]       for v in colonnes_par_periode.values()]
nb_cols_f2  = [v["nb_f2"]       for v in colonnes_par_periode.values()]
nb_codes_f2 = [v["nb_codes_f2"] for v in colonnes_par_periode.values()]

logger.info(f"Colonnes F1 uniques (toutes périodes) : {len(toutes_cols_f1)}")
logger.info(f"Colonnes F2 uniques (toutes périodes) : {len(toutes_cols_f2)}")
logger.info(f"Codes numériques F2 uniques           : {len(tous_codes_f2)}")

if tous_codes_f2:
    logger.info(f"  Min code F2 : {min(tous_codes_f2, key=int)}")
    logger.info(f"  Max code F2 : {max(tous_codes_f2, key=int)}")

logger.info(f"F1 — colonnes/fichier : "
            f"min={min(nb_cols_f1)}, max={max(nb_cols_f1)}, "
            f"moy={sum(nb_cols_f1)/len(nb_cols_f1):.1f}")
logger.info(f"F2 — colonnes/fichier : "
            f"min={min(nb_cols_f2)}, max={max(nb_cols_f2)}, "
            f"moy={sum(nb_cols_f2)/len(nb_cols_f2):.1f}")
logger.info(f"F2 — codes num/fichier : "
            f"min={min(nb_codes_f2)}, max={max(nb_codes_f2)}, "
            f"moy={sum(nb_codes_f2)/len(nb_codes_f2):.1f}")

# ============================================================
# SAUVEGARDE DES RAPPORTS
# ============================================================

logger.info("=" * 70)
logger.info("SAUVEGARDE DES RAPPORTS")
logger.info("=" * 70)

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

logger.info("✓ PRÉ-ANALYSE TERMINÉE")
logger.info(f"Rapports disponibles dans : s3://{BUCKET}/{PREFIX_SORTIE}/")
logger.info("Prochaine étape : exécuter 02_staging_to_bronze.py")
