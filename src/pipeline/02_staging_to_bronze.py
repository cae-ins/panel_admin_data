# ============================================================
# PANEL ADMIN — ÉTAPE 2 : STAGING → BRONZE PARQUET
# ============================================================
# Lit chaque fichier Excel mensuel depuis MinIO staging,
# applique le mapping minimal de colonnes (normalisation des
# noms uniquement, sans transformation métier),
# et écrit un fichier Parquet par période dans le bucket bronze.
#
# Bronze = données brutes, tout en String, toutes périodes
# empilées. Les types et règles métier vont en Silver (étape 3).
#
# Sortie : s3://bronze/panel_admin/YYYY-MM.parquet (un par mois)
#
# Architecture : Polars uniquement, pas de Spark ni d'Iceberg.
#
# Dépendances :
#   pip install polars fastexcel boto3 python-dotenv
# ============================================================

import io
import os
import re
import tempfile
import unicodedata
from common.s3_client import create_s3_client
from common.logging_config import setup_pipeline_logging
from common.polars_s3 import normaliser_nom_colonne
import warnings
import polars as pl

warnings.filterwarnings("ignore", message="Could not determine dtype")

# --- CONFIGURATION ---

BUCKET_STAGING  = os.getenv("BUCKET_STAGING", "staging")
BUCKET_BRONZE   = os.getenv("BUCKET_BRONZE",  "bronze")
PREFIX_MENSUELS = "panel_admin/fichiers_mensuels"
PREFIX_BRONZE   = "panel_admin"

# --- CLIENT S3 (MinIO) ---
s3 = create_s3_client()
logger, _ = setup_pipeline_logging("02_staging_to_bronze")

# ============================================================
# FONCTIONS UTILITAIRES
# ============================================================

def deduplication_noms(noms: list[str]) -> list[str]:
    """Rend une liste de noms de colonnes unique en suffixant les doublons."""
    seen: dict = {}
    result = []
    for n in noms:
        if n in seen:
            seen[n] += 1
            result.append(f"{n}_{seen[n]}")
        else:
            seen[n] = 1
            result.append(n)
    return result


def detecter_entete(fichier: str, sheet_id: int) -> int:
    """Détecte la ligne d'en-tête contenant 'matricule' (0-indexed)."""
    try:
        preview = pl.read_excel(
            fichier,
            sheet_id=sheet_id + 1,
            read_options={"header_row": None, "n_rows": 20},
        )
        rows = preview.rows()
        if len(rows) > 1:
            rows = rows[:-1]
        for i, row in enumerate(rows):
            if any("matricule" in str(v).lower() for v in row if v is not None):
                return i
        return 1 if sheet_id == 1 else 0
    except Exception:
        return 1 if sheet_id == 1 else 0


def ecrire_parquet_bronze(df: pl.DataFrame, periode: str) -> None:
    """Sérialise un DataFrame Polars et l'envoie sur MinIO bronze."""
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)
    key = f"{PREFIX_BRONZE}/{periode}.parquet"
    s3.put_object(Bucket=BUCKET_BRONZE, Key=key, Body=buf.getvalue())


# ============================================================
# LISTER LES FICHIERS DANS STAGING
# ============================================================

logger.info("=" * 70)
logger.info("STAGING → BRONZE : Panel Administratif")
logger.info("=" * 70)

paginator = s3.get_paginator("list_objects_v2")
pages = paginator.paginate(Bucket=BUCKET_STAGING, Prefix=PREFIX_MENSUELS)

fichiers_minio = sorted([
    obj["Key"]
    for page in pages
    for obj in page.get("Contents", [])
    if obj["Key"].lower().endswith(".xlsx")
])

logger.info(f"Fichiers dans staging : {len(fichiers_minio)}")

# ============================================================
# TRAITEMENT FICHIER PAR FICHIER (Polars → Parquet/bronze)
# ============================================================

nb_ok  = 0
nb_err = 0

for chemin_objet in fichiers_minio:
    periode     = os.path.splitext(os.path.basename(chemin_objet))[0]
    nom_fichier = os.path.basename(chemin_objet)

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name
        s3.download_file(BUCKET_STAGING, chemin_objet, tmp_path)

        # --- LECTURE FEUILLE 1 (données agents) ---
        skip_f1 = detecter_entete(tmp_path, sheet_id=0)
        feuille1 = pl.read_excel(
            tmp_path,
            sheet_id=1,
            read_options={"header_row": skip_f1},
        )

        # Détecter la clé composite MATRICULE||CODE_ORGANISME dans F1
        # AVANT normalisation (la normalisation supprimerait les "||")
        nom_cle_f1 = next(
            (c for c in feuille1.columns
             if re.search(r"MATRICULE.*\|\|", str(c), re.IGNORECASE)),
            None,
        )
        if nom_cle_f1:
            feuille1 = feuille1.rename({nom_cle_f1: "CLE_UNIQUE"})

        # Normalisation + déduplication (CLE_UNIQUE est déjà protégé)
        noms_norm = [normaliser_nom_colonne(c) for c in feuille1.columns]
        feuille1  = feuille1.rename(dict(zip(feuille1.columns, deduplication_noms(noms_norm))))

        # --- LECTURE FEUILLE 2 (codes numériques) ---
        skip_f2 = detecter_entete(tmp_path, sheet_id=1)
        feuille2 = pl.read_excel(
            tmp_path,
            sheet_id=2,
            read_options={"header_row": skip_f2},
        )

        # Jointure F2 si clé MATRICULE||CODE_ORGANISME trouvée
        nom_cle_f2 = next(
            (c for c in feuille2.columns
             if re.search(r"MATRICULE.*\|\|", str(c), re.IGNORECASE)),
            None,
        )

        if nom_cle_f2:
            feuille2 = feuille2.rename({nom_cle_f2: "CLE_UNIQUE_F2"})
            rename_f2 = {
                c: normaliser_nom_colonne(c) + "_F2"
                for c in feuille2.columns if c != "CLE_UNIQUE_F2"
            }
            feuille2 = feuille2.rename(rename_f2)

            cols_num_f2      = [c for c in feuille2.columns if re.match(r"^\d+_F2$", c)]
            cols_obligatoires = {"124_F2", "421_F2"}

            if cols_obligatoires.issubset(set(cols_num_f2)):
                feuille2 = feuille2.filter(
                    pl.col("124_F2").is_not_null()
                    & (pl.col("124_F2").cast(pl.Utf8).str.strip_chars() != "")
                    & pl.col("421_F2").is_not_null()
                    & (pl.col("421_F2").cast(pl.Utf8).str.strip_chars() != "")
                )
                logger.info(f"  [{periode}] F2 : {len(feuille2):,} lignes avec 124+421 renseignés")
            else:
                logger.info(f"  [{periode}] F2 : colonnes 124_F2/421_F2 absentes, F2 ignoré")
                feuille2 = None

            if feuille2 is not None:
                cle_f1 = "CLE_UNIQUE" if "CLE_UNIQUE" in feuille1.columns else None

                if cle_f1:
                    feuille1 = feuille1.join(
                        feuille2,
                        left_on=cle_f1,
                        right_on="CLE_UNIQUE_F2",
                        how="inner",
                    )
                    logger.info(f"  [{periode}] F1↔F2 : {len(feuille1):,} individus communs")
                else:
                    logger.info(f"  [{periode}] CLE_UNIQUE absente de F1, jointure ignorée")
        else:
            logger.info(f"  [{periode}] F2 : clé MATRICULE||CODE_ORGANISME introuvable, F2 ignoré")

        # Ajout des métadonnées et cast tout-String (Bronze = brut)
        feuille1 = (
            feuille1
            .with_columns([
                pl.lit(periode).alias("PERIODE"),
                pl.lit(nom_fichier).alias("FICHIER_SOURCE"),
            ])
            .with_columns([pl.col(c).cast(pl.Utf8) for c in feuille1.columns])
        )

        os.unlink(tmp_path)

        # --- ÉCRITURE PARQUET SUR BRONZE ---
        ecrire_parquet_bronze(feuille1, periode)

        logger.info(f"  [{periode}] ✓ {len(feuille1):,} lignes × {len(feuille1.columns)} cols")
        nb_ok += 1

    except Exception as e:
        logger.error(f"  [{periode}] ✗ ERREUR : {e}")
        nb_err += 1
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

# ============================================================
# RÉSUMÉ FINAL
# ============================================================

logger.info("=" * 70)
logger.info("RÉSUMÉ")
logger.info("=" * 70)
logger.info(f"Fichiers traités avec succès : {nb_ok}")
logger.info(f"Erreurs                      : {nb_err}")
logger.info(f"Fichiers Parquet disponibles dans : s3://{BUCKET_BRONZE}/{PREFIX_BRONZE}/")
logger.info("✓ INGESTION BRONZE TERMINÉE")
logger.info("Prochaine étape : exécuter 03_bronze_to_silver.py")
