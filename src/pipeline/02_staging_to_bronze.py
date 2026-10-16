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
from math import ceil
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import warnings
import polars as pl

warnings.filterwarnings("ignore", message="Could not determine dtype")

load_dotenv(".env")

# --- CONFIGURATION ---

# LORSQU'ON TRAVAILLE DEPUIS SA MACHINE LOCAL
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "")

BUCKET_STAGING  = os.getenv("BUCKET_STAGING", "staging")
BUCKET_BRONZE   = os.getenv("BUCKET_BRONZE",  "bronze")
PREFIX_MENSUELS = "panel_admin/fichiers_mensuels"
PREFIX_BRONZE   = "panel_admin"

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

# ============================================================
# FONCTIONS UTILITAIRES
# ============================================================

def normaliser_nom_colonne(nom: str) -> str:
    """Normalise un nom de colonne : ASCII majuscule, sans caractères spéciaux."""
    if not nom or isinstance(nom, float):
        return "colonne_vide"
    nom = str(nom)
    nom = re.sub(r"^[A-Z_]+[0-9]+\.", "", nom)
    nom = nom.upper()
    nom = re.sub(r"[-/_ ]", "_", nom)
    nom = re.sub(r"\.", "_", nom)
    nom = unicodedata.normalize("NFKD", nom)
    nom = "".join(c for c in nom if not unicodedata.combining(c))
    nom = re.sub(r"[^A-Z0-9_]", "", nom)
    nom = re.sub(r"_+", "_", nom)
    return nom.strip("_")


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

print("=" * 70)
print("STAGING → BRONZE : Panel Administratif")
print("=" * 70)
print()

paginator = s3.get_paginator("list_objects_v2")
pages = paginator.paginate(Bucket=BUCKET_STAGING, Prefix=PREFIX_MENSUELS)

fichiers_minio = sorted([
    obj["Key"]
    for page in pages
    for obj in page.get("Contents", [])
    if obj["Key"].lower().endswith(".xlsx")
])

print(f"Fichiers dans staging : {len(fichiers_minio)}\n")

# ============================================================
# TRAITEMENT FICHIER PAR FICHIER (Polars → Parquet/bronze)
# ============================================================

nb_ok  = 0
nb_err = 0

for chemin_objet in fichiers_minio:
    periode     = os.path.splitext(os.path.basename(chemin_objet))[0]
    nom_fichier = os.path.basename(chemin_objet)
    print(f"  [{periode}] ... ", end="", flush=True)

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

        # Normalisation + déduplication des noms de colonnes
        # Détecter la clé composite MATRICULE||CODE_ORGANISME dans F1 AVANT normalisation
        # (les || sont supprimés par normaliser_nom_colonne → colonne jamais reconnue comme CLE_UNIQUE)
        noms_orig = list(feuille1.columns)
        noms_norm = [normaliser_nom_colonne(c) for c in noms_orig]
        for i, nom in enumerate(noms_orig):
            if re.search(r"MATRICULE.*\|\|", str(nom), re.IGNORECASE):
                noms_norm[i] = "CLE_UNIQUE"
                break
        feuille1  = feuille1.rename(dict(zip(noms_orig, deduplication_noms(noms_norm))))

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

            # Clé F1 au même format que la clé F2 (MATRICULE||CODE_ORGANISME)
            if "CLE_UNIQUE" in feuille1.columns:
                expr_cle_f1 = pl.col("CLE_UNIQUE").cast(pl.Utf8)
            elif {"MATRICULE", "CODE_ORGANISME"}.issubset(feuille1.columns):
                expr_cle_f1 = pl.concat_str(
                    [pl.col("MATRICULE").cast(pl.Utf8), pl.col("CODE_ORGANISME").cast(pl.Utf8)],
                    separator="||",
                )
            else:
                raise RuntimeError(
                    "Feuille 1 sans colonne MATRICULE||CODE_ORGANISME ni MATRICULE + "
                    "CODE_ORGANISME : impossible de rattacher la Feuille 2"
                )

            def _normaliser_cle(expr: pl.Expr) -> pl.Expr:
                return (expr.str.strip_chars().str.to_uppercase()
                        .str.replace_all(r"\s*\|\|\s*", "||"))

            feuille1 = feuille1.with_columns(_normaliser_cle(expr_cle_f1).alias("_CLE_JOINTURE_F2"))
            feuille2 = feuille2.with_columns(
                _normaliser_cle(pl.col("CLE_UNIQUE_F2").cast(pl.Utf8)).alias("_CLE_JOINTURE_F2"),
                pl.lit(True).alias("_F2_TROUVEE"),
            ).drop("CLE_UNIQUE_F2")

            n_doublons_f2 = len(feuille2) - feuille2["_CLE_JOINTURE_F2"].n_unique()
            if n_doublons_f2 > 0:
                print(f"\n      ⚠️  Feuille 2 : {n_doublons_f2:,} clés en double "
                      f"(lignes F1 dupliquées au rattachement)", end="")

            feuille1 = feuille1.join(feuille2, on="_CLE_JOINTURE_F2", how="left")
            n_f1      = len(feuille1)
            n_trouves = int(feuille1["_F2_TROUVEE"].fill_null(False).sum())
            feuille1  = feuille1.drop(["_CLE_JOINTURE_F2", "_F2_TROUVEE"])
            print(f"F2 rattachée {n_trouves:,}/{n_f1:,} ({100 * n_trouves / n_f1 if n_f1 else 0:.1f}%)  ",
                  end="", flush=True)
            if len(feuille2) > 0 and n_trouves == 0:
                raise RuntimeError(
                    "Feuille 2 présente mais AUCUNE ligne rattachée à la Feuille 1 "
                    "(format de la clé MATRICULE||CODE_ORGANISME différent entre les deux onglets)"
                )
        else:
            print("⚠️  Feuille 2 sans colonne MATRICULE||… — non rattachée  ", end="", flush=True)

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

        print(f"✓  {len(feuille1):,} lignes × {len(feuille1.columns)} cols")
        nb_ok += 1

    except Exception as e:
        print(f"✗ ERREUR : {e}")
        nb_err += 1
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

# ============================================================
# RÉSUMÉ FINAL
# ============================================================

print()
print("=" * 70)
print("RÉSUMÉ")
print("=" * 70)
print()
print(f"Fichiers traités avec succès : {nb_ok}")
print(f"Erreurs                      : {nb_err}")
print(f"\nFichiers Parquet disponibles dans : s3://{BUCKET_BRONZE}/{PREFIX_BRONZE}/")
print("\n✓ INGESTION BRONZE TERMINÉE")
print("Prochaine étape : exécuter 03_bronze_to_silver.py")