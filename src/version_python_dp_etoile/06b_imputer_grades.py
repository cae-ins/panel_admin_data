# ============================================================
# PANEL ADMIN — ÉTAPE 6b : IMPUTATION DES GRADES
# ============================================================
# Lit le panel complet (toutes années), calcule les grades
# manquants ou invalides par imputation statistique, puis
# réécrit les colonnes GRADE et GRADE_SOURCE dans chaque
# fichier panel annuel ET dans panel_complet.parquet.
#
# Pourquoi une étape dédiée après 06_compiler_panel.py ?
#   L'imputation (surtout le niveau P3 global) est d'autant
#   plus fiable qu'elle s'appuie sur l'intégralité de
#   l'historique. Travailler année par année donnerait des
#   lookups instables pour les emplois rares.
#
# Méthode : cascade emploi sur 3 niveaux de priorité
#   P1 : grade modal de l'emploi pour ce mois précis
#   P2 : grade modal de l'emploi pour cette année
#   P3 : grade modal de l'emploi sur tout l'historique
#
# Colonnes ajoutées au Gold :
#   GRADE        (Utf8)  — ex. "A3", "B2", "NF"
#   GRADE_SOURCE (Utf8)  — "OBSERVE", "IMPUTE", "NF_NON_IMPUTABLE"
#
# Source  : s3://gold/panel_admin/panel_complet.parquet
#           s3://gold/panel_admin/panel_YYYY.parquet
# Sortie  : mise à jour en place des mêmes fichiers
#
# Dépendances :
#   pip install polars boto3 python-dotenv
# ============================================================

import gc
import io
import os
import re
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

BUCKET_GOLD  = "gold"
PREFIX_PANEL = "panel_admin"
KEY_COMPLET  = "panel_admin/panel_complet.parquet"

# Grades valides et invalides (identiques à 07_calcul_indicateur.py)
GRADES_VALIDES = (
    [f"A{i}" for i in range(1, 8)] +
    [f"B{i}" for i in range(1, 7)] +
    [f"C{i}" for i in range(1, 6)] +
    [f"D{i}" for i in range(1, 4)]
)
GRADES_INVALIDES = ["B7", "C7", "D7", "A", "B", "C", "D"]

# --- CLIENT S3 ---
s3 = boto3.client(
    "s3",
    endpoint_url          = MINIO_ENDPOINT,
    aws_access_key_id     = MINIO_ACCESS_KEY,
    aws_secret_access_key = MINIO_SECRET_KEY,
    config                = Config(
        signature_version = "s3v4",
        retries           = {"max_attempts": 5, "mode": "adaptive"},
        connect_timeout   = 30,
        read_timeout      = 120,
    ),
    region_name = "us-east-1",
    verify      = False,
)

# ============================================================
# FONCTIONS UTILITAIRES
# ============================================================

def lire_parquet_s3(bucket: str, key: str) -> pl.DataFrame:
    buf = io.BytesIO()
    s3.download_fileobj(bucket, key, buf)
    buf.seek(0)
    return pl.read_parquet(buf)


def ecrire_parquet_s3(df: pl.DataFrame, bucket: str, key: str) -> None:
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)
    taille = len(buf.getvalue()) / 1024**2
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    print(f"  ✓ s3://{bucket}/{key}  ({len(df):,} lignes  {taille:.1f} MB)")


def extraire_grade(statut: str | None) -> str:
    """Extrait le grade (ex: A3, B2) depuis statut_fonctionnaire."""
    if not statut:
        return "NF"
    m = re.search(r"Cat[eé]gorie\s+([ABCD][0-9]*)", str(statut), re.IGNORECASE)
    if m:
        return m.group(1).upper().strip()
    return "NF"


# ============================================================
# ÉTAPE 1 : CHARGEMENT DU PANEL COMPLET
# ============================================================

print("=" * 70)
print("IMPUTATION DES GRADES — Panel complet")
print("=" * 70)
print()

print("1. Chargement du panel complet...")
panel = lire_parquet_s3(BUCKET_GOLD, KEY_COMPLET)
print(f"   ✓ {len(panel):,} lignes × {len(panel.columns)} colonnes\n")

# ============================================================
# ÉTAPE 2 : EXTRACTION DU GRADE BRUT
# ============================================================

print("2. Extraction du grade depuis statut_fonctionnaire...")

if "statut_fonctionnaire" not in panel.columns:
    print("   ⚠️  Colonne statut_fonctionnaire absente — GRADE = NF pour toutes les lignes")
    panel = panel.with_columns([
        pl.lit("NF").alias("GRADE"),
        pl.lit("NF_NON_IMPUTABLE").alias("GRADE_SOURCE"),
    ])
else:
    panel = panel.with_columns(
        pl.col("statut_fonctionnaire")
        .map_elements(extraire_grade, return_dtype=pl.Utf8)
        .alias("GRADE")
    )

n_total     = len(panel)
n_valides   = int(panel["GRADE"].is_in(GRADES_VALIDES).sum())
n_invalides = int(panel["GRADE"].is_in(GRADES_INVALIDES).sum())
n_nf        = int((panel["GRADE"] == "NF").sum())

print(f"   Grades valides     : {n_valides:,}   ({100*n_valides/n_total:.1f}%)")
print(f"   Grades invalides   : {n_invalides:,}   ({100*n_invalides/n_total:.1f}%)")
print(f"   Non fonctionnaires : {n_nf:,}   ({100*n_nf/n_total:.1f}%)\n")

# ============================================================
# ÉTAPE 3 : IMPUTATION PAR CASCADE EMPLOI (3 NIVEAUX)
# ============================================================
# On travaille sur TOUT le panel complet pour que les lookups
# P2 et P3 soient calculés sur l'intégralité de l'historique.
# ============================================================

print("3. Imputation des grades invalides (cascade emploi — 3 niveaux)...")

if n_invalides == 0:
    print("   ✓ Aucun grade invalide détecté, pas d'imputation nécessaire")
    panel = panel.with_columns(
        pl.when(pl.col("GRADE").is_in(GRADES_VALIDES))
        .then(pl.lit("OBSERVE"))
        .otherwise(pl.lit("NF_NON_IMPUTABLE"))
        .alias("GRADE_SOURCE")
    )
else:
    # mois_annee est la colonne période dans le Silver/Gold
    # (format MMYYYY, ex. "012015")
    col_periode = "mois_annee"
    has_emploi  = "emploi" in panel.columns

    if not has_emploi:
        print("   ⚠️  Colonne emploi absente — imputation impossible, GRADE conservé tel quel")
        panel = panel.with_columns(
            pl.when(pl.col("GRADE").is_in(GRADES_VALIDES))
            .then(pl.lit("OBSERVE"))
            .otherwise(pl.lit("NF_NON_IMPUTABLE"))
            .alias("GRADE_SOURCE")
        )
    else:
        # Extraction ANNEE et MOIS_NUM depuis mois_annee (format MMYYYY)
        # mois  = slice(0, 2)  ex. "01"
        # annee = slice(2, 4)  ex. "2015"
        panel = panel.with_columns([
            pl.col(col_periode).str.slice(2, 4).cast(pl.Int32, strict=False).alias("_ANNEE"),
            pl.col(col_periode).str.slice(0, 2).cast(pl.Int32, strict=False).alias("_MOIS_NUM"),
        ])

        # Base des grades valides uniquement pour construire les lookups
        base_valides = panel.filter(pl.col("GRADE").is_in(GRADES_VALIDES))

        # ── Lookup P1 : emploi × _ANNEE × _MOIS_NUM ──────────────────────
        print("   Calcul lookup P1 (emploi × année × mois)...")
        lk_p1 = (
            base_valides
            .group_by(["emploi", "_ANNEE", "_MOIS_NUM"])
            .agg(pl.col("GRADE").mode().first().alias("GRADE_P1"))
        )
        print(f"   ✓ P1 : {len(lk_p1):,} combinaisons")

        # ── Lookup P2 : emploi × _ANNEE ───────────────────────────────────
        print("   Calcul lookup P2 (emploi × année)...")
        lk_p2 = (
            base_valides
            .group_by(["emploi", "_ANNEE"])
            .agg(pl.col("GRADE").mode().first().alias("GRADE_P2"))
        )
        print(f"   ✓ P2 : {len(lk_p2):,} combinaisons")

        # ── Lookup P3 : emploi (global, toutes années) ────────────────────
        print("   Calcul lookup P3 (emploi — global)...")
        lk_p3 = (
            base_valides
            .group_by(["emploi"])
            .agg(pl.col("GRADE").mode().first().alias("GRADE_P3"))
        )
        print(f"   ✓ P3 : {len(lk_p3):,} emplois couverts")

        del base_valides
        gc.collect()

        # ── Jointure des lookups ──────────────────────────────────────────
        print("   Jointure des lookups sur le panel...")
        panel = panel.join(lk_p1, on=["emploi", "_ANNEE", "_MOIS_NUM"], how="left")
        panel = panel.join(lk_p2, on=["emploi", "_ANNEE"],              how="left")
        panel = panel.join(lk_p3, on=["emploi"],                        how="left")

        del lk_p1, lk_p2, lk_p3
        gc.collect()

        # ── Application de la cascade sur les grades invalides ────────────
        print("   Application de la cascade P1 → P2 → P3...")

        panel = panel.with_columns([
            # GRADE : remplacement uniquement sur les invalides
            pl.when(pl.col("GRADE").is_in(GRADES_INVALIDES))
            .then(
                pl.when(pl.col("GRADE_P1").is_not_null()).then(pl.col("GRADE_P1"))
                .when(pl.col("GRADE_P2").is_not_null()).then(pl.col("GRADE_P2"))
                .when(pl.col("GRADE_P3").is_not_null()).then(pl.col("GRADE_P3"))
                .otherwise(pl.col("GRADE"))  # garde l'invalide si aucun lookup
            )
            .otherwise(pl.col("GRADE"))
            .alias("GRADE"),

            # GRADE_SOURCE_TMP : traçabilité intermédiaire
            pl.when(pl.col("GRADE").is_in(GRADES_VALIDES))
            .then(pl.lit("OBSERVE"))
            .when(
                pl.col("GRADE").is_in(GRADES_INVALIDES)
                & pl.col("GRADE_P1").is_not_null()
            )
            .then(pl.lit("IMPUTE"))
            .when(
                pl.col("GRADE").is_in(GRADES_INVALIDES)
                & pl.col("GRADE_P2").is_not_null()
            )
            .then(pl.lit("IMPUTE"))
            .when(
                pl.col("GRADE").is_in(GRADES_INVALIDES)
                & pl.col("GRADE_P3").is_not_null()
            )
            .then(pl.lit("IMPUTE"))
            .when(pl.col("GRADE") == "NF")
            .then(pl.lit("NF_NON_IMPUTABLE"))
            .otherwise(pl.lit("NF_NON_IMPUTABLE"))
            .alias("GRADE_SOURCE_TMP"),
        ])

        # Recalcul propre de GRADE_SOURCE après remplacement effectif
        panel = panel.with_columns(
            pl.when(
                pl.col("GRADE").is_in(GRADES_VALIDES)
                & (pl.col("GRADE_SOURCE_TMP") == "OBSERVE")
            )
            .then(pl.lit("OBSERVE"))
            .when(
                pl.col("GRADE").is_in(GRADES_VALIDES)
                & (pl.col("GRADE_SOURCE_TMP") == "IMPUTE")
            )
            .then(pl.lit("IMPUTE"))
            .otherwise(pl.lit("NF_NON_IMPUTABLE"))
            .alias("GRADE_SOURCE")
        ).drop(["GRADE_P1", "GRADE_P2", "GRADE_P3", "GRADE_SOURCE_TMP",
                "_ANNEE", "_MOIS_NUM"])

        # ── Bilan de l'imputation ─────────────────────────────────────────
        n_observe       = int((panel["GRADE_SOURCE"] == "OBSERVE").sum())
        n_impute        = int((panel["GRADE_SOURCE"] == "IMPUTE").sum())
        n_non_imputable = int((panel["GRADE_SOURCE"] == "NF_NON_IMPUTABLE").sum())
        n_encore_invalide = int(panel["GRADE"].is_in(GRADES_INVALIDES).sum())

        print()
        print("   Bilan imputation :")
        print(f"     OBSERVE          : {n_observe:,}  ({100*n_observe/n_total:.1f}%)")
        print(f"     IMPUTE           : {n_impute:,}  ({100*n_impute/n_total:.1f}%)")
        print(f"     NF_NON_IMPUTABLE : {n_non_imputable:,}  ({100*n_non_imputable/n_total:.1f}%)")
        if n_encore_invalide > 0:
            print(f"     ⚠️  Grades encore invalides (emploi sans historique) : {n_encore_invalide:,}")
        print()

# ============================================================
# ÉTAPE 4 : RÉORDONNANCEMENT DES COLONNES
# ============================================================
# On place GRADE et GRADE_SOURCE juste après statut_fonctionnaire.

print("4. Réordonnancement des colonnes...")

cols = panel.columns
if "statut_fonctionnaire" in cols:
    idx  = cols.index("statut_fonctionnaire")
    cols = [c for c in cols if c not in ("GRADE", "GRADE_SOURCE")]
    cols = cols[:idx + 1] + ["GRADE", "GRADE_SOURCE"] + cols[idx + 1:]
    panel = panel.select(cols)

print(f"   ✓ Colonnes finales : {len(panel.columns)}")
print(f"   Position GRADE        : colonne {panel.columns.index('GRADE') + 1}")
print(f"   Position GRADE_SOURCE : colonne {panel.columns.index('GRADE_SOURCE') + 1}\n")

# ============================================================
# ÉTAPE 5 : MISE À JOUR DES FICHIERS GOLD ANNUELS
# ============================================================
# On réécrit chaque panel_YYYY.parquet en ajoutant / remplaçant
# uniquement les colonnes GRADE et GRADE_SOURCE.

print("5. Mise à jour des fichiers Gold annuels...")
print()

pages_panel = s3.get_paginator("list_objects_v2").paginate(
    Bucket=BUCKET_GOLD, Prefix=PREFIX_PANEL
)
cles_annuels = sorted([
    obj["Key"]
    for page in pages_panel
    for obj in page.get("Contents", [])
    if re.search(r"panel_20\d{2}\.parquet$", obj["Key"])
])

print(f"   Fichiers panel annuels trouvés : {len(cles_annuels)}")

# Table de correspondance : clé + grades imputés
# Jointure sur (matricule, mois_annee) — clés présentes dans tous les panels annuels
col_join = "matricule" if "matricule" in panel.columns else "cle_unique"
grades_table = panel.select([col_join, "mois_annee", "GRADE", "GRADE_SOURCE"])

for cle in cles_annuels:
    annee_str = re.search(r"20\d{2}", os.path.basename(cle))
    annee_str = annee_str.group() if annee_str else os.path.basename(cle)

    print(f"   [{annee_str}] Lecture...", end=" ", flush=True)
    df_annee = lire_parquet_s3(BUCKET_GOLD, cle)
    print(f"✓ {len(df_annee):,} lignes", flush=True)

    # Supprimer les éventuelles anciennes colonnes GRADE / GRADE_SOURCE
    cols_a_garder = [c for c in df_annee.columns if c not in ("GRADE", "GRADE_SOURCE")]
    df_annee = df_annee.select(cols_a_garder)

    # Jointure avec la table de grades imputés
    df_annee = df_annee.join(
        grades_table,
        on=[col_join, "mois_annee"],
        how="left"
    )

    # Réordonner : GRADE et GRADE_SOURCE après statut_fonctionnaire
    cols = df_annee.columns
    if "statut_fonctionnaire" in cols:
        idx  = cols.index("statut_fonctionnaire")
        cols = [c for c in cols if c not in ("GRADE", "GRADE_SOURCE")]
        cols = cols[:idx + 1] + ["GRADE", "GRADE_SOURCE"] + cols[idx + 1:]
        df_annee = df_annee.select(cols)

    ecrire_parquet_s3(df_annee, BUCKET_GOLD, cle)
    del df_annee
    gc.collect()

# ============================================================
# ÉTAPE 6 : MISE À JOUR DU PANEL COMPLET
# ============================================================

print()
print("6. Mise à jour du panel complet...")
ecrire_parquet_s3(panel, BUCKET_GOLD, KEY_COMPLET)

# ============================================================
# RÉSUMÉ FINAL
# ============================================================

print()
print("=" * 70)
print("RÉSUMÉ IMPUTATION")
print("=" * 70)
print()
print(f"Lignes totales           : {n_total:,}")
print(f"Grades OBSERVE           : {int((panel['GRADE_SOURCE'] == 'OBSERVE').sum()):,}  "
      f"({100*int((panel['GRADE_SOURCE'] == 'OBSERVE').sum())/n_total:.1f}%)")
print(f"Grades IMPUTE            : {int((panel['GRADE_SOURCE'] == 'IMPUTE').sum()):,}  "
      f"({100*int((panel['GRADE_SOURCE'] == 'IMPUTE').sum())/n_total:.1f}%)")
print(f"Grades NF_NON_IMPUTABLE  : {int((panel['GRADE_SOURCE'] == 'NF_NON_IMPUTABLE').sum()):,}  "
      f"({100*int((panel['GRADE_SOURCE'] == 'NF_NON_IMPUTABLE').sum())/n_total:.1f}%)")
print()
print(f"Fichiers Gold mis à jour : {len(cles_annuels)} fichiers annuels + panel_complet")
print()
print("=" * 70)
print("✓ IMPUTATION TERMINÉE")
print("=" * 70)
print()
print("Colonnes ajoutées dans tous les fichiers Gold :")
print("  GRADE        — grade final (valide ou imputé)")
print("  GRADE_SOURCE — OBSERVE | IMPUTE | NF_NON_IMPUTABLE")
print()
print("Prochaine étape : exécuter 07_calcul_indicateur.py")
print("  → Le script lira GRADE et GRADE_SOURCE directement,")
print("    sans recalculer l'imputation.")