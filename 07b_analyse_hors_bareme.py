# ============================================================
# PANEL ADMIN — ÉTAPE 7b : ANALYSE DES AGENTS HORS BARÈME
# ============================================================
# Extrait et analyse les agents dont le salaire brut se situe
# en dehors des bornes min/max du référentiel ANSTAT 2025.
#
# Prérequis : 07_calcul_indicateur.py doit avoir été exécuté
#             (produit base_avec_statut.parquet sur Gold).
#
# Sorties  :
#   - Excel multi-feuilles avec statistiques détaillées
#   - Fichier Parquet des agents hors barème (Gold)
#
# Source   : s3://gold/panel_admin/base_avec_statut.parquet
# Sortie   : s3://staging/panel_admin/exports_gold/analyse_hors_bareme.xlsx
#             s3://gold/panel_admin/agents_hors_bareme.parquet
# ============================================================

import io
import os
import tempfile
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import polars as pl
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment

load_dotenv(".env")

# ============================================================
# CONFIGURATION
# ============================================================

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "datalab-team")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio-datalabteam123")

BUCKET_GOLD    = os.getenv("BUCKET_GOLD",    "gold")
BUCKET_STAGING = os.getenv("BUCKET_STAGING", "staging")

KEY_BASE_STATUT   = "panel_admin/base_avec_statut.parquet"
PREFIX_EXPORTS    = "panel_admin/exports_gold"
OUTPUT_KEY        = f"{PREFIX_EXPORTS}/analyse_hors_bareme.xlsx"
KEY_HORS_BAREME_PQ = "panel_admin/agents_hors_bareme.parquet"

# Statuts "hors barème" à analyser
STATUTS_HORS_BAREME = [
    "Inférieur au minimum du barème",
    "Supérieur au maximum du barème",
]

# ============================================================
# CLIENT S3
# ============================================================

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

print("=" * 70)
print("ANALYSE DES AGENTS HORS BARÈME")
print("=" * 70)
print()

# ============================================================
# 1. CHARGEMENT DE base_avec_statut.parquet
# ============================================================

print("1. Chargement de base_avec_statut.parquet depuis Gold...")

try:
    buf = io.BytesIO()
    s3.download_fileobj(BUCKET_GOLD, KEY_BASE_STATUT, buf)
    buf.seek(0)
    base = pl.read_parquet(buf)
    print(f"   ✓ {len(base):,} lignes × {len(base.columns)} colonnes chargées")
except Exception as e:
    raise RuntimeError(
        f"Impossible de charger {KEY_BASE_STATUT} — "
        f"exécutez d'abord 07_calcul_indicateur.py.\n"
        f"Erreur : {e}"
    )

# Vérification de la colonne Statut
if "Statut" not in base.columns:
    raise RuntimeError(
        "Colonne 'Statut' absente de base_avec_statut.parquet. "
        "Régénérez le fichier avec 07_calcul_indicateur.py."
    )

# Distribution globale des statuts
print("\n   Distribution globale des statuts :")
n_total = len(base)
for row in base["Statut"].value_counts().sort("Statut").iter_rows(named=True):
    pct = 100 * row["count"] / n_total
    print(f"     {row['Statut']:<40} : {row['count']:>10,}  ({pct:.1f}%)")
print()

# ============================================================
# 2. FILTRAGE DES AGENTS HORS BARÈME
# ============================================================

print("2. Extraction des agents hors barème...")

hors_bareme = base.filter(pl.col("Statut").is_in(STATUTS_HORS_BAREME))

n_hors = len(hors_bareme)
print(f"   ✓ {n_hors:,} agents hors barème ({100*n_hors/n_total:.1f}% du total)")

if n_hors == 0:
    print("\n   ⚠️  Aucun agent hors barème trouvé — arrêt du script.")
    exit(0)

# Sous-catégories
n_inf = int((hors_bareme["Statut"] == "Inférieur au minimum du barème").sum())
n_sup = int((hors_bareme["Statut"] == "Supérieur au maximum du barème").sum())
print(f"     Inférieur au minimum : {n_inf:,} ({100*n_inf/n_hors:.1f}%)")
print(f"     Supérieur au maximum : {n_sup:,} ({100*n_sup/n_hors:.1f}%)")
print()

# ============================================================
# 3. STATISTIQUES DÉTAILLÉES
# ============================================================

print("3. Calcul des statistiques détaillées...")

# Identifier les colonnes disponibles
col_salaire = next(
    (c for c in ["montant_brut_bareme", "SALAIRE_BRUT", "montant_brut"] if c in hors_bareme.columns),
    None,
)
col_grade = "GRADE" if "GRADE" in hors_bareme.columns else None
col_bareme = "bareme" if "bareme" in hors_bareme.columns else None
col_sexe = next(
    (c for c in ["SEXE_STD", "sexe"] if c in hors_bareme.columns), None
)
col_annee = "ANNEE" if "ANNEE" in hors_bareme.columns else None

if col_salaire is None:
    print("   ⚠️  Aucune colonne salaire trouvée — statistiques limitées")

# --- 3a. Par Statut ---
if col_salaire:
    stats_statut = (
        hors_bareme
        .group_by("Statut")
        .agg([
            pl.len().alias("Effectif"),
            pl.col(col_salaire).mean().round(0).alias("Salaire_moyen"),
            pl.col(col_salaire).median().round(0).alias("Salaire_median"),
            pl.col(col_salaire).min().round(0).alias("Salaire_min"),
            pl.col(col_salaire).max().round(0).alias("Salaire_max"),
            pl.col(col_salaire).quantile(0.25, interpolation="nearest").round(0).alias("P25"),
            pl.col(col_salaire).quantile(0.75, interpolation="nearest").round(0).alias("P75"),
            pl.col(col_salaire).std().round(0).alias("Ecart_type"),
        ])
        .sort("Statut")
    )
    print(f"   ✓ Stats par Statut : {len(stats_statut)} lignes")

# --- 3b. Par Statut × Grade ---
stats_statut_grade = pl.DataFrame()
if col_salaire and col_grade:
    stats_statut_grade = (
        hors_bareme
        .group_by(["Statut", col_grade])
        .agg([
            pl.len().alias("Effectif"),
            pl.col(col_salaire).mean().round(0).alias("Salaire_moyen"),
            pl.col(col_salaire).median().round(0).alias("Salaire_median"),
            pl.col(col_salaire).min().round(0).alias("Salaire_min"),
            pl.col(col_salaire).max().round(0).alias("Salaire_max"),
            pl.col(col_salaire).std().round(0).alias("Ecart_type"),
        ])
        .sort(["Statut", col_grade])
    )
    print(f"   ✓ Stats par Statut × Grade : {len(stats_statut_grade)} lignes")

# --- 3c. Par Statut × Barème (secteur) ---
stats_statut_bareme = pl.DataFrame()
if col_salaire and col_bareme:
    stats_statut_bareme = (
        hors_bareme
        .filter(pl.col(col_bareme).is_not_null())
        .group_by(["Statut", col_bareme])
        .agg([
            pl.len().alias("Effectif"),
            pl.col(col_salaire).mean().round(0).alias("Salaire_moyen"),
            pl.col(col_salaire).median().round(0).alias("Salaire_median"),
            pl.col(col_salaire).min().round(0).alias("Salaire_min"),
            pl.col(col_salaire).max().round(0).alias("Salaire_max"),
            pl.col(col_salaire).std().round(0).alias("Ecart_type"),
        ])
        .sort(["Statut", col_bareme])
    )
    print(f"   ✓ Stats par Statut × Barème : {len(stats_statut_bareme)} lignes")

# --- 3d. Par Statut × Sexe ---
stats_statut_sexe = pl.DataFrame()
if col_salaire and col_sexe:
    stats_statut_sexe = (
        hors_bareme
        .group_by(["Statut", col_sexe])
        .agg([
            pl.len().alias("Effectif"),
            pl.col(col_salaire).mean().round(0).alias("Salaire_moyen"),
            pl.col(col_salaire).median().round(0).alias("Salaire_median"),
            pl.col(col_salaire).min().round(0).alias("Salaire_min"),
            pl.col(col_salaire).max().round(0).alias("Salaire_max"),
        ])
        .sort(["Statut", col_sexe])
    )
    print(f"   ✓ Stats par Statut × Sexe : {len(stats_statut_sexe)} lignes")

# --- 3e. Par Statut × Année ---
stats_statut_annee = pl.DataFrame()
if col_salaire and col_annee:
    stats_statut_annee = (
        hors_bareme
        .group_by(["Statut", col_annee])
        .agg([
            pl.len().alias("Effectif"),
            pl.col(col_salaire).mean().round(0).alias("Salaire_moyen"),
            pl.col(col_salaire).median().round(0).alias("Salaire_median"),
            pl.col(col_salaire).min().round(0).alias("Salaire_min"),
            pl.col(col_salaire).max().round(0).alias("Salaire_max"),
        ])
        .sort(["Statut", col_annee])
    )
    print(f"   ✓ Stats par Statut × Année : {len(stats_statut_annee)} lignes")

# --- 3f. Par Statut × Barème × Grade (croisement complet) ---
stats_croise = pl.DataFrame()
if col_salaire and col_bareme and col_grade:
    stats_croise = (
        hors_bareme
        .filter(pl.col(col_bareme).is_not_null())
        .group_by(["Statut", col_bareme, col_grade])
        .agg([
            pl.len().alias("Effectif"),
            pl.col(col_salaire).mean().round(0).alias("Salaire_moyen"),
            pl.col(col_salaire).median().round(0).alias("Salaire_median"),
            pl.col(col_salaire).min().round(0).alias("Salaire_min"),
            pl.col(col_salaire).max().round(0).alias("Salaire_max"),
        ])
        .sort(["Statut", col_bareme, col_grade])
    )
    print(f"   ✓ Stats croisées Statut × Barème × Grade : {len(stats_croise)} lignes")

# --- 3g. Écart au barème ---
ecart_detail = pl.DataFrame()
if col_salaire and "min_bareme" in hors_bareme.columns and "max_bareme" in hors_bareme.columns:
    ecart_detail = (
        hors_bareme
        .with_columns([
            pl.when(pl.col("Statut") == "Inférieur au minimum du barème")
              .then(pl.col(col_salaire) - pl.col("min_bareme"))
              .otherwise(pl.col(col_salaire) - pl.col("max_bareme"))
              .alias("ecart_absolu"),
            pl.when(pl.col("Statut") == "Inférieur au minimum du barème")
              .then(((pl.col(col_salaire) - pl.col("min_bareme")) / pl.col("min_bareme") * 100).round(1))
              .otherwise(((pl.col(col_salaire) - pl.col("max_bareme")) / pl.col("max_bareme") * 100).round(1))
              .alias("ecart_pct"),
        ])
    )

    # Stats sur l'écart par Statut
    stats_ecart = (
        ecart_detail
        .group_by("Statut")
        .agg([
            pl.col("ecart_absolu").mean().round(0).alias("Ecart_moyen_FCFA"),
            pl.col("ecart_absolu").median().round(0).alias("Ecart_median_FCFA"),
            pl.col("ecart_pct").mean().round(1).alias("Ecart_moyen_pct"),
            pl.col("ecart_pct").median().round(1).alias("Ecart_median_pct"),
            pl.col("ecart_absolu").min().round(0).alias("Ecart_min_FCFA"),
            pl.col("ecart_absolu").max().round(0).alias("Ecart_max_FCFA"),
        ])
        .sort("Statut")
    )
    print(f"   ✓ Stats écart au barème : {len(stats_ecart)} lignes")

print()

# ============================================================
# 4. EXPORT PARQUET (données complètes hors barème)
# ============================================================

print("4. Export Parquet des agents hors barème...")

# Si on a calculé les écarts, les inclure
if len(ecart_detail) > 0:
    export_pq = ecart_detail
else:
    export_pq = hors_bareme

buf_pq = io.BytesIO()
export_pq.write_parquet(buf_pq, compression="snappy")
buf_pq.seek(0)
s3.put_object(Bucket=BUCKET_GOLD, Key=KEY_HORS_BAREME_PQ, Body=buf_pq.getvalue())
print(f"   ✓ s3://{BUCKET_GOLD}/{KEY_HORS_BAREME_PQ} ({len(export_pq):,} lignes)")
print()

# ============================================================
# 5. EXPORT EXCEL
# ============================================================

print("5. Export Excel analyse hors barème...")


def appliquer_style_entete(ws, n_cols: int) -> None:
    fill  = PatternFill("solid", fgColor="C0392B")  # Rouge foncé pour "hors barème"
    font  = Font(color="FFFFFF", bold=True, size=11)
    align = Alignment(horizontal="center", vertical="center")
    for col in range(1, n_cols + 1):
        cell = ws.cell(row=1, column=col)
        cell.fill      = fill
        cell.font      = font
        cell.alignment = align


def ecrire_feuille(wb: Workbook, nom: str, df: pl.DataFrame) -> None:
    if len(df) == 0:
        return
    ws = wb.create_sheet(title=nom)
    for j, col in enumerate(df.columns, start=1):
        ws.cell(row=1, column=j, value=col)
    for i, row in enumerate(df.iter_rows(named=True), start=2):
        for j, col in enumerate(df.columns, start=1):
            ws.cell(row=i, column=j, value=row[col])
    appliquer_style_entete(ws, len(df.columns))
    ws.freeze_panes = "A2"
    # Auto-ajuster les largeurs de colonnes
    for j, col in enumerate(df.columns, start=1):
        max_len = max(len(str(col)), 12)
        ws.column_dimensions[chr(64 + j) if j <= 26 else "A"].width = max_len + 2
    print(f"   ✓ {nom} ({len(df):,} lignes)")


wb = Workbook()
wb.remove(wb.active)

# Feuille 1 : Résumé par statut
if col_salaire:
    ecrire_feuille(wb, "RESUME_Statut", stats_statut)

# Feuille 2 : Par Statut × Grade
if len(stats_statut_grade) > 0:
    ecrire_feuille(wb, "HORS_Statut_Grade", stats_statut_grade)

# Feuille 3 : Par Statut × Barème
if len(stats_statut_bareme) > 0:
    ecrire_feuille(wb, "HORS_Statut_Bareme", stats_statut_bareme)

# Feuille 4 : Par Statut × Sexe
if len(stats_statut_sexe) > 0:
    ecrire_feuille(wb, "HORS_Statut_Sexe", stats_statut_sexe)

# Feuille 5 : Par Statut × Année
if len(stats_statut_annee) > 0:
    ecrire_feuille(wb, "HORS_Statut_Annee", stats_statut_annee)

# Feuille 6 : Croisement complet Statut × Barème × Grade
if len(stats_croise) > 0:
    ecrire_feuille(wb, "HORS_Bareme_Grade", stats_croise)

# Feuille 7 : Stats d'écart au barème
if len(ecart_detail) > 0:
    ecrire_feuille(wb, "ECART_Resume", stats_ecart)

# Feuille 8 : Échantillon des agents hors barème (max 5000 lignes pour Excel)
# Sélection des colonnes les plus pertinentes
cols_echantillon = [c for c in [
    "matricule", "cle_unique", "mois_annee", "ANNEE", "MOIS",
    col_salaire, "min_bareme", "max_bareme", "Statut",
    "ecart_absolu", "ecart_pct",
    col_grade, "GRADE_SOURCE", col_bareme, "bareme_detail",
    col_sexe, "Code_CITP", "Metier_CITP",
    "organisme", "CODE_ORGANISME", "emploi",
] if c is not None and c in (ecart_detail.columns if len(ecart_detail) > 0 else hors_bareme.columns)]

df_echantillon = (ecart_detail if len(ecart_detail) > 0 else hors_bareme).select(cols_echantillon)

# Trier par écart le plus important (valeur absolue)
if "ecart_absolu" in df_echantillon.columns:
    df_echantillon = df_echantillon.sort("ecart_absolu")

# Limiter à 10 000 lignes
if len(df_echantillon) > 10_000:
    print(f"   ⚠️  Échantillon limité à 10 000 lignes (sur {len(df_echantillon):,})")
    # Garder les plus extrêmes (5000 premiers + 5000 derniers)
    df_echantillon = pl.concat([
        df_echantillon.head(5_000),
        df_echantillon.tail(5_000),
    ]).unique()

ecrire_feuille(wb, "DETAIL_Agents", df_echantillon)

# Sauvegarde
buf_xlsx = io.BytesIO()
wb.save(buf_xlsx)
buf_xlsx.seek(0)
xlsx_bytes = buf_xlsx.getvalue()

s3.put_object(
    Bucket=BUCKET_STAGING,
    Key=OUTPUT_KEY,
    Body=xlsx_bytes,
    ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

taille_kb = len(xlsx_bytes) / 1024
print(f"\n✅ Fichier créé : s3://{BUCKET_STAGING}/{OUTPUT_KEY}")
print(f"   Taille   : {taille_kb:.1f} KB")
print(f"   Feuilles : {len(wb.sheetnames)}")
print(f"\n✅ Parquet complet : s3://{BUCKET_GOLD}/{KEY_HORS_BAREME_PQ}")
print(f"   ({len(export_pq):,} lignes — toutes les données hors barème)")

print()
print("=" * 70)
print("✓ ANALYSE HORS BARÈME TERMINÉE")
print("=" * 70)
