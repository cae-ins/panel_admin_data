# ============================================================
# PANEL ADMIN — DIAGNOSTIC : PÉRIODES MANQUANTES & SAUT 2025
# ============================================================
# Investiguer deux anomalies détectées dans l'aperçu du panel :
#
#   1. 129 périodes distinctes au lieu de 132 attendues
#      → Quels mois manquent ? À quelle étape ont-ils disparu ?
#        (bronze présent mais absent du panel ?)
#
#   2. Saut d'individus uniques en 2025 : +51k vs +10-25k habituels
#      → Nouveaux matricules jamais vus avant 2025 ?
#      → Matricules réapparus après absence ?
#      → Concentration dans certains organismes ?
#
# Source : s3://gold/panel_admin/panel_YYYY.parquet
#          s3://bronze/panel_admin/*.parquet  (vérification croisée)
#
# Dépendances :
#   pip install polars boto3 python-dotenv
# ============================================================

import io
import os
import re
import gc
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import polars as pl

load_dotenv(".env")

# --- CONFIGURATION ---
MINIO_ENDPOINT   = "http://192.168.1.230:30137"
MINIO_ACCESS_KEY = "datalab-team"
MINIO_SECRET_KEY = "minio-datalabteam123"

# LORSQU'ON TRAVAILLE SUR JHUB
# MINIO_ENDPOINT   = "http://minio.mon-namespace.svc.cluster.local:80"
# MINIO_ACCESS_KEY = "datalab-team"
# MINIO_SECRET_KEY = "minio-datalabteam123"

BUCKET_GOLD   = "gold"
BUCKET_BRONZE = "bronze"
BUCKET_STAGING = "staging"
PREFIX_PANEL  = "panel_admin"
PREFIX_BRONZE = "panel_admin"
PREFIX_EXPORTS = "panel_admin/exports_gold"

s3 = boto3.client(
    "s3",
    endpoint_url          = MINIO_ENDPOINT,
    aws_access_key_id     = MINIO_ACCESS_KEY,
    aws_secret_access_key = MINIO_SECRET_KEY,
    config                = Config(signature_version="s3v4"),
    region_name           = "us-east-1",
    verify                = False,
)

def lire_parquet_s3(bucket: str, key: str) -> pl.DataFrame:
    buf = io.BytesIO()
    s3.download_fileobj(bucket, key, buf)
    buf.seek(0)
    return pl.read_parquet(buf)

def sauvegarder_csv(df: pl.DataFrame, nom: str) -> None:
    s3.put_object(
        Bucket=BUCKET_STAGING,
        Key=f"{PREFIX_EXPORTS}/{nom}",
        Body=df.write_csv().encode("utf-8"),
    )
    print(f"  ✓ Rapport : {nom}")

# ============================================================
# CHARGEMENT DU PANEL (années par années pour économiser RAM)
# ============================================================

print("=" * 70)
print("DIAGNOSTIC : PÉRIODES MANQUANTES & SAUT INDIVIDUS 2025")
print("=" * 70)
print()

pages = s3.get_paginator("list_objects_v2").paginate(
    Bucket=BUCKET_GOLD, Prefix=PREFIX_PANEL
)
cles_panel = sorted([
    obj["Key"]
    for page in pages
    for obj in page.get("Contents", [])
    if re.search(r"panel_20\d{2}\.parquet$", obj["Key"])
])
print(f"Fichiers panel trouvés : {len(cles_panel)}\n")

# Accumulateurs
periodes_par_annee: dict[str, set] = {}
matricules_par_annee: dict[str, set] = {}
nb_obs_par_annee: dict[str, int] = {}

for cle in cles_panel:
    annee = re.search(r"20\d{2}", os.path.basename(cle)).group()
    df = lire_parquet_s3(BUCKET_GOLD, cle)

    if "mois_annee" in df.columns:
        periodes_par_annee[annee] = set(df["mois_annee"].drop_nulls().unique().to_list())
    if "matricule" in df.columns:
        matricules_par_annee[annee] = set(df["matricule"].drop_nulls().unique().to_list())
    nb_obs_par_annee[annee] = len(df)

    del df
    gc.collect()

# ============================================================
# DIAGNOSTIC 1 : PÉRIODES MANQUANTES
# ============================================================

print("=" * 70)
print("DIAGNOSTIC 1 : PÉRIODES MANQUANTES")
print("=" * 70)
print()

# Construire toutes les périodes attendues : 012015 → 122025
periodes_attendues = set()
for annee in range(2015, 2026):
    for mois in range(1, 13):
        periodes_attendues.add(f"{mois:02d}{annee}")

# Périodes présentes dans le panel
periodes_panel = set()
for p in periodes_par_annee.values():
    periodes_panel.update(p)

manquantes = sorted(periodes_attendues - periodes_panel)
en_trop    = sorted(periodes_panel - periodes_attendues)

print(f"Périodes attendues  : {len(periodes_attendues)}")
print(f"Périodes dans panel : {len(periodes_panel)}")
print(f"Manquantes          : {len(manquantes)}")
print(f"Inattendues         : {len(en_trop)}")

if manquantes:
    print(f"\n⚠️  PÉRIODES ABSENTES DU PANEL :")
    for p in manquantes:
        mois  = p[:2]
        annee = p[2:]
        print(f"   {p}  (mois {mois} année {annee})")

if en_trop:
    print(f"\n⚠️  PÉRIODES NON ATTENDUES (format inhabituel ?) :")
    for p in en_trop[:20]:
        print(f"   {p}")

# Vérification croisée : ces périodes sont-elles dans le bronze ?
print()
print("Vérification croisée avec le bronze...")

pages_bronze = s3.get_paginator("list_objects_v2").paginate(
    Bucket=BUCKET_BRONZE, Prefix=PREFIX_BRONZE
)
periodes_bronze = set(
    os.path.splitext(os.path.basename(obj["Key"]))[0]
    for page in pages_bronze
    for obj in page.get("Contents", [])
    if obj["Key"].endswith(".parquet")
)

for p in manquantes:
    dans_bronze = p in periodes_bronze
    statut = "✓ présent dans bronze → perdu entre bronze et silver/panel" \
             if dans_bronze else "✗ absent du bronze → jamais ingéré"
    print(f"   {p} : {statut}")

# ============================================================
# DIAGNOSTIC 2 : SAUT D'INDIVIDUS EN 2025
# ============================================================

print()
print("=" * 70)
print("DIAGNOSTIC 2 : SAUT D'INDIVIDUS EN 2025")
print("=" * 70)
print()

# Tous les matricules vus avant 2025
mat_avant_2025 = set()
for annee, mats in matricules_par_annee.items():
    if int(annee) < 2025:
        mat_avant_2025.update(mats)

mat_2025 = matricules_par_annee.get("2025", set())

# Catégorisation
nouveaux_2025     = mat_2025 - mat_avant_2025   # jamais vus avant
deja_vus          = mat_2025 & mat_avant_2025   # connus avant 2025

print(f"Individus uniques en 2025          : {len(mat_2025):,}")
print(f"  → Déjà présents avant 2025       : {len(deja_vus):,}  ({100*len(deja_vus)/len(mat_2025):.1f}%)")
print(f"  → Nouveaux (jamais vus avant)    : {len(nouveaux_2025):,}  ({100*len(nouveaux_2025)/len(mat_2025):.1f}%)")

# Comparer avec les nouvelles entrées des années précédentes
print()
print("Nouveaux individus par année (jamais vus dans les années précédentes) :")
mat_cumul = set()
rows_nouveaux = []
for annee in sorted(matricules_par_annee.keys()):
    mat_annee   = matricules_par_annee[annee]
    nb_nouveaux = len(mat_annee - mat_cumul)
    nb_total    = len(mat_annee)
    mat_cumul.update(mat_annee)
    flag = "  ⚠️  SAUT" if nb_nouveaux > 30_000 else ""
    print(f"  {annee} : {nb_nouveaux:>6,} nouveaux  /  {nb_total:>6,} total{flag}")
    rows_nouveaux.append({
        "annee": annee,
        "nb_individus_total": nb_total,
        "nb_nouveaux": nb_nouveaux,
        "pct_nouveaux": round(100 * nb_nouveaux / nb_total, 1),
        "nb_obs": nb_obs_par_annee.get(annee, 0),
    })

# ============================================================
# DIAGNOSTIC 2b : RÉAPPARITIONS EN 2025
# ============================================================
# Matricules présents en 2025 mais absents en 2024
# → agents "revenus" après une absence

mat_2024 = matricules_par_annee.get("2024", set())
mat_avant_2024 = set()
for annee, mats in matricules_par_annee.items():
    if int(annee) < 2024:
        mat_avant_2024.update(mats)

reapparus_2025 = (mat_2025 - mat_2024) & mat_avant_2024  # absents en 2024, réapparus en 2025

print()
print(f"Matricules absents en 2024 mais réapparus en 2025 : {len(reapparus_2025):,}")
print(f"  (agents partis puis revenus, changement de situation, etc.)")

# ============================================================
# DIAGNOSTIC 2c : CONCENTRATION PAR ORGANISME (panel 2025)
# ============================================================
# Charger le panel 2025 pour voir d'où viennent les nouveaux

print()
print("Concentration des nouveaux par organisme (panel 2025)...")

cle_2025 = next((c for c in cles_panel if "2025" in c), None)
if cle_2025:
    df25 = lire_parquet_s3(BUCKET_GOLD, cle_2025)

    col_org = next(
        (c for c in ["CODE_ORGANISME", "organisme", "ORGANISME"] if c in df25.columns),
        None
    )

    if col_org and "matricule" in df25.columns:
        # Nouveaux matricules 2025 dans ce DataFrame
        df25 = df25.with_columns(
            pl.col("matricule").is_in(list(nouveaux_2025)).alias("est_nouveau")
        )

        dist_org = (
            df25
            .filter(pl.col("est_nouveau"))
            .group_by(col_org)
            .agg([
                pl.col("matricule").n_unique().alias("nb_nouveaux"),
                pl.len().alias("nb_obs"),
            ])
            .sort("nb_nouveaux", descending=True)
            .head(20)
        )

        print(f"\n  Top 20 organismes avec le plus de nouveaux matricules en 2025 :")
        for row in dist_org.iter_rows(named=True):
            print(f"    {str(row[col_org]):<50}  {row['nb_nouveaux']:>6,} nouveaux")

        sauvegarder_csv(dist_org, "diag_nouveaux_2025_par_organisme.csv")

    # Distribution mensuelle des nouveaux matricules en 2025
    if "mois_annee" in df25.columns and "matricule" in df25.columns:
        # Premier mois d'apparition de chaque matricule nouveau en 2025
        premiere_apparition = (
            df25
            .filter(pl.col("est_nouveau"))
            .sort("mois_annee")
            .group_by("matricule")
            .agg(pl.col("mois_annee").first().alias("premier_mois"))
            .group_by("premier_mois")
            .agg(pl.len().alias("nb_nouveaux"))
            .sort("premier_mois")
        )
        print(f"\n  Répartition mensuelle des premières apparitions (nouveaux 2025) :")
        for row in premiere_apparition.iter_rows(named=True):
            print(f"    {row['premier_mois']} : {row['nb_nouveaux']:>6,} nouveaux")

        sauvegarder_csv(premiere_apparition, "diag_nouveaux_2025_par_mois.csv")

    del df25
    gc.collect()

# ============================================================
# SAUVEGARDE DES RAPPORTS
# ============================================================

print()
print("=" * 70)
print("SAUVEGARDE DES RAPPORTS")
print("=" * 70)
print()

sauvegarder_csv(
    pl.DataFrame({
        "periode_manquante": manquantes,
        "dans_bronze": [p in periodes_bronze for p in manquantes],
    }),
    "diag_periodes_manquantes.csv",
)

sauvegarder_csv(pl.DataFrame(rows_nouveaux), "diag_nouveaux_par_annee.csv")

# ============================================================
# RÉSUMÉ
# ============================================================

print()
print("=" * 70)
print("RÉSUMÉ DU DIAGNOSTIC")
print("=" * 70)
print()
print(f"1. Périodes manquantes     : {len(manquantes)}")
for p in manquantes:
    dans_b = "bronze ✓" if p in periodes_bronze else "bronze ✗ (jamais ingéré)"
    print(f"   {p} — {dans_b}")
print()
print(f"2. Nouveaux matricules 2025 : {len(nouveaux_2025):,}")
print(f"   Dont réapparus (absents 2024) : {len(reapparus_2025):,}")
print(f"   Dont vraiment nouveaux        : {len(nouveaux_2025) - len(reapparus_2025):,}")
print()
print("Rapports disponibles dans :")
print(f"  s3://{BUCKET_STAGING}/{PREFIX_EXPORTS}/diag_periodes_manquantes.csv")
print(f"  s3://{BUCKET_STAGING}/{PREFIX_EXPORTS}/diag_nouveaux_par_annee.csv")
print(f"  s3://{BUCKET_STAGING}/{PREFIX_EXPORTS}/diag_nouveaux_2025_par_organisme.csv")
print(f"  s3://{BUCKET_STAGING}/{PREFIX_EXPORTS}/diag_nouveaux_2025_par_mois.csv")