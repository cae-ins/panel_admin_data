# ============================================================
# PANEL ADMIN — ÉTAPE 7b : ANALYSE DES AGENTS HORS-BARÈME
# ============================================================
# Lit base_avec_statut.parquet (produit par 07_calcul_indicateur.py)
# et produit un fichier Excel d'analyse des agents hors-barème
# (Inférieur au minimum / Supérieur au maximum du barème).
#
# Feuilles produites :
#   RESUME_GLOBAL          : Effectif total par statut + proportions
#   PAR_SITUATION          : Hors-barème ventilé par situation administrative
#   PAR_SECTEUR            : Hors-barème ventilé par secteur (barème)
#   PAR_GRADE              : Hors-barème ventilé par grade
#   PAR_EMPLOI             : Hors-barème ventilé par emploi × secteur (top 50)
#   DETAIL_SIT_GRADE       : Croisement situation × grade
#   DETAIL_SIT_EMPLOI      : Croisement situation × secteur × emploi
#   DETAIL_COMPLET         : Détail complet situation × secteur × emploi × grade + montants
#   COHERENCE_GRADE        : Transitions de grade anormales (sauts, régressions)
#   COHERENCE_GRADE_RESUME : Résumé des anomalies par type et amplitude
#
# Prérequis : 07_calcul_indicateur.py doit avoir été exécuté.
#
# Source  : s3://gold/panel_admin/base_avec_statut.parquet
# Sortie  : s3://staging/panel_admin/exports_gold/analyse_hors_bareme.xlsx
# ============================================================

import io
import os
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import polars as pl
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter

load_dotenv(".env")

# ============================================================
# CONNEXION S3 / MINIO
# ============================================================

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "datalab-team")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio-datalabteam123")

BUCKET_GOLD    = os.getenv("BUCKET_GOLD",    "gold")
BUCKET_STAGING = os.getenv("BUCKET_STAGING", "staging")

KEY_INPUT  = "panel_admin/base_avec_statut.parquet"
KEY_OUTPUT = "panel_admin/exports_gold/analyse_hors_bareme.xlsx"

s3 = boto3.client(
    "s3",
    endpoint_url      = MINIO_ENDPOINT,
    aws_access_key_id = MINIO_ACCESS_KEY,
    aws_secret_access_key = MINIO_SECRET_KEY,
    config = Config(signature_version="s3v4", connect_timeout=30, read_timeout=120),
    region_name = "us-east-1",
    verify      = False,
)

print("\n" + "=" * 70)
print("ANALYSE DES AGENTS HORS-BARÈME")
print("=" * 70 + "\n")

# ============================================================
# CHARGEMENT
# ============================================================

print(f"Chargement : s3://{BUCKET_GOLD}/{KEY_INPUT}")
buf = io.BytesIO()
s3.download_fileobj(BUCKET_GOLD, KEY_INPUT, buf)
buf.seek(0)
base = pl.read_parquet(buf)
print(f"  ✓ {len(base):,} lignes × {len(base.columns)} colonnes\n")

if "Statut" not in base.columns:
    raise ValueError("Colonne 'Statut' absente — relancer 07_calcul_indicateur.py")

# ============================================================
# PÉRIMÈTRES
# ============================================================

STATUTS_HORS = ["Inférieur au minimum du barème", "Supérieur au maximum du barème"]

total_base  = len(base)
hors        = base.filter(pl.col("Statut").is_in(STATUTS_HORS))
total_hors  = len(hors)

print(f"Distribution Statut (base complète : {total_base:,}) :")
for row in base["Statut"].value_counts().sort("Statut").iter_rows(named=True):
    pct = 100 * row["count"] / total_base
    print(f"  {row['Statut']:<45} : {row['count']:>10,}  ({pct:.1f}%)")
print(f"\nAgents hors-barème retenus pour l'analyse : {total_hors:,}\n")

# ============================================================
# FONCTIONS UTILITAIRES EXCEL
# ============================================================

FILL_TITRE   = PatternFill("solid", fgColor="1F4E78")
FILL_HEADER  = PatternFill("solid", fgColor="4472C4")
FILL_INFOS   = PatternFill("solid", fgColor="D9E1F2")
FONT_TITRE   = Font(color="FFFFFF", bold=True, size=13)
FONT_HEADER  = Font(color="FFFFFF", bold=True, size=10)
FONT_NORMAL  = Font(size=10)
ALIGN_CENTER = Alignment(horizontal="center", vertical="center", wrap_text=True)
ALIGN_LEFT   = Alignment(horizontal="left",   vertical="center")


def ecrire_feuille(wb: Workbook, nom: str, df: pl.DataFrame, titre: str = "") -> None:
    ws = wb.create_sheet(nom)
    start = 1
    if titre:
        ws.merge_cells(f"A1:{get_column_letter(len(df.columns))}1")
        c = ws.cell(row=1, column=1, value=titre)
        c.fill = FILL_TITRE
        c.font = FONT_TITRE
        c.alignment = ALIGN_CENTER
        ws.row_dimensions[1].height = 22
        start = 2
    # En-têtes
    for j, col in enumerate(df.columns, 1):
        c = ws.cell(row=start, column=j, value=col)
        c.fill = FILL_HEADER
        c.font = FONT_HEADER
        c.alignment = ALIGN_CENTER
    ws.row_dimensions[start].height = 18
    # Données
    for i, row in enumerate(df.iter_rows(named=True), start + 1):
        for j, col in enumerate(df.columns, 1):
            ws.cell(row=i, column=j, value=row[col])
    # Largeurs automatiques
    for j, col in enumerate(df.columns, 1):
        max_len = max(len(str(col)), 10)
        ws.column_dimensions[get_column_letter(j)].width = min(max_len + 2, 40)
    ws.freeze_panes = f"A{start + 1}"
    print(f"  ✓ {nom} ({len(df):,} lignes)")


def stats_ventilees(df: pl.DataFrame, cols_group: list[str],
                    total: int) -> pl.DataFrame:
    """Effectif + proportion sur le total hors-barème, par groupe."""
    return (
        df.group_by(cols_group)
        .agg(pl.len().alias("Effectif"))
        .with_columns(
            (pl.col("Effectif") * 100 / total).round(2).alias("Proportion (%)")
        )
        .sort(cols_group)
    )


# ============================================================
# PRODUCTION DES TABLEAUX
# ============================================================

print("Production des tableaux...")

wb = Workbook()
wb.remove(wb.active)

# ── 1. Résumé global ────────────────────────────────────────
resume_global = (
    base
    .group_by("Statut")
    .agg(pl.len().alias("Effectif"))
    .with_columns(
        (pl.col("Effectif") * 100 / total_base).round(2).alias("Proportion base (%)")
    )
    .sort("Statut")
)
ecrire_feuille(wb, "RESUME_GLOBAL", resume_global,
               titre="Résumé global — Distribution du statut barème")

# ── 1b. HORS_BAREME_Detail : situation × emploi × grade ─────
cols_hors_det = [c for c in ["situation_normalisee", "emploi", "GRADE"]
                 if c in hors.columns]
if cols_hors_det:
    hors_detail = (
        hors
        .group_by(["Statut"] + cols_hors_det)
        .agg(pl.len().alias("Effectif"))
        .with_columns(
            (pl.col("Effectif") * 100 / total_hors).round(2).alias("Proportion (%)")
        )
        .sort(["Statut"] + cols_hors_det)
    )
    ecrire_feuille(wb, "HORS_BAREME_Detail", hors_detail,
                   titre="Hors-barème : situation × emploi × grade")

# ── 2. Par situation ────────────────────────────────────────
if "situation_normalisee" in hors.columns:
    par_sit = stats_ventilees(hors, ["Statut", "situation_normalisee"], total_hors)
    ecrire_feuille(wb, "PAR_SITUATION", par_sit,
                   titre="Hors-barème par situation administrative")

# ── 3. Par grade ────────────────────────────────────────────
if "GRADE" in hors.columns:
    par_grade = stats_ventilees(hors, ["Statut", "GRADE"], total_hors)
    ecrire_feuille(wb, "PAR_GRADE", par_grade,
                   titre="Hors-barème par grade")

# ── 4. Par secteur (bareme) ─────────────────────────────────
if "bareme" in hors.columns:
    par_secteur = stats_ventilees(hors, ["Statut", "bareme"], total_hors)
    ecrire_feuille(wb, "PAR_SECTEUR", par_secteur,
                   titre="Hors-barème par secteur (barème)")

# ── 5. Par emploi × secteur (top 50 effectifs) ──────────────
cols_emploi_secteur = [c for c in ["Statut", "bareme", "emploi"] if c in hors.columns]
if len(cols_emploi_secteur) > 1:
    par_emploi_all = stats_ventilees(hors, cols_emploi_secteur, total_hors)
    par_emploi = par_emploi_all.sort("Effectif", descending=True).head(50)
    ecrire_feuille(wb, "PAR_EMPLOI", par_emploi,
                   titre="Hors-barème par emploi × secteur (top 50 effectifs)")

# ── 6. Croisement situation × grade ─────────────────────────
cols_sit_grade = [c for c in ["Statut", "situation_normalisee", "GRADE"]
                  if c in hors.columns]
if len(cols_sit_grade) > 1:
    detail_sit_grade = stats_ventilees(hors, cols_sit_grade, total_hors)
    ecrire_feuille(wb, "DETAIL_SIT_GRADE", detail_sit_grade,
                   titre="Hors-barème : situation × grade")

# ── 7. Croisement situation × secteur × emploi ──────────────
cols_sit_emploi = [c for c in ["Statut", "situation_normalisee", "bareme", "emploi"]
                   if c in hors.columns]
if len(cols_sit_emploi) > 1:
    detail_sit_emploi = (
        stats_ventilees(hors, cols_sit_emploi, total_hors)
        .sort("Effectif", descending=True)
    )
    ecrire_feuille(wb, "DETAIL_SIT_EMPLOI", detail_sit_emploi,
                   titre="Hors-barème : situation × secteur × emploi")

# ── 8. Détail complet avec montants ─────────────────────────
cols_detail = [c for c in ["Statut", "situation_normalisee", "bareme", "emploi", "GRADE"]
               if c in hors.columns]
if cols_detail:
    agg_exprs = [pl.len().alias("Effectif")]
    if "montant_brut_bareme" in hors.columns:
        agg_exprs += [
            pl.col("montant_brut_bareme").min().round(0).alias("Revenu min"),
            pl.col("montant_brut_bareme").max().round(0).alias("Revenu max"),
            pl.col("montant_brut_bareme").mean().round(0).alias("Revenu moyen"),
            pl.col("montant_brut_bareme").median().round(0).alias("Revenu médian"),
        ]
    if "min_bareme" in hors.columns:
        agg_exprs.append(pl.col("min_bareme").first().round(0).alias("Barème min"))
    if "max_bareme" in hors.columns:
        agg_exprs.append(pl.col("max_bareme").first().round(0).alias("Barème max"))

    detail_complet = (
        hors
        .group_by(cols_detail)
        .agg(agg_exprs)
        .with_columns(
            (pl.col("Effectif") * 100 / total_hors).round(2).alias("Proportion (%)")
        )
        .sort(cols_detail)
    )
    ecrire_feuille(wb, "DETAIL_COMPLET", detail_complet,
                   titre="Hors-barème : détail complet avec montants")

# ── 9. Cohérence progression des grades ─────────────────────
# Un agent ne peut pas sauter de grade (ex : A4 → A7 sans A5, A6)
# Ordre numérique au sein de chaque série (A, B, C, D)
ORDRE_GRADE = {g: i for i, g in enumerate(
    [f"A{i}" for i in range(1, 8)] +
    [f"B{i}" for i in range(1, 7)] +
    [f"C{i}" for i in range(1, 6)] +
    [f"D{i}" for i in range(1, 4)]
)}

cols_prog = [c for c in ["matricule", "mois_annee", "annee", "mois", "GRADE"]
             if c in hors.columns]

if "matricule" in hors.columns and "GRADE" in hors.columns:
    # Trier par agent et par période
    tri_col = "mois_annee" if "mois_annee" in hors.columns else "annee"
    prog = (
        hors
        .select(cols_prog)
        .filter(pl.col("GRADE").is_in(list(ORDRE_GRADE.keys())))
        .with_columns(
            pl.col("GRADE").replace(ORDRE_GRADE, default=None).cast(pl.Int32).alias("_rang")
        )
        .sort(["matricule", tri_col])
    )

    # Calculer le grade précédent par agent
    prog = prog.with_columns([
        pl.col("GRADE").shift(1).over("matricule").alias("grade_precedent"),
        pl.col("_rang").shift(1).over("matricule").alias("_rang_precedent"),
        pl.col(tri_col).shift(1).over("matricule").alias("periode_precedente"),
    ])

    # Conserver uniquement les transitions (exclure la première obs de chaque agent)
    prog = prog.filter(pl.col("grade_precedent").is_not_null())

    # Calculer l'amplitude du saut (positif = progression, négatif = régression)
    prog = prog.with_columns(
        (pl.col("_rang") - pl.col("_rang_precedent")).alias("saut")
    )

    # Identifier les anomalies : saut > 1 ou saut < 0
    anomalies = prog.filter(
        (pl.col("saut") > 1) | (pl.col("saut") < 0)
    ).select([
        "matricule",
        pl.col("periode_precedente").alias("periode_de"),
        pl.col(tri_col).alias("periode_a"),
        pl.col("grade_precedent").alias("grade_de"),
        pl.col("GRADE").alias("grade_a"),
        pl.col("saut").alias("amplitude_saut"),
    ]).with_columns(
        pl.when(pl.col("amplitude_saut") > 1)
        .then(pl.lit("Saut en avant"))
        .otherwise(pl.lit("Régression"))
        .alias("type_anomalie")
    ).sort(["type_anomalie", "amplitude_saut"])

    if len(anomalies) > 0:
        ecrire_feuille(wb, "COHERENCE_GRADE", anomalies,
                       titre=f"Cohérence progression grades — {len(anomalies):,} anomalies détectées")

        # Résumé par type et amplitude
        resume_anom = (
            anomalies
            .group_by(["type_anomalie", "grade_de", "grade_a", "amplitude_saut"])
            .agg(pl.len().alias("Nb agents"))
            .sort(["type_anomalie", "amplitude_saut"])
        )
        ecrire_feuille(wb, "COHERENCE_GRADE_RESUME", resume_anom,
                       titre="Résumé anomalies progression grades")

        print(f"   ✓ COHERENCE_GRADE : {len(anomalies):,} transitions anormales détectées")
    else:
        print("   ✓ COHERENCE_GRADE : aucune anomalie de progression détectée")

# ============================================================
# EXPORT
# ============================================================

buf_out = io.BytesIO()
wb.save(buf_out)
buf_out.seek(0)

s3.put_object(
    Bucket=BUCKET_STAGING,
    Key=KEY_OUTPUT,
    Body=buf_out.getvalue(),
    ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

taille_kb = len(buf_out.getvalue()) / 1024
print(f"\n✅ Fichier créé : s3://{BUCKET_STAGING}/{KEY_OUTPUT}")
print(f"   Taille   : {taille_kb:.1f} KB")
print(f"   Feuilles : {len(wb.sheetnames)}\n")
print("=" * 70)
print("✓ ANALYSE HORS-BARÈME TERMINÉE")
print("=" * 70)
