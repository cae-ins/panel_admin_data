# ============================================================
# PANEL ADMIN — ÉTAPE 7c : ANALYSE ÉVOLUTION INDIVIDUS HORS BARÈME
# ============================================================
# Pour chaque individu hors barème, récupère son historique complet
# dans le panel gold pour analyser l'évolution avant/après le mois
# où le salaire est hors barème.
#
# Pour chaque agent, on garde :
#   - Tous les mois de présence dans le panel
#   - Le mois_annee où il est détecté hors barème
#   - L'évolution du salaire brut vs le barème de référence
#
# Prérequis : 
#   - 07b_analyse_hors_bareme.py doit avoir été exécuté
#   - 06_compiler_panel.py doit avoir été exécuté (panel_complet.parquet)
#
# Source   : s3://gold/panel_admin/agents_hors_bareme.parquet
#           s3://gold/panel_admin/panel_complet.parquet
# Sortie  : s3://gold/panel_admin/hors_bareme_avec_historique.parquet
#           s3://staging/panel_admin/exports_gold/hors_bareme_evolution.xlsx
# ============================================================

import io
import os
import tempfile
from common.s3_client import create_s3_client
from common.logging_config import setup_pipeline_logging
import polars as pl
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment

# ============================================================
# CONFIGURATION
# ============================================================

BUCKET_GOLD    = os.getenv("BUCKET_GOLD",    "gold")
BUCKET_STAGING = os.getenv("BUCKET_STAGING", "staging")

KEY_HORS_BAREME      = "panel_admin/agents_hors_bareme.parquet"
KEY_PANEL_COMPLET   = "panel_admin/panel_complet.parquet"
OUTPUT_PQ           = "panel_admin/hors_bareme_avec_historique.parquet"
OUTPUT_XLSX         = "panel_admin/exports_gold/hors_bareme_evolution.xlsx"

# ============================================================
# CLIENT S3 & LOGGING
# ============================================================

s3 = create_s3_client()
logger, _ = setup_pipeline_logging("07c_analyse_evolution_hors_bareme")

logger.info("Démarrage de l'analyse évolution — individus hors barème")

# ============================================================
# 1. CHARGEMENT DES AGENTS HORS BARÈME
# ============================================================

logger.info(f"1. Chargement des agents hors barème (s3://{BUCKET_GOLD}/{KEY_HORS_BAREME})...")

buf_hb = io.BytesIO()
s3.download_fileobj(BUCKET_GOLD, KEY_HORS_BAREME, buf_hb)
buf_hb.seek(0)
hors_bareme = pl.read_parquet(buf_hb)

logger.info(f"   ✓ {len(hors_bareme):,} lignes chargées")
logger.info(f"   Colonnes disponibles : {hors_bareme.columns}")

# Colonnes clés nécessaires pour le matching
cols_cle = [c for c in ["matricule", "cle_unique", "mois_annee", "ANNEE", "MOIS"] 
            if c in hors_bareme.columns]
logger.info(f"   Clés identifiées : {cols_cle}")

if not cols_cle:
    logger.error("Colonnes de clé (matricule/mois_annee) absentes du fichier hors barème")
    raise RuntimeError("Colonnes de clé (matricule/mois_annee) absentes du fichier hors barème")

# Identifier la colonne salaire
col_salaire = next(
    (c for c in ["montant_brut_bareme", "SALAIRE_BRUT", "montant_brut"] 
     if c in hors_bareme.columns),
    None,
)
logger.info(f"   Colonne salaire : {col_salaire}")

# Identifier les colonnes pour filtrer à grade/organisme/emploi constants
cols_filtre = []
for col in ["GRADE", "CODE_GRADE", "CODE_ORGANISME", "organisme", "CODE_EMPLOI", "emploi"]:
    if col in hors_bareme.columns:
        cols_filtre.append(col)
logger.info(f"   Colonnes filtre (grade/organisme/emploi) : {cols_filtre}")

# ============================================================
# 2. EXTRACTION DES INDIVIDUS UNIQUES HORS BARÈME
# ============================================================

logger.info("2. Extraction des individus uniques...")

# Créer une clé d'identification individuelle
# Utiliser matricule comme clé principale
if "matricule" in hors_bareme.columns:
    identifiants_uniques = hors_bareme.select("matricule").unique().to_series().to_list()
    logger.info(f"   ✓ {len(identifiants_uniques):,} individus uniques identifiés par matricule")
elif "cle_unique" in hors_bareme.columns:
    identifiants_uniques = hors_bareme.select("cle_unique").unique().to_series().to_list()
    logger.info(f"   ✓ {len(identifiants_uniques):,} individus uniques identifiés par cle_unique")
else:
    logger.error("Ni matricule ni cle_unique disponibles")
    raise RuntimeError("Ni matricule ni cle_unique disponibles")

# ============================================================
# 3. CHARGEMENT DU PANEL COMPLET
# ============================================================

logger.info(f"3. Chargement du panel complet (s3://{BUCKET_GOLD}/{KEY_PANEL_COMPLET})...")

buf_panel = io.BytesIO()
s3.download_fileobj(BUCKET_GOLD, KEY_PANEL_COMPLET, buf_panel)
buf_panel.seek(0)
panel = pl.read_parquet(buf_panel)

logger.info(f"   ✓ {len(panel):,} lignes × {len(panel.columns)} colonnes")
logger.info(f"   Période : {panel['annee'].min()} - {panel['annee'].max()}")

# Vérifier les colonnes nécessaires dans le panel
cols_panel_dispos = panel.columns
logger.info(f"   Colonnes panel : {cols_panel_dispos[:15]}...")  # Afficher les 15 premières

# ============================================================
# 4. Pour chaque individu, récupérer son historique complet
# ============================================================

logger.info("4. Construction de l'historique pour chaque individu hors barème...")

# Définir la clé d'identification dans le panel
cle_panel = "matricule" if "matricule" in panel.columns else "cle_unique"
logger.info(f"   Clé utilisée dans le panel : {cle_panel}")

# Colonnes à garder du panel
cols_a_garder = [
    cle_panel,
    "mois_annee", "annee", "mois",
    "GRADE", "CODE_GRADE",
    "CODE_ORGANISME", "organisme",
    "CODE_EMPLOI", "emploi",
    "montant_brut_bareme", "montant_brut_total",
    "Statut" if "Statut" in panel.columns else None,
    "min_bareme", "max_bareme" if "max_bareme" in panel.columns else None,
    "GRADE_SOURCE", "montant_brut_SOURCE",
    "situation_normalisee",
]
cols_a_garder = [c for c in cols_a_garder if c is not None]

# Filtrer sur les colonnes existantes
cols_panel_final = [c for c in cols_a_garder if c in panel.columns]
logger.info(f"   Colonnes retenues pour l'historique : {cols_panel_final}")

# Créer le dataframe de résultat
historiques = []

# Pour chaque individu hors barème
for i, identifiant in enumerate(identifiants_uniques):
    if i % 1000 == 0:
        logger.info(f"   [{i}/{len(identifiants_uniques)}] Traitement...")
    
    # Récupérer l'historique complet de l'individu dans le panel
    histo = panel.filter(pl.col(cle_panel) == identifiant).select(cols_panel_final)
    
    if len(histo) == 0:
        continue
    
    # Ajouter le mois_annee où l'individu est détecté hors barème
    # (pour chaque ligne du panel, on vérifie si cette période est dans les données hors barème)
    hb_this_individu = hors_bareme.filter(pl.col(cle_panel) == identifiant)
    mois_hors_bareme = hb_this_individu.select("mois_annee").unique().to_series().to_list()
    
    # Ajouter une colonne indicating si ce mois est hors barème
    histo = histo.with_columns(
        pl.col("mois_annee").is_in(mois_hors_bareme).alias("est_hors_bareme")
    )
    
    # Ajouter aussi le statut spécifique pour ce mois (s'il est dans les données hb)
    # et les valeurs min/max barème
    if "min_bareme" in hb_this_individu.columns:
        # Pour chaque ligne, récupérer les valeurs min/max si disponible
        min_bareme_val = hb_this_individu.select("min_bareme").to_series().to_list()[0] if len(hb_this_individu) > 0 else None
        max_bareme_val = hb_this_individu.select("max_bareme").to_series().to_list()[0] if len(hb_this_individu) > 0 else None
        statut_val = hb_this_individu.select("Statut").to_series().to_list()[0] if len(hb_this_individu) > 0 else None
        
        histo = histo.with_columns([
            pl.lit(min_bareme_val).alias("min_bareme_ref") if "min_bareme" not in histo.columns else pl.col("min_bareme"),
            pl.lit(max_bareme_val).alias("max_bareme_ref") if "max_bareme" not in histo.columns else pl.col("max_bareme"),
            pl.lit(statut_val).alias("Statut_hors_bareme") if "Statut" not in histo.columns else pl.col("Statut"),
        ])
    
    historiques.append(histo)

# Concaténer tous les historiques
logger.info("   Concaténation des historiques...")
if historiques:
    panel_evol = pl.concat(historiques, how="diagonal")
else:
    panel_evol = pl.DataFrame()

logger.info(f"   ✓ {len(panel_evol):,} lignes dans l'historique complet")

# ============================================================
# 5. Tri chronologique et calcul des deltas
# ============================================================

logger.info("5. Tri chronologique et calculs complémentaires...")

if len(panel_evol) > 0 and "annee" in panel_evol.columns and "mois" in panel_evol.columns:
    # Créer une colonne période pour le tri (YYYYMM)
    panel_evol = panel_evol.with_columns(
        (pl.col("annee") * 100 + pl.col("mois")).alias("periode_tri")
    )
    
    # Trier par individu puis par période
    panel_evol = panel_evol.sort([cle_panel, "periode_tri"])
    
    # Calculer le delta salaire vs mois précédent (pour le même individu)
    panel_evol = panel_evol.with_columns(
        pl.col("montant_brut_bareme")
          .diff()
          .over(cle_panel)
          .alias("delta_salaire_vs_mois_prec")
    )
    
    # Calculer la position par rapport au barème (si min/max dispo)
    if "min_bareme_ref" in panel_evol.columns and "max_bareme_ref" in panel_evol.columns:
        panel_evol = panel_evol.with_columns([
            (pl.col("montant_brut_bareme") - pl.col("min_bareme_ref")).alias("ecart_min"),
            (pl.col("montant_brut_bareme") - pl.col("max_bareme_ref")).alias("ecart_max"),
        ])
    
    logger.info(f"   ✓ Tri et calculs terminés")

# ============================================================
# 6. Export Parquet vers Gold
# ============================================================

logger.info("6. Export Parquet vers Gold...")

buf_out = io.BytesIO()
panel_evol.write_parquet(buf_out, compression="snappy")
buf_out.seek(0)
s3.put_object(Bucket=BUCKET_GOLD, Key=OUTPUT_PQ, Body=buf_out.getvalue())

taille_mb = len(buf_out.getvalue()) / 1024**2
logger.info(f"   ✓ s3://{BUCKET_GOLD}/{OUTPUT_PQ} ({taille_mb:.1f} MB)")

# ============================================================
# 7. Export Excel résumée (échantillon)
# ============================================================

logger.info("7. Export Excel résumée...")

# Pour l'Excel, on va créer un résumé par individu avec :
# -première apparition, dernière apparition, nombre de mois
# -situation par rapport au barème (avant/pendant/après)

def creer_resume_individus(df: pl.DataFrame, cle: str) -> pl.DataFrame:
    """Crée un résumé par individu de son historique."""
    
    resume_cols = [cle, "annee", "mois", "GRADE", "CODE_ORGANISME", "organisme", 
                   "CODE_EMPLOI", "emploi", "montant_brut_bareme", "est_hors_bareme"]
    resume_cols = [c for c in resume_cols if c in df.columns]
    
    # Grouper par individu
    resume = (
        df.group_by(cle)
        .agg([
            pl.col("annee").min().alias("annee_premiere_observation"),
            pl.col("annee").max().alias("annee_derniere_observation"),
            pl.col("mois").count().alias("nb_mois_observables"),
            pl.col("est_hors_bareme").sum().alias("nb_mois_hors_bareme"),
            pl.col("montant_brut_bareme").mean().alias("moyenne_salaire"),
            pl.col("montant_brut_bareme").min().alias("min_salaire"),
            pl.col("montant_brut_bareme").max().alias("max_salaire"),
        ])
    )
    
    return resume

resume = creer_resume_individus(panel_evol, cle_panel)
logger.info(f"   ✓ {len(resume):,} individus dans le résumé")

# Créer l'Excel avec plusieurs feuilles
wb = Workbook()
wb.remove(wb.active)

# Feuille 1 : Résumé par individu
ws1 = wb.create_sheet("RESUME_INDIVIDUS")
for j, col in enumerate(resume.columns, start=1):
    ws1.cell(row=1, column=j, value=col)
for i, row in enumerate(resume.iter_rows(named=True), start=2):
    for j, col in enumerate(resume.columns, start=1):
        ws1.cell(row=i, column=j, value=row[col])

# Feuille 2 : Détail (limité à 10000 lignes)
ws2 = wb.create_sheet("DETAIL_HISTORIQUE")
cols_detail = [c for c in [cle_panel, "mois_annee", "annee", "mois", "GRADE", 
                           "CODE_ORGANISME", "organisme", "CODE_EMPLOI", "emploi",
                           "montant_brut_bareme", "delta_salaire_vs_mois_prec",
                           "est_hors_bareme", "min_bareme_ref", "max_bareme_ref"]
                if c in panel_evol.columns]

df_detail = panel_evol.head(10000)  # Limiter pour Excel

for j, col in enumerate(cols_detail, start=1):
    ws2.cell(row=1, column=j, value=col)
for i, row in enumerate(df_detail.iter_rows(named=True), start=2):
    for j, col in enumerate(cols_detail, start=1):
        ws2.cell(row=i, column=j, value=row[col])

# Feuille 3 : Statistiques globales
ws3 = wb.create_sheet("STATISTIQUES")
ws3.cell(row=1, column=1, value="Métrique")
ws3.cell(row=1, column=2, value="Valeur")

stats = [
    ("Nombre d'individus analysés", len(resume)),
    ("Total lignes historique", len(panel_evol)),
    ("Mois avec hors barème", int(panel_evol.filter(pl.col("est_hors_bareme") == True).len())),
    ("Pourcentage hors barème", f"{100 * panel_evol.filter(pl.col('est_hors_bareme') == True).len() / len(panel_evol):.1f}%"),
]

for i, (metrique, valeur) in enumerate(stats, start=2):
    ws3.cell(row=i, column=1, value=metrique)
    ws3.cell(row=i, column=2, value=valeur)

# Sauvegarde Excel
buf_xlsx = io.BytesIO()
wb.save(buf_xlsx)
buf_xlsx.seek(0)
xlsx_bytes = buf_xlsx.getvalue()

s3.put_object(
    Bucket=BUCKET_STAGING,
    Key=OUTPUT_XLSX,
    Body=xlsx_bytes,
    ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

taille_xlsx = len(xlsx_bytes) / 1024
logger.info(f"   ✓ s3://{BUCKET_STAGING}/{OUTPUT_XLSX} ({taille_xlsx:.1f} KB)")

# ============================================================
# RÉSUMÉ
# ============================================================

logger.info("✓ ANALYSE ÉVOLUTION HORS BARÈME TERMINÉE")
logger.info(f"  • Parquet (complet) : s3://{BUCKET_GOLD}/{OUTPUT_PQ}")
logger.info(f"  • Excel (résumé)    : s3://{BUCKET_STAGING}/{OUTPUT_XLSX}")
logger.info("Colonnes disponibles dans le fichier Parquet :")
for col in panel_evol.columns:
    logger.info(f"  • {col}")
logger.info("Vous pouvez maintenant analyser l'évolution des individus avant et après leur mois de détection hors barème.")

