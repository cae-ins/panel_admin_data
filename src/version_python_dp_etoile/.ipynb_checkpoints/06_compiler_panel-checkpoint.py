# ============================================================
# PANEL ADMIN — ÉTAPE 6 : COMPILATION DU PANEL INDIVIDUS
# ============================================================
# Produit un fichier panel au format long :
#   une ligne = un individu × une période
#
# Colonnes produites :
#   — Identifiants individuels
#       cle_unique, matricule, nom, sexe, date_naissance,
#       situation_matrimoniale, nombre_enfant
#   — Position administrative
#       situation_normalisee,
#       CODE_ORGANISME, organisme,
#       CODE_EMPLOI,    emploi,
#       CODE_GRADE,     grade,  statut_fonctionnaire,
#       CODE_FONCTION,  fonction,
#       CODE_SERVICE,   service,
#       CODE_AFFECTATION, lieu_affectation,
#       CODE_POSTE,     poste,
#       prise_service, date_retraite, age_retraite
#   — Grade normalisé (03b)
#       GRADE, GRADE_SOURCE
#   — Dimensions temporelles
#       mois_annee (String), annee (Int32), mois (Int32)
#   — Salaires mensuels
#       montant_brut, montant_net,
#       retenue_pension, impot, charge_patronale
#   — Traçabilité imputation salaires (03c)
#       montant_brut_SOURCE, montant_net_SOURCE
#   — Traçabilité fichier
#       fichier_source
#
# Source  : s3://silver/panel_admin/YYYY.parquet (un par année)
# Sortie  : s3://gold/panel_admin/panel_YYYY.parquet (un par année)
#
# Architecture : Polars uniquement, traitement année par année.
#
# Dépendances :
#   pip install polars boto3 python-dotenv
# ============================================================

import gc
import io
import os
import re
import tempfile
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import polars as pl

load_dotenv(".env")

# --- CONFIGURATION ---

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "datalab-team")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio-datalabteam123")

BUCKET_SILVER  = os.getenv("BUCKET_SILVER", "silver")
BUCKET_GOLD    = os.getenv("BUCKET_GOLD",   "gold")
PREFIX_SILVER  = "panel_admin"
PREFIX_PANEL   = "panel_admin"

# --- CLIENT S3 (MinIO) ---
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
# COLONNES RETENUES DANS LE PANEL
# ============================================================
# Ordre logique : identité → position → grade → temps → salaires → trace
# La sélection est DÉFENSIVE : les colonnes absentes du Silver
# sont ignorées silencieusement (cols_presentes ci-dessous).
# Pour ajouter une colonne : l'inscrire ici suffit.

COLONNES_PANEL = [
    # --- Identité individuelle ---
    "cle_unique",            # matricule || CODE_ORGANISME (03b)
    "matricule",
    "nom",
    "sexe",
    "date_naissance",
    "situation_matrimoniale",
    "nombre_enfant",

    # --- Position administrative ---
    "situation_normalisee",
    "CODE_ORGANISME",    "organisme",
    "CODE_EMPLOI",       "emploi",
    "Code_CITP",         "Metier_CITP",
    "CODE_GRADE",        "grade",    "statut_fonctionnaire",
    "CODE_FONCTION",     "fonction",
    "CODE_SERVICE",      "service",
    "CODE_AFFECTATION",  "lieu_affectation",
    "CODE_POSTE",
    "prise_service",
    "date_retraite",
    "age_retraite",

    # --- Grade normalisé (03b_imputer_grades_silver) ---
    "GRADE",             # grade normalisé utilisé pour l'imputation
    "GRADE_SOURCE",      # OBSERVE | IMPUTE_Ln | NON_IMPUTABLE

    # --- Dimensions temporelles ---
    "mois_annee",        # String brut du Silver (MMYYYY)
    "annee",             # Int32 extrait
    "mois",              # Int32 extrait

    # --- Salaires mensuels ---
    "montant_brut",
    "montant_net",
    "retenue_pension",
    "impot",
    "charge_patronale",

    # --- Traçabilité imputation salaires (03c_imputer_salaires_silver) ---
    "montant_brut_SOURCE",   # OBSERVE | LOCF_FWD | LOCF_BWD | IMPUTE_L1..L4 | NON_IMPUTABLE
    "montant_net_SOURCE",    # idem

    # --- Traçabilité fichier ---
    "fichier_source",
]

# ============================================================
# UTILITAIRES
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
    print(f"  ✓ s3://{bucket}/{key}  ({len(df):,} lignes × {len(df.columns)} cols  {taille:.1f} MB)")


def construire_panel_annee(df: pl.DataFrame) -> pl.DataFrame:
    """
    Applique sur un DataFrame Silver annuel :
      - extraction annee/mois depuis mois_annee
      - sélection et réordonnancement des colonnes
      - déduplication matricule × mois_annee
      - tri matricule → annee → mois
    """

    # Extraction annee et mois depuis mois_annee (format MMYYYY)
    if "mois_annee" in df.columns:
        df = df.with_columns([
            pl.col("mois_annee")
              .str.replace_all(r"[_/\.]", "-")
              .alias("mois_annee"),
        ])
        df = df.with_columns([
            pl.when(pl.col("mois_annee").str.len_chars() == 6)
              .then(pl.col("mois_annee").str.slice(2, 4).cast(pl.Int32, strict=False))
              .otherwise(pl.col("mois_annee").str.slice(0, 4).cast(pl.Int32, strict=False))
              .alias("annee"),

            pl.when(pl.col("mois_annee").str.len_chars() == 6)
              .then(pl.col("mois_annee").str.slice(0, 2).cast(pl.Int32, strict=False))
              .otherwise(pl.col("mois_annee").str.slice(5, 2).cast(pl.Int32, strict=False))
              .alias("mois"),
        ])
    else:
        df = df.with_columns([
            pl.lit(None).cast(pl.Int32).alias("annee"),
            pl.lit(None).cast(pl.Int32).alias("mois"),
        ])

    # Sélection défensive : colonnes de COLONNES_PANEL présentes dans df
    cols_presentes = [c for c in COLONNES_PANEL if c in df.columns]
    cols_absentes  = [c for c in COLONNES_PANEL if c not in df.columns]
    df = df.select(cols_presentes)

    if cols_absentes:
        print(f"    ⚠️  Colonnes absentes du Silver (ignorées) : {cols_absentes}", flush=True)

    # Déduplication matricule × mois_annee (garde la première occurrence)
    cles_dedup = [c for c in ["matricule", "mois_annee"] if c in df.columns]
    if cles_dedup:
        avant = len(df)
        df    = df.unique(subset=cles_dedup, keep="first", maintain_order=True)
        apres = len(df)
        if avant != apres:
            print(f"    ⚠️  {avant - apres:,} doublons supprimés", flush=True)

    # Tri individu → chronologique
    tri_cols = [c for c in ["matricule", "annee", "mois"] if c in df.columns]
    df = df.sort(tri_cols, nulls_last=True)

    return df


# ============================================================
# LISTER LES FICHIERS SILVER
# ============================================================

print("=" * 70)
print("COMPILATION DU PANEL INDIVIDUS")
print("=" * 70)
print()

pages_silver = s3.get_paginator("list_objects_v2").paginate(
    Bucket=BUCKET_SILVER, Prefix=PREFIX_SILVER
)
cles_silver = sorted([
    obj["Key"]
    for page in pages_silver
    for obj in page.get("Contents", [])
    if obj["Key"].endswith(".parquet")
])

print(f"Fichiers Silver trouvés : {len(cles_silver)}\n")

# ============================================================
# TRAITEMENT ANNÉE PAR ANNÉE
# ============================================================

nb_total_lignes = 0
nb_total_agents = set()
periodes_vues   = []
stats_par_annee = []

for cle in cles_silver:
    annee_str = re.search(r"20\d{2}", os.path.basename(cle))
    annee_str = annee_str.group() if annee_str else os.path.splitext(os.path.basename(cle))[0]

    print(f"[{annee_str}] Lecture...", end=" ", flush=True)
    df = lire_parquet_s3(BUCKET_SILVER, cle)
    print(f"✓  {len(df):,} lignes × {len(df.columns)} cols", flush=True)

    df_panel = construire_panel_annee(df)
    del df
    gc.collect()

    nb_lignes = len(df_panel)
    print(f"    Panel : {nb_lignes:,} lignes × {len(df_panel.columns)} cols", flush=True)

    # Vérification explicite des colonnes critiques
    for col_critique in ["GRADE", "GRADE_SOURCE", "montant_brut_SOURCE", "montant_net_SOURCE"]:
        statut = "✓" if col_critique in df_panel.columns else "✗ ABSENT"
        print(f"    {col_critique:<25} : {statut}", flush=True)

    # Écriture Gold annuelle
    key_panel = f"{PREFIX_PANEL}/panel_{annee_str}.parquet"
    ecrire_parquet_s3(df_panel, BUCKET_GOLD, key_panel)

    # Accumulation stats
    nb_total_lignes += nb_lignes
    if "matricule" in df_panel.columns:
        nb_total_agents.update(df_panel["matricule"].drop_nulls().to_list())
    if "mois_annee" in df_panel.columns:
        periodes_vues.extend(df_panel["mois_annee"].drop_nulls().unique().to_list())

    row_stat = {"annee": annee_str, "nb_obs": nb_lignes}
    if "matricule" in df_panel.columns:
        row_stat["nb_individus"] = df_panel["matricule"].n_unique()
    if "montant_brut" in df_panel.columns:
        brut = df_panel["montant_brut"].drop_nulls()
        row_stat["brut_moyen"]   = round(brut.mean(),   0) if len(brut) > 0 else None
        row_stat["brut_mediane"] = round(brut.median(), 0) if len(brut) > 0 else None
    stats_par_annee.append(row_stat)

    del df_panel
    gc.collect()

# ============================================================
# RÉSUMÉ FINAL
# ============================================================

print()
print("=" * 70)
print("APERÇU DU PANEL")
print("=" * 70)
print()
print(f"Observations totales  : {nb_total_lignes:,}")
print(f"Individus uniques     : {len(nb_total_agents):,}")
if periodes_vues:
    print(f"Plage temporelle      : {min(periodes_vues)} → {max(periodes_vues)}")
    print(f"Périodes distinctes   : {len(set(periodes_vues))}")

print(f"\nDétail par année :")
for row in stats_par_annee:
    ligne = f"  {row['annee']}  :  {row.get('nb_obs', 0):>8,} obs"
    if "nb_individus" in row:
        ligne += f"  |  {row['nb_individus']:>6,} individus"
    if "brut_moyen" in row and row["brut_moyen"]:
        ligne += f"  |  brut moy = {row['brut_moyen']:,.0f}"
    print(ligne)

print()
print("=" * 70)
print("✓ PANEL INDIVIDUS TERMINÉ")
print("=" * 70)
print(f"\nFichiers disponibles dans : s3://{BUCKET_GOLD}/{PREFIX_PANEL}/")
print()
print("Prochaine étape : exécuter 07_calcul_indicateur.py")

# ============================================================
# FUSION COMPLÈTE : tous les fichiers annuels → panel_complet
# ============================================================

print()
print("=" * 70)
print("FUSION : panel_complet.parquet (toutes années)")
print("=" * 70)
print()

KEY_PANEL_COMPLET = f"{PREFIX_PANEL}/panel_complet.parquet"

pages_panel = s3.get_paginator("list_objects_v2").paginate(
    Bucket=BUCKET_GOLD, Prefix=PREFIX_PANEL
)
cles_panel = sorted([
    obj["Key"]
    for page in pages_panel
    for obj in page.get("Contents", [])
    if re.search(r"panel_20\d{2}\.parquet$", obj["Key"])
])

print(f"Fichiers panel annuels trouvés : {len(cles_panel)}")
for cle in cles_panel:
    print(f"  · {os.path.basename(cle)}")
print()

with tempfile.TemporaryDirectory() as tmpdir:
    chemins_locaux = []
    for cle in cles_panel:
        dest = os.path.join(tmpdir, os.path.basename(cle))
        print(f"  Téléchargement {os.path.basename(cle)}...", end=" ", flush=True)
        s3.download_file(BUCKET_GOLD, cle, dest)
        chemins_locaux.append(dest)
        print("✓", flush=True)

    print(f"\nFusion en streaming...", flush=True)
    chemin_complet = os.path.join(tmpdir, "panel_complet.parquet")

    (
        pl.scan_parquet(chemins_locaux)
        .sort(["matricule", "annee", "mois"], nulls_last=True)
        .sink_parquet(chemin_complet, compression="snappy")
    )

    print(f"Upload vers s3://{BUCKET_GOLD}/{KEY_PANEL_COMPLET}...", end=" ", flush=True)
    s3.upload_file(chemin_complet, BUCKET_GOLD, KEY_PANEL_COMPLET)
    taille = os.path.getsize(chemin_complet) / 1024**2
    print(f"✓  {taille:.1f} MB")

print()
print("=" * 70)
print("✓ PANEL COMPLET ÉCRIT")
print("=" * 70)
print(f"\n  s3://{BUCKET_GOLD}/{KEY_PANEL_COMPLET}")
print(f"  {nb_total_lignes:,} lignes  ·  {len(nb_total_agents):,} individus uniques")