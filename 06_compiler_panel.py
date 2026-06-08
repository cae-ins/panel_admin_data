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

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

BUCKET_SILVER  = os.getenv("BUCKET_SILVER", "silver")
BUCKET_GOLD    = os.getenv("BUCKET_GOLD",   "gold")
PREFIX_SILVER  = "panel_admin"
PREFIX_PANEL   = "panel_admin"

# Noms des colonnes F2 (composantes barème issues de la Feuille 2 du fichier source)
# Recherche insensible à la casse : 124_F2 ou 124_f2 selon la normalisation Bronze
NOMS_F2 = {"124_F2", "228_F2", "271_F2", "421_F2", "425_F2"}

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

    # --- Salaires mensuels (ligne individuelle) ---
    "montant_brut",
    "montant_net",
    "retenue_pension",
    "impot",
    "charge_patronale",

    # --- Salaires agrégés multi-postes (calculés dans construire_panel_annee) ---
    "NB_POSTES",             # Nombre de postes de l'individu × mois
    "montant_brut_total",    # Somme montant_brut sur TOUS les postes
    "montant_brut_bareme",   # Somme montant_brut postes {124,228,271,421,425} uniquement
    "montant_net_total",     # Somme montant_net sur TOUS les postes

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
      - pré-agrégation multi-postes (NB_POSTES, montant_brut_total,
        montant_brut_bareme, montant_net_total) avant déduplication
      - sélection et réordonnancement des colonnes
      - déduplication matricule × mois_annee
      - jointure des colonnes agrégées
      - tri matricule → annee → mois
    """

    # ── Extraction annee et mois depuis mois_annee (format MMYYYY) ────────
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

    # ── Pré-agrégation multi-postes (avant déduplication) ─────────────────
    cles_agg = [c for c in ["matricule", "mois_annee"] if c in df.columns]

    agg_nb         = None
    agg_brut_total = None
    agg_brut_bareme = None
    agg_net_total  = None

    if cles_agg:
        # Nombre de postes par individu × mois
        agg_nb = (
            df.group_by(cles_agg)
            .agg(pl.len().alias("NB_POSTES"))
        )

        # ── Check unicité agent × mois × organisme ───────────────────────
        if "CODE_ORGANISME" in df.columns:
            n_lignes_tot = len(df)
            n_unique_org = (
                df.select(["matricule", "mois_annee", "CODE_ORGANISME"])
                .unique()
                .select(pl.len())
                .item()
            )
            if n_lignes_tot > n_unique_org:
                n_doublons = n_lignes_tot - n_unique_org
                raise ValueError(
                    f"ARRÊT — Doublons intra-organisme détectés : {n_doublons:,} lignes en excès "
                    f"({n_lignes_tot:,} lignes vs {n_unique_org:,} uniques "
                    f"matricule × mois_annee × CODE_ORGANISME). "
                    f"Corriger le Silver avant de relancer le programme 06."
                )
            else:
                print(f"    ✓ Unicité agent × mois × organisme confirmée", flush=True)

        # ── Somme montant_brut sur tous les postes ────────────────────────
        if "montant_brut" in df.columns:
            df_brut = df.with_columns(
                pl.col("montant_brut").cast(pl.Float64, strict=False).alias("montant_brut")
            )
            agg_brut_total = (
                df_brut.group_by(cles_agg)
                .agg(pl.col("montant_brut").sum().alias("montant_brut_total"))
            )
            del df_brut

        # ── montant_brut_bareme = somme horizontale colonnes F2 ───────────
        # Les colonnes F2 (124_F2, 228_F2, 271_F2, 421_F2, 425_F2) proviennent
        # de la Feuille 2 du fichier source, jointe par MATRICULE||CODE_ORGANISME.
        # Chaque ligne contient déjà la valeur correcte pour son organisme.
        # On somme horizontalement puis on agrège par (matricule × mois_annee)
        # pour cumuler les organismes différents d'un agent multi-postes.
        cols_f2 = [c for c in df.columns if c.upper() in NOMS_F2]
        if cols_f2:
            print(f"    Composantes F2 détectées : {cols_f2}", flush=True)
            agg_brut_bareme = (
                df
                .with_columns([
                    pl.col(c).cast(pl.Float64, strict=False) for c in cols_f2
                ])
                .with_columns(
                    pl.sum_horizontal([pl.col(c) for c in cols_f2])
                    .alias("_bareme_row")
                )
                .group_by(cles_agg)
                .agg(pl.col("_bareme_row").sum().alias("montant_brut_bareme"))
            )
        else:
            print(f"    ⚠️  Aucune colonne F2 trouvée {sorted(NOMS_F2)} — "
                  f"montant_brut_bareme non calculé", flush=True)

        # Somme montant_net sur tous les postes
        if "montant_net" in df.columns:
            agg_net_total = (
                df.with_columns(
                    pl.col("montant_net").cast(pl.Float64, strict=False).alias("montant_net")
                )
                .group_by(cles_agg)
                .agg(pl.col("montant_net").sum().alias("montant_net_total"))
            )

        n_postes = agg_nb["NB_POSTES"]
        n_multi  = int((n_postes > 1).sum())
        print(f"    Multi-postes : {n_multi:,} / {len(agg_nb):,} individus×mois", flush=True)

        # ── Stats NB_POSTES par période (mois_annee) ──────────────────────
        if "mois_annee" in agg_nb.columns:
            stats_periode = (
                agg_nb
                .group_by("mois_annee")
                .agg([
                    pl.len().alias("nb_agents"),
                    (pl.col("NB_POSTES") > 1).sum().alias("nb_multipostes"),
                    pl.col("NB_POSTES").mean().round(3).alias("moy_postes_tous"),
                    pl.col("NB_POSTES")
                      .filter(pl.col("NB_POSTES") > 1)
                      .mean().round(3).alias("moy_postes_multi"),
                ])
                .with_columns(
                    (100 * pl.col("nb_multipostes") / pl.col("nb_agents"))
                    .round(2).alias("pct_multi")
                )
                .sort("mois_annee")
            )
            print(f"    {'mois_annee':<12} {'nb_agents':>12} {'multi-postes':>14} "
                  f"{'%':>8} {'moy(tous)':>11} {'moy(multi)':>12}", flush=True)
            print(f"    {'-'*12} {'-'*12} {'-'*14} {'-'*8} {'-'*11} {'-'*12}", flush=True)
            for row in stats_periode.iter_rows(named=True):
                moy_multi = f"{row['moy_postes_multi']:.3f}" if row['moy_postes_multi'] is not None else "   —"
                print(f"    {row['mois_annee']:<12} {row['nb_agents']:>12,} "
                      f"{row['nb_multipostes']:>14,} {row['pct_multi']:>7.2f}% "
                      f"{row['moy_postes_tous']:>11.3f} {moy_multi:>12}", flush=True)
            print(flush=True)

    # ── Sélection défensive : colonnes COLONNES_PANEL présentes dans df ───
    # Les nouvelles colonnes agrégées (NB_POSTES, montant_brut_total, etc.)
    # ne sont pas encore dans df — elles seront jointes après déduplication.
    cols_silver    = [c for c in COLONNES_PANEL
                      if c in df.columns
                      and c not in ("NB_POSTES", "montant_brut_total",
                                    "montant_brut_bareme", "montant_net_total")]
    cols_absentes  = [c for c in COLONNES_PANEL
                      if c not in df.columns
                      and c not in ("NB_POSTES", "montant_brut_total",
                                    "montant_brut_bareme", "montant_net_total")]
    df = df.select(cols_silver)

    if cols_absentes:
        print(f"    ⚠️  Colonnes absentes du Silver (ignorées) : {cols_absentes}", flush=True)

    # ── Déduplication matricule × mois_annee (garde la première occurrence) ──
    cles_dedup = [c for c in ["matricule", "mois_annee"] if c in df.columns]
    if cles_dedup:
        avant = len(df)
        df    = df.unique(subset=cles_dedup, keep="first", maintain_order=True)
        apres = len(df)
        if avant != apres:
            print(f"    Déduplication : {avant - apres:,} lignes supprimées "
                  f"→ {apres:,} individus×mois", flush=True)

    # ── Jointure des colonnes agrégées ────────────────────────────────────
    if cles_agg:
        if agg_nb is not None:
            df = df.join(agg_nb, on=cles_agg, how="left")
        if agg_brut_total is not None:
            df = df.join(agg_brut_total, on=cles_agg, how="left")
        if agg_brut_bareme is not None:
            df = df.join(agg_brut_bareme, on=cles_agg, how="left")
        else:
            # Colonnes F2 absentes : fallback montant_brut_bareme = montant_brut_total
            if agg_brut_total is not None:
                df = df.with_columns(
                    pl.col("montant_brut_total").alias("montant_brut_bareme")
                )
                print("    ⚠️  Colonnes F2 absentes — montant_brut_bareme = montant_brut_total",
                      flush=True)
        if agg_net_total is not None:
            df = df.join(agg_net_total, on=cles_agg, how="left")

    # ── Tri individu → chronologique ──────────────────────────────────────
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
    for col_critique in ["GRADE", "GRADE_SOURCE", "montant_brut_SOURCE", "montant_net_SOURCE",
                         "NB_POSTES", "montant_brut_total", "montant_brut_bareme"]:
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
    # moy(montant_brut_total)
    col_brut = "montant_brut_total" if "montant_brut_total" in df_panel.columns else "montant_brut"
    if col_brut in df_panel.columns:
        brut = df_panel[col_brut].drop_nulls()
        row_stat["moy_montant_brut_total"] = round(brut.mean(), 0) if len(brut) > 0 else None
    # moy(montant_brut_bareme)
    if "montant_brut_bareme" in df_panel.columns:
        bareme = df_panel["montant_brut_bareme"].drop_nulls()
        row_stat["moy_montant_brut_bareme"] = round(bareme.mean(), 0) if len(bareme) > 0 else None
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
    if "moy_montant_brut_total" in row and row["moy_montant_brut_total"]:
        ligne += f"  |  moy(montant_brut_total) = {row['moy_montant_brut_total']:,.0f}"
    if "moy_montant_brut_bareme" in row and row["moy_montant_brut_bareme"]:
        ligne += f"  |  moy(montant_brut_bareme) = {row['moy_montant_brut_bareme']:,.0f}"
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