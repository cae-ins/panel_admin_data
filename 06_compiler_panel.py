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
#
# BRANCHE No_allocation_Familiale :
#   Code 421 = Allocation familiale → retirée de montant_brut_sans_ind_sans_alloc
#   montant_brut_sans_ind_sans_alloc = 124 + 228 + 271 + 425 (sans allocation familiale)
#   → utilisé pour les indicateurs par CITP, GRADE, sexe
#   → conformité barème (comparé à montant_brut_sans_indemnites sans alloc_fam dans anstat_2025)
#
#   montant_brut_total (existant) = sum(montant_brut) tous postes / tous organismes
#   → utilisé pour les indicateurs secteur / barème (Méthodologie 2)
NOMS_F2_BAREME = {"124_F2", "228_F2", "271_F2", "425_F2"}  # sans 421 allocation familiale

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
    "montant_brut_sans_ind_sans_alloc",   # Somme F2 : {124,228,271,425} — sans 421 allocation familiale
    "montant_brut_total_composantes_F2",    # Somme de TOUTES les colonnes *_F2 — Méthodologie 2 secteur/barème
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
        montant_brut_sans_ind_sans_alloc, montant_net_total) avant déduplication
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

    agg_nb          = None
    agg_brut_total  = None
    agg_brut_bareme = None
    agg_brut_Total  = None   # montant_brut_total_composantes_F2 = somme toutes *_F2 (Méthodologie 2)
    agg_net_total   = None

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

        # ── Diagnostic + calcul montant_brut_total_composantes_F2 ───────────────────────────
        # montant_brut_total_composantes_F2 = somme de TOUTES les colonnes *_F2
        #   (toutes composantes de la Feuille 2, excluant les colonnes "total
        #    déclaré" qui récapitulent déjà la somme → double compte sinon).
        #
        # La sommation est TOUJOURS effectuée sur les composantes (Option A).
        # Le total déclaré F2 (brut_montant_f2 / total_general_f2) est utilisé
        # uniquement comme référence de vérification dans les diagnostics.
        #
        # Deux diagnostics imprimés (informatifs, n'arrêtent pas le programme) :
        #   Diag 1 — Cohérence interne F2 :
        #              sum(composantes *_F2)  vs  total_déclaré_F2
        #   Diag 2 — Cohérence inter-feuilles :
        #              total_déclaré_F2       vs  montant_brut (Feuille 1)
        #
        # Pour la suite des analyses :
        #   · Méthodologie 1 (CITP/GRADE/sexe) → montant_brut_sans_ind_sans_alloc (4 composantes)
        #   · Méthodologie 2 (secteur/barème)  → montant_brut_total  (Feuille 1)

        # Noms des colonnes "total déclaré" de F2 à exclure de la somme
        NOMS_TOTAL_F2 = {"brut_montant_f2", "total_general_f2", "montant_general_f2",
                         "total_f2", "montant_brut_f2"}

        # Colonne "total déclaré" présente dans le Silver (s'il y en a une)
        col_total_f2 = next(
            (c for c in df.columns if c.lower() in NOMS_TOTAL_F2), None
        )

        # Composantes *_F2 = toutes colonnes *_F2 SAUF les colonnes total déclaré
        # (on exclut aussi toute colonne dont le nom contient "montant_brut"
        #  pour éviter les doublons si la normalisation a créé un alias)
        cols_f2_composantes = [
            c for c in df.columns
            if c.upper().endswith("_F2")
            and c.lower() not in NOMS_TOTAL_F2
        ]

        # ── Résumé de présence F2 : données réelles ou colonnes vides ? ──────
        # Vérification robuste : la colonne doit avoir des valeurs non-nulles
        # ET non-vides ET parsables en nombre (cast Float64 réussi).
        def _col_f2_peuplee(col: str) -> bool:
            """True si la colonne a au moins une valeur numérique réelle."""
            vals = df[col].drop_nulls()
            if vals.len() == 0:
                return False
            # Si déjà numérique → peuplée
            if vals.dtype in (pl.Float64, pl.Float32, pl.Int64, pl.Int32):
                return vals.len() > 0
            # Si string : filtrer les chaînes vides puis tenter le cast
            vals_str = vals.cast(pl.Utf8).str.strip_chars()
            vals_str = vals_str.filter(vals_str != "")
            if vals_str.len() == 0:
                return False
            # Vérifier que le cast Float64 produit au moins une valeur non-nulle
            n_numeriques = vals_str.cast(pl.Float64, strict=False).drop_nulls().len()
            return n_numeriques > 0

        _f2_peuplees = [c for c in cols_f2_composantes if _col_f2_peuplee(c)] \
                       if cols_f2_composantes else []
        _f2_types    = {df[c].dtype for c in cols_f2_composantes} if cols_f2_composantes else set()

        if _f2_peuplees:
            print(f"    ✅ Feuille 2 PRÉSENTE dans le Silver : {len(_f2_peuplees)}/{len(cols_f2_composantes)}"
                  f" colonnes *_F2 ont des valeurs numériques réelles"
                  f"  [types : {_f2_types}]", flush=True)
        elif cols_f2_composantes:
            # Distinguer : toutes nulles vs toutes vides (strings non parsables)
            _n_non_null = sum(df[c].drop_nulls().len() for c in cols_f2_composantes)
            if _n_non_null == 0:
                print(f"    ❌ Feuille 2 ABSENTE du Silver : {len(cols_f2_composantes)} colonnes *_F2"
                      f" toutes nulles → relancer step 02 (Bronze)", flush=True)
            else:
                print(f"    ⚠️  Feuille 2 non parsable : {len(cols_f2_composantes)} colonnes *_F2"
                      f" non-nulles MAIS aucune valeur numérique après cast Float64"
                      f"  [types : {_f2_types}]", flush=True)
                # Afficher un échantillon pour comprendre le format
                for c in cols_f2_composantes[:3]:
                    sample = df[c].drop_nulls().head(3).to_list()
                    print(f"      Exemple '{c}' : {sample}", flush=True)
        else:
            print(f"    ❌ Feuille 2 ABSENTE du Silver : aucune colonne *_F2 trouvée", flush=True)

        print(f"    Colonnes total F2 exclues de la somme : {sorted(NOMS_TOTAL_F2)}",
              flush=True)
        if col_total_f2:
            print(f"    ✅ Total F2 déclaré détecté   : '{col_total_f2}' "
                  f"(utilisé pour le diagnostic uniquement)", flush=True)
        else:
            print(f"    ⚠️  Aucun total F2 déclaré détecté — Diag 2 (F2 vs F1) impossible",
                  flush=True)
        print(f"    Composantes *_F2 détectées : {len(cols_f2_composantes)} colonnes"
              + (f"  |  Total général déclaré : {col_total_f2}" if col_total_f2 else ""),
              flush=True)

        if cols_f2_composantes:
            # Cast avec nettoyage préalable des strings (espaces, virgules)
            def _nettoyer_col_f2(c: str) -> pl.Expr:
                if df[c].dtype == pl.Utf8:
                    return (
                        pl.col(c)
                        .str.strip_chars()
                        .str.replace_all(r"\s", "", literal=False)  # "1 500" → "1500"
                        .str.replace_all(",", ".", literal=True)     # "1,5" → "1.5"
                        .cast(pl.Float64, strict=False)
                    )
                return pl.col(c).cast(pl.Float64, strict=False)

            df_avec_sum = df.with_columns([
                _nettoyer_col_f2(c).alias(c) for c in cols_f2_composantes
            ]).with_columns(
                pl.sum_horizontal([pl.col(c) for c in cols_f2_composantes])
                .alias("_sum_composantes_f2")
            )
            # Vérification post-cast : combien de lignes ont une somme F2 > 0 ?
            n_f2_ok = int(df_avec_sum["_sum_composantes_f2"].drop_nulls().len())
            n_f2_nz = int((df_avec_sum["_sum_composantes_f2"].fill_null(0) > 0).sum())
            print(f"    Après cast → {n_f2_ok:,} lignes avec _sum_composantes_f2 non-null"
                  f"  dont {n_f2_nz:,} > 0", flush=True)

            # ── Diagnostic 1 : sum(composantes F2) vs total déclaré F2 ──────
            # Vérifie la cohérence INTERNE de la Feuille 2.
            # Un écart ici signifie que les composantes listées ne couvrent
            # pas exactement ce que la Feuille 2 déclare comme total.
            if col_total_f2:
                df_d1 = df_avec_sum.with_columns(
                    pl.col(col_total_f2).cast(pl.Float64, strict=False)
                    .alias("_total_f2_declared")
                ).with_columns(
                    (pl.col("_sum_composantes_f2") - pl.col("_total_f2_declared"))
                    .alias("_ecart_interne")
                )
                n_tot1     = len(df_d1)
                n_abs1     = int(df_d1["_total_f2_declared"].is_null().sum())
                n_pres1    = n_tot1 - n_abs1
                pct_abs1   = 100 * n_abs1  / n_tot1 if n_tot1 > 0 else 0
                pct_pres1  = 100 * n_pres1 / n_tot1 if n_tot1 > 0 else 0
                df_p1      = df_d1.filter(pl.col("_total_f2_declared").is_not_null())
                n_eg1      = int((df_p1["_ecart_interne"] == 0).sum())
                n_ec1      = n_pres1 - n_eg1
                pct_eg1    = 100 * n_eg1  / n_pres1 if n_pres1 > 0 else 0
                pct_ec1    = 100 * n_ec1  / n_pres1 if n_pres1 > 0 else 0

                print(f"\n    DIAGNOSTIC 1 — Cohérence interne F2 :", flush=True)
                print(f"      sum(composantes *_F2) vs '{col_total_f2}'", flush=True)
                print(f"    {'─'*65}", flush=True)
                print(f"    Lignes totales                       : {n_tot1:>10,}  (100.0%)",
                      flush=True)
                print(f"    '{col_total_f2}' absent (null)       : {n_abs1:>10,}  "
                      f"({pct_abs1:5.1f}%)  → pas de total déclaré F2", flush=True)
                print(f"    '{col_total_f2}' présent             : {n_pres1:>10,}  "
                      f"({pct_pres1:5.1f}%)", flush=True)
                print(f"    {'─'*65}", flush=True)
                print(f"    Parmi les lignes avec total F2 déclaré :", flush=True)
                print(f"      ✅ Égalité exacte (écart = 0)      : {n_eg1:>10,}  "
                      f"({pct_eg1:5.1f}%)", flush=True)
                print(f"      ⚠️  Avec écart (≠ 0)               : {n_ec1:>10,}  "
                      f"({pct_ec1:5.1f}%)", flush=True)
                if n_ec1 > 0:
                    lec1    = df_p1.filter(pl.col("_ecart_interne") != 0)["_ecart_interne"]
                    ec1_moy = lec1.mean()
                    ec1_abs = lec1.abs().mean()
                    ec1_med = lec1.median()
                    # Écart signé : + = sum(composantes) > total F2 déclaré
                    #               - = sum(composantes) < total F2 déclaré
                    print(f"        Écart moyen (signé)           : {ec1_moy:>12,.0f} FCFA  "
                          f"(+ = sum > total_F2  /  - = sum < total_F2)", flush=True)
                    print(f"        Écart moyen (absolu)          : {ec1_abs:>12,.0f} FCFA",
                          flush=True)
                    print(f"        Écart médian (signé)          : {ec1_med:>12,.0f} FCFA",
                          flush=True)
                del df_d1, df_p1

            # ── Diagnostic 2 : total déclaré F2 vs montant_brut Feuille 1 ───
            # Vérifie la cohérence INTER-FEUILLES.
            # Un agent absent de F2 → colonnes F2 nulles (pas de Feuille 2).
            # Un écart résiduel (présent dans les deux mais différent) est à investiguer.
            if "montant_brut" in df.columns:
                if col_total_f2:
                    # Cas normal : comparer le total déclaré F2 à montant_brut F1
                    df_d2 = df_avec_sum.with_columns([
                        pl.col("montant_brut").cast(pl.Float64, strict=False),
                        pl.col(col_total_f2).cast(pl.Float64, strict=False)
                        .alias("_ref_f2"),
                    ]).with_columns([
                        pl.col("_ref_f2").is_null().alias("_absent_f2"),
                        (pl.col("_ref_f2") - pl.col("montant_brut")).alias("_ecart_f1f2"),
                    ])
                    ref_label = f"'{col_total_f2}' (total déclaré F2)"
                else:
                    # Pas de total déclaré : comparer sum(composantes) à montant_brut F1
                    df_d2 = df_avec_sum.with_columns(
                        pl.col("montant_brut").cast(pl.Float64, strict=False)
                    ).with_columns([
                        pl.col("_sum_composantes_f2").is_null().alias("_absent_f2"),
                        (pl.col("_sum_composantes_f2") - pl.col("montant_brut"))
                        .alias("_ecart_f1f2"),
                    ])
                    ref_label = "sum(composantes *_F2)"

                n_tot2   = len(df_d2)
                n_abs2   = int(df_d2["_absent_f2"].sum())
                n_pres2  = n_tot2 - n_abs2
                pct_ab2  = 100 * n_abs2  / n_tot2 if n_tot2 > 0 else 0
                pct_pr2  = 100 * n_pres2 / n_tot2 if n_tot2 > 0 else 0
                df_p2    = df_d2.filter(~pl.col("_absent_f2"))
                n_eg2    = int((df_p2["_ecart_f1f2"] == 0).sum())
                n_ec2    = n_pres2 - n_eg2
                pct_eg2  = 100 * n_eg2  / n_pres2 if n_pres2 > 0 else 0
                pct_ec2  = 100 * n_ec2  / n_pres2 if n_pres2 > 0 else 0

                print(f"\n    DIAGNOSTIC 2 — Cohérence inter-feuilles :", flush=True)
                print(f"      {ref_label} vs montant_brut (Feuille 1)", flush=True)
                print(f"    {'─'*65}", flush=True)
                print(f"    Lignes totales (Feuille 1)           : {n_tot2:>10,}  (100.0%)",
                      flush=True)
                print(f"    Absents de Feuille 2 (null)          : {n_abs2:>10,}  "
                      f"({pct_ab2:5.1f}%)  → agent sans données F2", flush=True)
                print(f"    Présents dans les deux feuilles      : {n_pres2:>10,}  "
                      f"({pct_pr2:5.1f}%)", flush=True)
                print(f"    {'─'*65}", flush=True)
                print(f"    Parmi les présents en F2 :", flush=True)
                print(f"      ✅ Égalité exacte (écart = 0)      : {n_eg2:>10,}  "
                      f"({pct_eg2:5.1f}%)", flush=True)
                print(f"      ⚠️  Avec écart résiduel (≠ 0)      : {n_ec2:>10,}  "
                      f"({pct_ec2:5.1f}%)", flush=True)
                if n_ec2 > 0:
                    lec2    = df_p2.filter(pl.col("_ecart_f1f2") != 0)["_ecart_f1f2"]
                    ec2_moy = lec2.mean()
                    ec2_abs = lec2.abs().mean()
                    ec2_med = lec2.median()
                    # Écart signé : + = F2 supérieur à montant_brut F1
                    #               - = F2 inférieur à montant_brut F1
                    print(f"        Écart moyen (signé)           : {ec2_moy:>12,.0f} FCFA  "
                          f"(+ = F2 > F1  /  - = F2 < F1)", flush=True)
                    print(f"        Écart moyen (absolu)          : {ec2_abs:>12,.0f} FCFA",
                          flush=True)
                    print(f"        Écart médian (signé)          : {ec2_med:>12,.0f} FCFA",
                          flush=True)
                    print(f"        → À investiguer : différence F1 vs F2 pour ces agents",
                          flush=True)
                del df_d2, df_p2

            print(f"\n    {'─'*65}", flush=True)
            print(f"    → montant_brut_total_composantes_F2 = sum({len(cols_f2_composantes)} composantes *_F2) "
                  f"[Option A — conservé tel quel]", flush=True)
            print(f"    → Analyses Méthodo 2 : montant_brut_total (Feuille 1) sera utilisé",
                  flush=True)
            print(f"    {'─'*65}", flush=True)

            # Agrégation montant_brut_total_composantes_F2 par individu × mois (tous organismes)
            agg_brut_Total = (
                df_avec_sum
                .group_by(cles_agg)
                .agg(pl.col("_sum_composantes_f2").sum().alias("montant_brut_total_composantes_F2"))
            )
            del df_avec_sum
        else:
            print(f"    ⚠️  Aucune composante *_F2 trouvée — montant_brut_total_composantes_F2 non calculé",
                  flush=True)

        # ── montant_brut_sans_ind_sans_alloc = somme des 4 composantes F2 connues (sans 421) ──
        # NOMS_F2_BAREME = {124_F2, 228_F2, 271_F2, 425_F2} — code 421 exclu.
        # On somme horizontalement puis on agrège par (matricule × mois_annee)
        # pour cumuler les organismes différents d'un agent multi-postes.
        cols_f2 = [c for c in df.columns if c.upper() in NOMS_F2_BAREME]
        if cols_f2:
            print(f"    Composantes F2 barème (sans 421) : {sorted(cols_f2)}", flush=True)
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
                .agg(pl.col("_bareme_row").sum().alias("montant_brut_sans_ind_sans_alloc"))
            )
        else:
            print(f"    ⚠️  Aucune colonne F2 barème trouvée {sorted(NOMS_F2_BAREME)} — "
                  f"montant_brut_sans_ind_sans_alloc non calculé", flush=True)

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
    _cols_calculees = ("NB_POSTES", "montant_brut_total", "montant_brut_sans_ind_sans_alloc",
                       "montant_brut_total_composantes_F2", "montant_net_total")
    cols_silver    = [c for c in COLONNES_PANEL
                      if c in df.columns and c not in _cols_calculees]
    cols_absentes  = [c for c in COLONNES_PANEL
                      if c not in df.columns and c not in _cols_calculees]
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
            print("    ⚠️  montant_brut_sans_ind_sans_alloc non calculé (colonnes F2 barème absentes)",
                  flush=True)
        if agg_brut_Total is not None:
            df = df.join(agg_brut_Total, on=cles_agg, how="left")
        else:
            print("    ⚠️  montant_brut_total_composantes_F2 non calculé (aucune colonne *_F2 détectée)",
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

    # ── DIAGNOSTIC SILVER : multi-postes avant tout traitement ──────────────
    if "matricule" in df.columns and "mois_annee" in df.columns:
        _nb = df.group_by(["matricule", "mois_annee"]).agg(pl.len().alias("n"))
        _n_total = len(_nb)
        _n_multi = int((_nb["n"] > 1).sum())
        _max_n   = int(_nb["n"].max())

        print(f"    DIAG SILVER [{annee_str}] ─── lignes par (matricule × mois_annee) ───")
        print(f"      Lignes totales Silver          : {len(df):>12,}")
        print(f"      Paires (matricule × mois)      : {_n_total:>12,}")
        print(f"      Dont multi-postes (n > 1)      : {_n_multi:>12,}  "
              f"({100 * _n_multi / _n_total:.2f}%)")
        print(f"      Max postes par agent × mois    : {_max_n:>12}", flush=True)

        # Distribution NB_POSTES : 1 poste, 2 postes, 3+
        _distrib = (
            _nb["n"]
            .value_counts()
            .sort("n")
            .with_columns(
                (pl.col("count") * 100 / _n_total).round(2).alias("pct")
            )
        )
        print(f"      Distribution NB_POSTES :")
        for _row in _distrib.iter_rows(named=True):
            _label = f"{_row['n']} poste{'s' if _row['n'] > 1 else ' '}"
            print(f"        {_label:<12} : {_row['count']:>10,}  ({_row['pct']:.2f}%)",
                  flush=True)

        # Si multi-postes détectés : détail par CODE_ORGANISME
        if _n_multi > 0 and "CODE_ORGANISME" in df.columns:
            _multi_mat = _nb.filter(pl.col("n") > 1).select(["matricule", "mois_annee"])
            _multi_rows = df.join(_multi_mat, on=["matricule", "mois_annee"], how="inner")

            # Nb d'organismes distincts impliqués
            _nb_org = _multi_rows["CODE_ORGANISME"].n_unique()
            print(f"\n      Détail multi-postes — organismes impliqués : {_nb_org}")

            # Top organismes les plus fréquents chez les multi-postes
            _org_counts = (
                _multi_rows
                .group_by("CODE_ORGANISME")
                .agg(pl.len().alias("nb_lignes"))
                .sort("nb_lignes", descending=True)
                .head(10)
            )
            print(f"      Top organismes (max 10) :")
            for _row in _org_counts.iter_rows(named=True):
                _org_label = str(_row["CODE_ORGANISME"])
                # Ajouter libellé organisme si disponible
                if "organisme" in df.columns:
                    _lib = (
                        _multi_rows
                        .filter(pl.col("CODE_ORGANISME") == _row["CODE_ORGANISME"])
                        ["organisme"]
                        .drop_nulls()
                        .head(1)
                        .to_list()
                    )
                    if _lib:
                        _org_label += f" ({_lib[0][:30]})"
                print(f"        {_org_label:<40} : {_row['nb_lignes']:>8,} lignes",
                      flush=True)

            # Détail des paires multi-postes (jusqu'à 10 exemples)
            _exemples = (
                _multi_rows
                .select([c for c in ["matricule", "mois_annee", "CODE_ORGANISME",
                                     "organisme", "montant_brut"]
                         if c in _multi_rows.columns])
                .sort(["matricule", "mois_annee"])
                .head(20)
            )
            print(f"\n      Exemples (jusqu'à 20 lignes) :")
            print(_exemples, flush=True)
            del _multi_mat, _multi_rows

        elif _n_multi == 0:
            print(f"\n      ✅ Silver {annee_str} : aucun multi-poste détecté "
                  f"— 1 ligne par (matricule × mois) confirmé.", flush=True)

        del _nb
    # ────────────────────────────────────────────────────────────────────────

    df_panel = construire_panel_annee(df)
    del df
    gc.collect()

    nb_lignes = len(df_panel)
    print(f"    Panel : {nb_lignes:,} lignes × {len(df_panel.columns)} cols", flush=True)

    # Vérification explicite des colonnes critiques
    for col_critique in ["GRADE", "GRADE_SOURCE", "montant_brut_SOURCE", "montant_net_SOURCE",
                         "NB_POSTES", "montant_brut_total", "montant_brut_sans_ind_sans_alloc"]:
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
    # moy(montant_brut_sans_ind_sans_alloc)
    if "montant_brut_sans_ind_sans_alloc" in df_panel.columns:
        bareme = df_panel["montant_brut_sans_ind_sans_alloc"].drop_nulls()
        row_stat["moy_montant_brut_sans_ind_sans_alloc"] = round(bareme.mean(), 0) if len(bareme) > 0 else None
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
    def _cle_chrono(p: str) -> str:
        return p[2:] + p[:2]   # MMYYYY → YYYYMM
    periode_min = min(periodes_vues, key=_cle_chrono)
    periode_max = max(periodes_vues, key=_cle_chrono)
    print(f"Plage temporelle      : {periode_min} → {periode_max}")
    print(f"Périodes distinctes   : {len(set(periodes_vues))}")

print(f"\nDétail par année :")
for row in stats_par_annee:
    ligne = f"  {row['annee']}  :  {row.get('nb_obs', 0):>8,} obs"
    if "nb_individus" in row:
        ligne += f"  |  {row['nb_individus']:>6,} individus"
    if "moy_montant_brut_total" in row and row["moy_montant_brut_total"]:
        ligne += f"  |  moy(montant_brut_total) = {row['moy_montant_brut_total']:,.0f}"
    if "moy_montant_brut_sans_ind_sans_alloc" in row and row["moy_montant_brut_sans_ind_sans_alloc"]:
        ligne += f"  |  moy(montant_brut_sans_ind_sans_alloc) = {row['moy_montant_brut_sans_ind_sans_alloc']:,.0f}"
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