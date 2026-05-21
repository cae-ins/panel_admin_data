# ============================================================
# PANEL ADMIN — ÉTAPE 3 : BRONZE → SILVER PARQUET
# ============================================================
# Lit tous les fichiers Parquet Bronze, applique la logique métier :
#   - Mapping des colonnes brutes → noms standardisés
#   - Normalisation et filtrage des situations administratives
#   - Enrichissement avec les codes ANSTAT
#   - Cast des montants en numérique
#   - Calcul de salaire_brut_f2 = somme des composantes F2
#     (124_f2 + 228_f2 + 271_f2 + 421_f2 + 425_f2)
#
# Ajout vs version initiale :
#   - Suivi du taux de matching ANSTAT PAR PÉRIODE
#   - Calcul de salaire_brut_f2 depuis les composantes F2
#
# Source  : s3://bronze/panel_admin/*.parquet
# Sortie  : s3://silver/panel_admin/YYYY.parquet (un par année)
#
# Dépendances :
#   pip install polars fastexcel boto3 python-dotenv PyYAML
# ============================================================

import gc
import io
import os
import re
import tempfile
import unicodedata
from pathlib import Path
import yaml
from common.s3_client import create_s3_client
from common.logging_config import setup_pipeline_logging
import polars as pl

# --- CONFIGURATION ---

CONFIG_PATH = Path(__file__).parent / "config" / "pipeline_settings.yaml"

def load_pipeline_settings():
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Fichier de configuration introuvable : {CONFIG_PATH}")
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

SETTINGS = load_pipeline_settings()
COLUMN_MAPPING = SETTINGS.get("column_mapping", {})
SITUATIONS_VALIDES = SETTINGS.get("situations_valides", {})

BUCKET_STAGING = os.getenv("BUCKET_STAGING", "staging")
BUCKET_BRONZE  = os.getenv("BUCKET_BRONZE",  "bronze")
BUCKET_SILVER  = os.getenv("BUCKET_SILVER",  "silver")
PREFIX_BRONZE  = "panel_admin"
PREFIX_SILVER  = "panel_admin"
PREFIX_REFS    = "panel_admin/references"
PREFIX_VALID   = "panel_admin/validation"

# Codes dont on suit le taux de matching par période
CODES_A_SURVEILLER = ["CODE_ORGANISME", "CODE_GRADE", "CODE_EMPLOI"]

# Composantes du salaire brut F2
# Noms en lowercase après normalisation par le script 02
COMPOSANTES_F2 = ["124_f2", "228_f2", "271_f2", "421_f2", "425_f2"]

# --- CLIENT S3 (MinIO) ---
s3 = create_s3_client()
logger, _ = setup_pipeline_logging("03_bronze_to_silver")

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
    logger.info(f"  ✓ s3://{bucket}/{key}  ({len(df):,} lignes × {len(df.columns)} cols  {taille:.1f} MB)")


def sauvegarder_csv_staging(df: pl.DataFrame, nom_fichier: str) -> None:
    csv_bytes = df.write_csv().encode("utf-8")
    s3.put_object(
        Bucket=BUCKET_STAGING,
        Key=f"{PREFIX_VALID}/{nom_fichier}",
        Body=csv_bytes,
    )
    logger.info(f"  ✓ Rapport sauvegardé : {nom_fichier}")


def normaliser_pour_matching(texte) -> str | None:
    if texte is None:
        return None
    texte = str(texte).strip()
    if not texte:
        return None
    texte = re.sub(r"[-]", " ", texte)
    texte = re.sub(r"[.,;:()\[\]/]", " ", texte)
    texte = re.sub("[''’]", " ", texte)
    texte = texte.upper()
    texte = unicodedata.normalize("NFKD", texte)
    texte = "".join(c for c in texte if not unicodedata.combining(c))
    texte = re.sub(r"\s+", " ", texte).strip()
    return texte if texte else None


def normaliser_nom_colonne(nom: str) -> str:
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


def mapper_colonnes(noms_colonnes: list[str]) -> dict[str, str]:
    correspondance = {}
    for nom in noms_colonnes:
        nom_norm = normaliser_nom_colonne(nom)
        matched  = None
        for std_name, patterns in COLUMN_MAPPING.items():
            for pattern in patterns:
                if re.search(pattern, nom_norm, re.IGNORECASE):
                    matched = std_name
                    break
            if matched:
                break
        correspondance[nom] = matched if matched else nom_norm.lower()
    return correspondance


def normaliser_situation(situation) -> str:
    if not situation or str(situation).strip() == "":
        return "autre"
    sit = str(situation).strip().upper()
    sit = unicodedata.normalize("NFKD", sit)
    sit = "".join(c for c in sit if not unicodedata.combining(c))
    sit = re.sub(r"\s+", " ", sit).strip()
    
    for categorie, valeurs in SITUATIONS_VALIDES.items():
        if sit in valeurs:
            return categorie
    return "autre"


# ============================================================
# CHARGEMENT DES TABLES ANSTAT
# ============================================================

logger.info("=" * 70)
logger.info("CHARGEMENT : Tables de codes ANSTAT (depuis staging)")
logger.info("=" * 70)

paginator = s3.get_paginator("list_objects_v2")
pages     = paginator.paginate(Bucket=BUCKET_STAGING, Prefix=PREFIX_REFS)
fichier_anstat_key = next(
    (obj["Key"]
     for page in pages
     for obj in page.get("Contents", [])
     if "ANSTAT_CODE" in obj["Key"].upper()),
    None,
)

if fichier_anstat_key is None:
    raise FileNotFoundError(f"Fichier ANSTAT introuvable dans s3://{BUCKET_STAGING}/{PREFIX_REFS}/")

with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
    tmp_anstat = tmp.name
s3.download_file(BUCKET_STAGING, fichier_anstat_key, tmp_anstat)
logger.info(f"✓ Fichier ANSTAT téléchargé : {os.path.basename(fichier_anstat_key)}")


def charger_table_codes(
    sheet_name: str,
    libelle_col_names: list[str],
    col_jointure: str,
    cols_extra: list[str] | None = None,
) -> dict:
    dt = pl.read_excel(tmp_anstat, sheet_name=sheet_name)
    libelle_col = next(
        (col for col in dt.columns
         for pattern in libelle_col_names
         if pattern.upper() in col.upper()),
        None,
    )
    if not libelle_col:
        raise ValueError(
            f"Colonne libellé non trouvée dans l'onglet '{sheet_name}'.\n"
            f"  Cherché  : {libelle_col_names}\n"
            f"  Colonnes : {dt.columns}"
        )
    dt = dt.rename({libelle_col: "libelle"})
    dt = dt.with_columns(
        pl.col("libelle")
        .map_elements(normaliser_pour_matching, return_dtype=pl.Utf8)
        .alias("libelle_norm")
    )
    col_code = dt.columns[0]
    cols_a_garder = [col_code, "libelle", "libelle_norm"]
    if cols_extra:
        cols_a_garder += [c for c in cols_extra if c in dt.columns]
    dt = dt.select(cols_a_garder)
    return {"table": dt, "col_code": col_code, "col_jointure_source": col_jointure}


dictionnaires_codes = {
    "CODE_AFFECTATION":    charger_table_codes(
                             "lieu affectation",
                             ["LIBELLÉ_LIEU_AFFECTATION", "LIBELLE_LIEU_AFFECTATION"],
                             "lieu_affectation",
                             cols_extra=["CODE_RÉGION", "CODE_SITE", "CODE_DRB",
                                         "CODE_DÉPARTEMENT", "MONTANT TRANSPORT"],
                           ),
    "CODE_ORGANISME":      charger_table_codes(
                             "ORGANISME_OK",
                             ["LIBELLÉ_ORGANISME", "LIBELLE_ORGANISME"],
                             "organisme",
                             cols_extra=["CODE_ÉTABLISSEMENT", "CODE_ACCT"],
                           ),
    "CODE_POSITION_SOLDE": charger_table_codes(
                             "CODE_SITUATION_SOLDE",
                             ["LIBELLÉ_POSITION_SOLDE", "LIBELLE_SITUATION_SOLDE",
                              "LIBELLÉ_SITUATION_SOLDE"],
                             "situation",
                           ),
    "CODE_EMPLOI":         charger_table_codes(
                             "HISTORIQUE_ECHELLES_CORPS",
                             ["LIBELLÉ_EMPLOI", "LIBELLE_EMPLOI"],
                             "emploi",
                             cols_extra=["GRADE_ADMINISTRATIF_ASSOCIE",
                                         "Code_CITP", "Metier_CITP"],
                           ),
    "CODE_SERVICE":        charger_table_codes(
                             "SERVICE",
                             ["LIBELLÉ_SERVICE", "LIBELLE_SERVICE"],
                             "service",
                           ),
    "CODE_FONCTION":       charger_table_codes(
                             "FONCTION",
                             ["LIBELLÉ_FONCTION", "LIBELLE_FONCTION"],
                             "fonction",
                           ),
    "CODE_GRADE":          charger_table_codes(
                             "GRADE",
                             ["LIBELLÉ_GRADE", "LIBELLE_GRADE"],
                             "grade",
                           ),
    "CODE_POSTE":          charger_table_codes(
                             "LIBELLE POSTE",
                             ["LIBELLÉ_POSTE", "LIBELLE_POSTE"],
                             "poste",
                           ),
}

for nom_code, cfg in dictionnaires_codes.items():
    nb     = len(cfg["table"])
    extras = [c for c in cfg["table"].columns
              if c not in (cfg["col_code"], "libelle", "libelle_norm")]
    detail = f" + {extras}" if extras else ""
    logger.info(f"  {nom_code:<25} : {nb:>4} codes{detail}")

os.unlink(tmp_anstat)

# ============================================================
# LISTER LES FICHIERS BRONZE (un par mois/période)
# ============================================================

logger.info("=" * 70)
logger.info("LECTURE BRONZE")
logger.info("=" * 70)

pages_bronze = s3.get_paginator("list_objects_v2").paginate(
    Bucket=BUCKET_BRONZE, Prefix=PREFIX_BRONZE
)
cles_bronze = sorted([
    obj["Key"]
    for page in pages_bronze
    for obj in page.get("Contents", [])
    if obj["Key"].endswith(".parquet")
])

logger.info(f"Fichiers Parquet Bronze trouvés : {len(cles_bronze)}")

# ============================================================
# TRAITEMENT MOIS PAR MOIS + REGROUPEMENT PAR ANNÉE
# ============================================================

frames_par_annee: dict[str, list[pl.DataFrame]] = {}
stats_matching_par_periode: list[dict] = []

nb_ok  = 0
nb_err = 0

for cle in cles_bronze:
    periode = os.path.splitext(os.path.basename(cle))[0]
    m = re.search(r"20\d{2}", periode)
    annee = m.group() if m else periode[:4]

    try:
        df = lire_parquet_s3(BUCKET_BRONZE, cle)
        logger.info(f"  [{periode}] ✓ {len(df):,} lignes")

        # --- Mapping colonnes ---
        correspondance = mapper_colonnes(list(df.columns))
        noms_std_vals = list(correspondance.values())
        seen: dict = {}
        new_corr: dict = {}
        for src, std in correspondance.items():
            if noms_std_vals.count(std) > 1:
                seen[std] = seen.get(std, 0) + 1
                if seen[std] > 1:
                    std = f"{std}_dup{seen[std]}"
            new_corr[src] = std
        df = df.rename(new_corr)

        # --- Forcer mois_annee depuis le nom de fichier ---
        df = df.with_columns(pl.lit(periode).alias("mois_annee"))

        # --- Normalisation situations ---
        if "situation" in df.columns:
            df = df.with_columns([
                pl.col("situation").alias("situation_brute"),
                pl.col("situation")
                .map_elements(normaliser_situation, return_dtype=pl.Utf8)
                .alias("situation_normalisee"),
            ])
            df = df.filter(
                pl.col("situation_normalisee").is_in(
                    ["en_activite", "regul_indemnites", "demi_solde"]
                )
            )

        if len(df) == 0:
            logger.warning(f"    [{periode}] ⚠️  Aucune ligne conservée après filtrage situations")
            continue

        # --- Enrichissement ANSTAT ---
        row_stats: dict = {"periode": periode}

        for nom_code, cfg in dictionnaires_codes.items():
            col_jointure = cfg["col_jointure_source"]
            table_codes  = cfg["table"]
            col_code     = cfg["col_code"]

            if col_jointure not in df.columns:
                df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(nom_code))
                row_stats[f"{nom_code}_taux"] = 0.0
                continue

            df = df.with_columns(
                pl.col(col_jointure)
                .map_elements(normaliser_pour_matching, return_dtype=pl.Utf8)
                .alias("_col_norm_tmp")
            )
            table_match = (
                table_codes
                .filter(pl.col("libelle_norm").is_not_null())
                .select(["libelle_norm", col_code])
                .unique(subset=["libelle_norm"])
                .rename({col_code: nom_code})
            )
            cols_extra_src = [c for c in table_codes.columns
                              if c not in (col_code, "libelle", "libelle_norm")]
            if cols_extra_src:
                table_extra = (
                    table_codes
                    .filter(pl.col("libelle_norm").is_not_null())
                    .select(["libelle_norm"] + cols_extra_src)
                    .unique(subset=["libelle_norm"])
                )
                table_match = table_match.join(
                    table_extra, on="libelle_norm", how="left"
                )

            df = (
                df.join(table_match, left_on="_col_norm_tmp", right_on="libelle_norm", how="left")
                .drop("_col_norm_tmp")
            )

            taux = 100 * df[nom_code].is_not_null().sum() / len(df)
            row_stats[f"{nom_code}_taux"] = round(taux, 1)

            if nom_code in CODES_A_SURVEILLER:
                logger.info(f"      [{periode}] {nom_code} : {taux:.1f}%")

        stats_matching_par_periode.append(row_stats)

        # --- Cast numérique ---
        cols_num = ["montant_brut", "montant_net", "retenue_pension",
                    "impot", "charge_patronale"]
        cols_codes_num = [c for c in df.columns if re.match(r"^[0-9]", c)]
        exprs_num = (
            [pl.col(c).cast(pl.Float64, strict=False) for c in cols_num if c in df.columns]
            + [pl.col(c).cast(pl.Float64, strict=False) for c in cols_codes_num]
        )
        if exprs_num:
            df = df.with_columns(exprs_num)

        # --- Calcul salaire_brut_f2 ---
        # Somme des composantes F2 disponibles dans ce fichier.
        # Les noms sont en lowercase après le mapping du script 02
        # (ex. "124_f2", "228_f2"...).
        # On calcule ici dans le Silver — couche métier — pour que
        # l'étape 06 n'ait plus qu'à lire la colonne depuis le Silver.
        comps_presentes = [c for c in COMPOSANTES_F2 if c in df.columns]
        if comps_presentes:
            df = df.with_columns(
                pl.sum_horizontal([pl.col(c) for c in comps_presentes])
                .alias("salaire_brut_f2")
            )
        else:
            df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("salaire_brut_f2"))
            logger.warning(f"    [{periode}] ⚠️  Composantes F2 absentes — salaire_brut_f2 = null")

        # Accumulation par année
        if annee not in frames_par_annee:
            frames_par_annee[annee] = []
        frames_par_annee[annee].append(df)
        nb_ok += 1

    except Exception as e:
        logger.error(f"  [{periode}] ✗ ERREUR : {e}")
        nb_err += 1

    gc.collect()

# ============================================================
# ÉCRITURE SILVER (un fichier par année)
# ============================================================

logger.info("=" * 70)
logger.info("ÉCRITURE SILVER (par année)")
logger.info("=" * 70)

for annee, frames in sorted(frames_par_annee.items()):
    df_annee = pl.concat(frames, how="diagonal")
    key_silver = f"{PREFIX_SILVER}/{annee}.parquet"
    ecrire_parquet_s3(df_annee, BUCKET_SILVER, key_silver)
    del df_annee, frames
    gc.collect()

# ============================================================
# RAPPORT MATCHING ANSTAT PAR PÉRIODE
# ============================================================

logger.info("=" * 70)
logger.info("RAPPORT : Taux de matching ANSTAT par période")
logger.info("=" * 70)

if stats_matching_par_periode:
    df_stats = pl.DataFrame(stats_matching_par_periode)

    for code in CODES_A_SURVEILLER:
        col = f"{code}_taux"
        if col in df_stats.columns:
            taux_vals = df_stats[col].drop_nulls()
            if len(taux_vals) > 0:
                logger.info(f"  {code} — min: {taux_vals.min():.1f}%  "
                            f"max: {taux_vals.max():.1f}%  "
                            f"moy: {taux_vals.mean():.1f}%")

    sauvegarder_csv_staging(df_stats, "taux_matching_anstat_par_periode.csv")

# ============================================================
# RÉSUMÉ FINAL
# ============================================================

logger.info("=" * 70)
logger.info("RÉSUMÉ")
logger.info("=" * 70)
logger.info(f"Périodes traitées avec succès : {nb_ok}")
logger.info(f"Erreurs                       : {nb_err}")
logger.info(f"Années Silver produites       : {len(frames_par_annee)}")
logger.info(f"Fichiers Silver disponibles dans : s3://{BUCKET_SILVER}/{PREFIX_SILVER}/")
logger.info(f"Rapport matching : s3://{BUCKET_STAGING}/{PREFIX_VALID}/taux_matching_anstat_par_periode.csv")
logger.info("✓ SILVER TERMINÉ")
logger.info("Prochaine étape : exécuter 04_validation_silver.py")
