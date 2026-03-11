# ============================================================
# PANEL ADMIN — ÉTAPE 3 : BRONZE → SILVER PARQUET
# ============================================================
# Lit tous les fichiers Parquet Bronze, applique la logique métier :
#   - Mapping des colonnes brutes → noms standardisés
#   - Normalisation et filtrage des situations administratives
#   - Enrichissement avec les codes ANSTAT
#   - Cast des montants en numérique
#
# Ajout vs version initiale :
#   - Suivi du taux de matching ANSTAT PAR PÉRIODE (CODE_ORGANISME,
#     CODE_GRADE, CODE_EMPLOI) → rapport CSV dans staging/validation/
#
# Source  : s3://bronze/panel_admin/*.parquet
# Sortie  : s3://silver/panel_admin/YYYY.parquet (un par année)
#
# Dépendances :
#   pip install polars fastexcel boto3 python-dotenv
# ============================================================

import gc
import io
import os
import re
import tempfile
import unicodedata
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

BUCKET_STAGING = "staging"
BUCKET_BRONZE  = "bronze"
BUCKET_SILVER  = "silver"
PREFIX_BRONZE  = "panel_admin"
PREFIX_SILVER  = "panel_admin"
PREFIX_REFS    = "panel_admin/references"
PREFIX_VALID   = "panel_admin/validation"

# Codes dont on suit le taux de matching par période
CODES_A_SURVEILLER = ["CODE_ORGANISME", "CODE_GRADE", "CODE_EMPLOI"]

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
    print(f"  ✓ s3://{bucket}/{key}  ({len(df):,} lignes × {len(df.columns)} cols  {taille:.1f} MB)")


def sauvegarder_csv_staging(df: pl.DataFrame, nom_fichier: str) -> None:
    csv_bytes = df.write_csv().encode("utf-8")
    s3.put_object(
        Bucket=BUCKET_STAGING,
        Key=f"{PREFIX_VALID}/{nom_fichier}",
        Body=csv_bytes,
    )
    print(f"  ✓ Rapport sauvegardé : {nom_fichier}")


def normaliser_pour_matching(texte) -> str | None:
    if texte is None:
        return None
    texte = str(texte).strip()
    if not texte:
        return None
    texte = re.sub(r"[-]", " ", texte)
    texte = re.sub(r"[.,;:()\[\]/]", " ", texte)
    texte = re.sub(r"[''\\u2019]", " ", texte)
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
    mapping = {
        "cle_unique":             [r"MATRICULE.*CODE.*ORGANISME", r"MATRICULE.*\\|\\|.*CODE"],
        "matricule":              [r"^MATRICULE$", r"^MATRIC$"],
        "nom":                    [r"^NOM$", r"^NOM_PRENOM$", r"^NOM_ET_PRENOM$"],
        "date_naissance":         [r"DATE.*NAISSANCE", r"DATE_NAISS", r"NAISSANCE"],
        "sexe":                   [r"^SEXE$"],
        "situation_matrimoniale": [r"SITUATION.*MATRIMONIALE", r"STATUT.*MATRIMONIAL"],
        "nombre_enfant":          [r"NOMBRE.*ENFANT", r"NBR.*ENFANT"],
        "situation":              [r"^SITUATION$", r"SITUATION_ADMINISTRATIVE"],
        "date_debut_situation":   [r"DATE.*DEBUT.*SITUATION", r"DATE_SITUATION"],
        "montant_brut":           [r"MONTANT_BRUT", r"SALAIRE_BRUT", r"REMUNERATION_BRUTE", r"^BRUT$"],
        "montant_net":            [r"MONTANT_NET", r"SALAIRE_NET", r"REMUNERATION_NETTE", r"^NET$"],
        "retenue_pension":        [r"RETENUE.*PENSION", r"^PENSION$", r"COTISATION.*PENSION"],
        "impot":                  [r"^IMPOT$", r"^IGR$", r"IMPOT.*REVENU"],
        "charge_patronale":       [r"CHARGE.*PATRONALE", r"CHARGES.*PATRONALES"],
        "organisme":              [r"^ORGANISME$", r"MINISTERE", r"DIRECTION"],
        "lieu_affectation":       [r"LIEU.*AFFECTATION", r"^AFFECTATION$"],
        "service":                [r"^SERVICE$"],
        "emploi":                 [r"^EMPLOI$", r"^CORPS$", r"CORPS.*EMPLOI"],
        "fonction":               [r"^FONCTION$"],
        "grade":                  [r"CLASSE.*ECHELON", r"CLASSE_ECHELON"],
        "statut_fonctionnaire":   [r"^GRADE$"],
        "poste":                  [r"^POSTE$", r"LIBELLE.*POSTE"],
        "prise_service":          [r"PRISE.*SERVICE", r"DATE.*PRISE.*SERVICE"],
        "date_retraite":          [r"DATE.*RETRAITE"],
        "age_retraite":           [r"AGE.*RETRAITE"],
        "mois_annee":             [r"MOIS.*ANNEE", r"^PERIODE$"],
    }
    correspondance = {}
    for nom in noms_colonnes:
        nom_norm = normaliser_nom_colonne(nom)
        matched  = None
        for std_name, patterns in mapping.items():
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
    situations_valides = {
        "en_activite":      ["EN ACTIVITE", "ACTIVITE"],
        "regul_indemnites": ["REGUL. INDEMNITES", "REGUL INDEMNITES", "REGULARISATION INDEMNITES"],
        "demi_solde":       ["DEMI-SOLDE", "DEMI SOLDE", "DEMISOLDE", "1/2 SOLDE"],
    }
    for categorie, valeurs in situations_valides.items():
        if sit in valeurs:
            return categorie
    return "autre"


# ============================================================
# CHARGEMENT DES TABLES ANSTAT
# ============================================================

print("=" * 70)
print("CHARGEMENT : Tables de codes ANSTAT (depuis staging)")
print("=" * 70)
print()

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
print(f"✓ Fichier ANSTAT téléchargé : {os.path.basename(fichier_anstat_key)}\n")


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
    print(f"  {nom_code:<25} : {nb:>4} codes{detail}")

os.unlink(tmp_anstat)

# ============================================================
# LISTER LES FICHIERS BRONZE (un par mois/période)
# ============================================================

print()
print("=" * 70)
print("LECTURE BRONZE")
print("=" * 70)
print()

pages_bronze = s3.get_paginator("list_objects_v2").paginate(
    Bucket=BUCKET_BRONZE, Prefix=PREFIX_BRONZE
)
cles_bronze = sorted([
    obj["Key"]
    for page in pages_bronze
    for obj in page.get("Contents", [])
    if obj["Key"].endswith(".parquet")
])

print(f"Fichiers Parquet Bronze trouvés : {len(cles_bronze)}\n")

# ============================================================
# TRAITEMENT MOIS PAR MOIS + REGROUPEMENT PAR ANNÉE
# ============================================================
# On traite chaque fichier Bronze mensuel individuellement pour
# pouvoir calculer le taux de matching ANSTAT par période,
# puis on regroupe par année pour l'écriture Silver.

frames_par_annee: dict[str, list[pl.DataFrame]] = {}
stats_matching_par_periode: list[dict] = []

nb_ok  = 0
nb_err = 0

for cle in cles_bronze:
    periode = os.path.splitext(os.path.basename(cle))[0]
    # Extraire l'année depuis le nom de fichier (format MMYYYY ou YYYY-MM)
    m = re.search(r"20\d{2}", periode)
    annee = m.group() if m else periode[:4]

    print(f"  [{periode}] Lecture... ", end="", flush=True)

    try:
        df = lire_parquet_s3(BUCKET_BRONZE, cle)
        print(f"✓  {len(df):,} lignes", flush=True)

        # --- Mapping colonnes ---
        correspondance = mapper_colonnes(list(df.columns))
        # Déduplication des noms standards
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

        # --- Forcer mois_annee depuis le nom de fichier (source fiable) ---
        # Ne pas faire confiance à la valeur interne : certains fichiers bronze
        # sont des doublons d'autres périodes et contiennent un mois_annee erroné.
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
            print(f"    ⚠️  Aucune ligne conservée après filtrage situations")
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
            # Ajouter les colonnes extras si présentes
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

            # Calcul du taux de matching pour cette période
            taux = 100 * df[nom_code].is_not_null().sum() / len(df)
            row_stats[f"{nom_code}_taux"] = round(taux, 1)

            # Log des codes principaux
            if nom_code in CODES_A_SURVEILLER:
                print(f"      {nom_code} : {taux:.1f}%", flush=True)

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

        # Accumulation par année
        if annee not in frames_par_annee:
            frames_par_annee[annee] = []
        frames_par_annee[annee].append(df)
        nb_ok += 1

    except Exception as e:
        print(f"✗ ERREUR : {e}")
        nb_err += 1

    gc.collect()

# ============================================================
# ÉCRITURE SILVER (un fichier par année)
# ============================================================

print()
print("=" * 70)
print("ÉCRITURE SILVER (par année)")
print("=" * 70)
print()

for annee, frames in sorted(frames_par_annee.items()):
    df_annee = pl.concat(frames, how="diagonal")
    key_silver = f"{PREFIX_SILVER}/{annee}.parquet"
    ecrire_parquet_s3(df_annee, BUCKET_SILVER, key_silver)
    del df_annee, frames
    gc.collect()

# ============================================================
# RAPPORT MATCHING ANSTAT PAR PÉRIODE
# ============================================================

print()
print("=" * 70)
print("RAPPORT : Taux de matching ANSTAT par période")
print("=" * 70)
print()

if stats_matching_par_periode:
    df_stats = pl.DataFrame(stats_matching_par_periode)

    # Afficher les colonnes surveillées
    for code in CODES_A_SURVEILLER:
        col = f"{code}_taux"
        if col in df_stats.columns:
            taux_vals = df_stats[col].drop_nulls()
            if len(taux_vals) > 0:
                print(f"  {code} — min: {taux_vals.min():.1f}%  "
                      f"max: {taux_vals.max():.1f}%  "
                      f"moy: {taux_vals.mean():.1f}%")

    sauvegarder_csv_staging(df_stats, "taux_matching_anstat_par_periode.csv")

# ============================================================
# RÉSUMÉ FINAL
# ============================================================

print()
print("=" * 70)
print("RÉSUMÉ")
print("=" * 70)
print()
print(f"Périodes traitées avec succès : {nb_ok}")
print(f"Erreurs                       : {nb_err}")
print(f"Années Silver produites       : {len(frames_par_annee)}")
print(f"\nFichiers Silver disponibles dans : s3://{BUCKET_SILVER}/{PREFIX_SILVER}/")
print(f"Rapport matching : s3://{BUCKET_STAGING}/{PREFIX_VALID}/taux_matching_anstat_par_periode.csv")
print("\n✓ SILVER TERMINÉ")
print("Prochaine étape : exécuter 04_validation_silver.py")