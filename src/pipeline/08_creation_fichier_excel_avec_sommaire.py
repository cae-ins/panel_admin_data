# ============================================================
# PANEL ADMIN — ÉTAPE 8 : FICHIER EXCEL AVEC SOMMAIRE STRUCTURÉ
# ============================================================
# Lit le fichier d'indicateurs produit par 07b_calcul_indicateurs.py
# et produit un fichier Excel navigable avec un sommaire à 5 sections.
#
# Chaque section affiche M1 et M2 côte à côte avec Effectif,
# Revenu moyen, P25 et P75 pour chaque approche.
#
# Structure du fichier produit :
#   FEUILLE SOMMAIRE (liens cliquables) :
#     Section 1 : Classification par grand groupe CITP
#     Section 2 : Classification par grade  (M1 + M2)
#     Section 3 : Classification par sexe   (M1 + M2)
#     Section 4 : Multi-postes par grade
#     Section 5 : Barème / Grille salariale (M1 + M2)
#     Annexe    : Modèle ANSTAT 2024 | 2025
#
#   Feuilles de détail :
#     CITP_GG1 … CITP_GG9
#     GRADE_A1 … GRADE_D3       — M1 et M2 côte à côte
#     SEXE_Homme / SEXE_Femme   — M1 et M2 côte à côte
#     MULTI_A1 … MULTI_D3
#     BAREME_*                   — M1 et M2 côte à côte
#     BAREME_G_*                 — barème × grade (M2)
#     BAREME_S_*                 — barème × sexe  (M2)
#     MODELE_ANSTAT_2024 / 2025
#
# Source  : s3://staging/panel_admin/exports_gold/indicateurs_*.xlsx
# Sortie  : s3://staging/panel_admin/exports_gold/indicateurs_*_SOMMAIRE.xlsx
# ============================================================

import io
import os
import re
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import polars as pl
from openpyxl import load_workbook, Workbook
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter

load_dotenv(".env")

# ============================================================
# PARAMÈTRES
# ============================================================

MODE_SALAIRE_BRUT = True
INCLURE_ZEROS     = True
ANNEE_MIN         = 2024   # première année des données (doit correspondre à 07b)
ANNEE_MAX         = None   # None = détecté depuis le dernier fichier produit par 07b


# ============================================================

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "")

BUCKET_STAGING = os.getenv("BUCKET_STAGING", "staging")
PREFIX_EXPORTS = "panel_admin/exports_gold"

_mode_label  = "salaire_brut" if MODE_SALAIRE_BRUT else "revenu_salarial"
_zeros_label = "avec_zeros"   if INCLURE_ZEROS     else "sans_zeros"

CITP_GRANDS_GROUPES = {
    1: "1 - Directeurs, cadres de direction et gérants",
    2: "2 - Professions intellectuelles et scientifiques",
    3: "3 - Professions intermédiaires",
    4: "4 - Employés de type administratif",
    5: "5 - Personnel des services directs aux particuliers, commerçants et vendeurs",
    6: "6 - Agriculteurs et ouvriers qualifiés de l'agriculture, de la sylviculture et de la pêche",
    7: "7 - Métiers qualifiés de l'industrie et de l'artisanat",
    8: "8 - Conducteurs d'installations et de machines, et ouvriers de l'assemblage",
    9: "9 - Professions élémentaires",
}

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

print("\n" + "=" * 70)
print("CRÉATION FICHIER AVEC SOMMAIRE (M1 + M2 côte à côte, P25/P75)")
print("=" * 70 + "\n")

# ============================================================
# DONNÉES IHPC NATIONALES (embarquées — source INS Burkina Faso)
# Série mensuelle 1997–2026 | base T2 2024
# ============================================================

IPC_BASE = (102.81 + 103.84 + 104.56) / 3  # moyenne avril+mai+juin 2024 = 103.74

ipc_dict: dict[tuple[int, int], float] = {
    # 2024 — année de référence (base T2 2024)
    (2024,  1):100.98, (2024,  2):101.80, (2024,  3):102.47, (2024,  4):102.81,
    (2024,  5):103.84, (2024,  6):104.56, (2024,  7):104.92, (2024,  8):105.05,
    (2024,  9):103.88, (2024, 10):103.47, (2024, 11):103.62, (2024, 12):104.02,
    # 2025
    (2025,  1):102.70, (2025,  2):102.50, (2025,  3):103.00, (2025,  4):103.60,
    (2025,  5):103.70, (2025,  6):104.00, (2025,  7):104.00, (2025,  8):104.20,
    (2025,  9):103.70, (2025, 10):103.80, (2025, 11):103.70, (2025, 12):104.10,
    # 2026 (données partielles)
    (2026,  1):104.20, (2026,  2):104.50, (2026,  3):105.00, (2026,  4):105.00,
    (2026,  6):105.80, (2026,  7):106.00,
}

_ipc_par_annee: dict[int, list[float]] = {}
for (_an, _m), _v in ipc_dict.items():
    _ipc_par_annee.setdefault(_an, []).append(_v)
ipc_annuel: dict[int, float] = {
    _an: round(sum(_vs) / len(_vs), 2)
    for _an, _vs in _ipc_par_annee.items()
}

_trim_mois = {"T1": [1,2,3], "T2": [4,5,6], "T3": [7,8,9], "T4": [10,11,12]}
ipc_trimestriel: dict[tuple[int, str], float] = {}
for _an in set(a for a, _ in ipc_dict):
    for _trim, _mois in _trim_mois.items():
        _vals = [ipc_dict[(_an, m)] for m in _mois if (_an, m) in ipc_dict]
        if _vals:
            ipc_trimestriel[(_an, _trim)] = round(sum(_vals) / len(_vals), 2)

print(f"✓ IHPC chargé : {len(ipc_dict)} observations  |  base T2 2024 = {IPC_BASE:.2f}")
print(f"  IHPC moyen annuel : { {k: v for k, v in sorted(ipc_annuel.items()) if k >= 2022} }")
print(f"  IHPC trimestriel  : { {k: v for k, v in sorted(ipc_trimestriel.items()) if k[0] >= 2024} }")
print()

# ============================================================
# CHARGEMENT
# ============================================================

if ANNEE_MAX is None:
    _motif = re.compile(
        rf"indicateurs_{_mode_label}_{ANNEE_MIN}_(\d{{4}})_{_zeros_label}\.xlsx$"
    )
    _candidats = [
        (o["LastModified"], int(m.group(1)))
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=BUCKET_STAGING, Prefix=PREFIX_EXPORTS)
        for o in page.get("Contents", [])
        if (m := _motif.search(o["Key"]))
    ]
    if not _candidats:
        raise FileNotFoundError(
            f"Aucun fichier indicateurs_{_mode_label}_{ANNEE_MIN}_XXXX_{_zeros_label}.xlsx "
            f"dans s3://{BUCKET_STAGING}/{PREFIX_EXPORTS} — lancer 07b d'abord."
        )
    ANNEE_MAX = max(_candidats)[1]
    print(f"✓ ANNEE_MAX détectée depuis la dernière sortie 07b : {ANNEE_MAX}")

ANNEES_ANSTAT = list(range(ANNEE_MIN, ANNEE_MAX + 1))
_base_name    = f"indicateurs_{_mode_label}_{ANNEE_MIN}_{ANNEE_MAX}_{_zeros_label}"
INPUT_KEY     = f"{PREFIX_EXPORTS}/{_base_name}.xlsx"
OUTPUT_KEY    = f"{PREFIX_EXPORTS}/{_base_name}_SOMMAIRE.xlsx"
print(f"  Source : s3://{BUCKET_STAGING}/{INPUT_KEY}")

print("✓ Chargement du fichier source...")
buf = io.BytesIO()
try:
    s3.download_fileobj(BUCKET_STAGING, INPUT_KEY, buf)
except Exception as e:
    try:
        _objs = s3.list_objects_v2(Bucket=BUCKET_STAGING, Prefix=PREFIX_EXPORTS, MaxKeys=20)
        _keys = [o["Key"] for o in _objs.get("Contents", [])]
        _disp = "\n  ".join(_keys) if _keys else "(aucun fichier trouvé)"
    except Exception:
        _disp = "(impossible de lister)"
    raise FileNotFoundError(
        f"Fichier introuvable : s3://{BUCKET_STAGING}/{INPUT_KEY}\n"
        f"Fichiers disponibles dans {PREFIX_EXPORTS} :\n  {_disp}\n{e}"
    )
buf.seek(0)

wb_source = load_workbook(buf, read_only=True, data_only=True)
feuilles_existantes = wb_source.sheetnames
print(f"Feuilles disponibles ({len(feuilles_existantes)}) :")
for f in feuilles_existantes:
    print(f"  • {f}")
print()
wb_source.close()

buf.seek(0)
xlsx_bytes_source = buf.getvalue()


def lire_feuille(nom: str) -> pl.DataFrame | None:
    if nom not in feuilles_existantes:
        return None
    buf2 = io.BytesIO(xlsx_bytes_source)
    try:
        df = pl.read_excel(buf2, sheet_name=nom)
        print(f"  ✓ {nom} : {len(df):,} lignes")
        return df
    except Exception as e:
        print(f"  ✗ {nom} : {e}")
        return None


print("📖 Lecture des feuilles...")
dt_citp         = lire_feuille("REVENU_CITP_Detail")
dt_grade        = lire_feuille("REVENU_Grade_Detail")       # M1 par grade
dt_grade_m2     = lire_feuille("REVENU_Total_Grade")        # M2 par grade
dt_grade_sexe   = lire_feuille("REVENU_Grade_Sexe")         # M1 par grade × sexe
dt_multi        = lire_feuille("MULTI_Par_Grade_NbPostes")
dt_bareme_m1    = lire_feuille("REVENU_Bareme_M1")          # M1 par barème
dt_bareme       = lire_feuille("REVENU_Total_Bareme")       # M2 par barème
dt_bareme_grade      = lire_feuille("REVENU_Total_Bar_Grade")    # M2 par barème × grade
dt_bareme_sexe       = lire_feuille("REVENU_Total_Bar_Sexe")     # M2 par barème × sexe
dt_grade_sexe_m2     = lire_feuille("REVENU_Total_Grade_Sexe")   # M2 par grade × sexe
dt_sexe_m2           = lire_feuille("REVENU_Total_Sexe")         # M2 par sexe (direct)
dt_citp_m2           = lire_feuille("REVENU_CITP_Total")         # M2 par CITP
dt_sexe_trimestre_m1 = lire_feuille("REVENU_Sexe_Trimestre")     # M1 Sexe×Trimestre
dt_sexe_trimestre_m2 = lire_feuille("REVENU_Sexe_Trimestre_M2")  # M2 Sexe×Trimestre
dt_bareme_grade_m1   = lire_feuille("REVENU_Bareme_Grade_M1")    # M1 par barème × grade
dt_bareme_sexe_m1    = lire_feuille("REVENU_Bareme_Sexe_M1")     # M1 par barème × sexe

# M3 = méthode M2 hors MAGISTRAT, GREFFE, CORPS PREF, CORPS DIPL et police (absentes → colonnes M3 vides)
dt_grade_m3          = lire_feuille("REVENU_M3_Grade")
dt_grade_sexe_m3     = lire_feuille("REVENU_M3_Grade_Sexe")
dt_sexe_m3           = lire_feuille("REVENU_M3_Sexe")
dt_citp_m3           = lire_feuille("REVENU_M3_CITP")
dt_sexe_trimestre_m3 = lire_feuille("REVENU_M3_Sexe_Trimestre")
dt_bareme_m3         = lire_feuille("REVENU_M3_Bareme")
dt_bareme_grade_m3   = lire_feuille("REVENU_M3_Bar_Grade")
dt_bareme_sexe_m3    = lire_feuille("REVENU_M3_Bar_Sexe")
M3_DISPONIBLE = dt_grade_m3 is not None
if not M3_DISPONIBLE:
    print("  ℹ️  Feuilles M3 absentes (07b antérieur) — colonnes M3 vides")

dt_anstat: dict[int, pl.DataFrame | None] = {}
for annee in ANNEES_ANSTAT:
    dt_anstat[annee] = lire_feuille(f"MODELE_ANSTAT_{annee}")

BAREME_DISPONIBLE = dt_bareme is not None or dt_bareme_m1 is not None
ANSTAT_DISPONIBLE = any(df is not None for df in dt_anstat.values())
print()

# ============================================================
# FONCTIONS UTILITAIRES
# ============================================================

FILL_TITLE    = PatternFill("solid", fgColor="1F4E78")
FILL_SUBTITLE = PatternFill("solid", fgColor="4472C4")
FILL_HEADER   = PatternFill("solid", fgColor="4F81BD")
FILL_HEADER_M1= PatternFill("solid", fgColor="2E75B6")
FILL_HEADER_M2= PatternFill("solid", fgColor="70AD47")
FILL_HEADER_M3= PatternFill("solid", fgColor="ED7D31")   # orange (M3)
FILL_ANSTAT   = PatternFill("solid", fgColor="1F4E79")

FONT_TITLE    = Font(color="FFFFFF", bold=True, size=14)
FONT_SUBTITLE = Font(color="FFFFFF", bold=True, size=12)
FONT_HEADER   = Font(color="FFFFFF", bold=True, size=11)
FONT_LINK     = Font(color="0563C1", underline="single", size=11)
FONT_ANSTAT   = Font(color="FFFFFF", bold=True, size=11)

ALIGN_CENTER  = Alignment(horizontal="center", vertical="center", wrap_text=True)
ALIGN_LEFT    = Alignment(horizontal="left",   vertical="center")


def style_cell(cell, fill=None, font=None, align=None):
    if fill:  cell.fill      = fill
    if font:  cell.font      = font
    if align: cell.alignment = align


def merge_titre(ws, texte: str, row: int, n_cols: int = 10,
                fill=FILL_TITLE, font=FONT_TITLE) -> None:
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=n_cols)
    cell = ws.cell(row=row, column=1, value=texte)
    style_cell(cell, fill=fill, font=font, align=ALIGN_CENTER)
    ws.row_dimensions[row].height = 28


def lien_interne(ws, row: int, col: int, texte: str, feuille_cible: str) -> None:
    feuille_safe = feuille_cible.replace("'", "''")
    cell = ws.cell(row=row, column=col,
                   value=f'=HYPERLINK("#{feuille_safe}!A1","{texte}")')
    style_cell(cell, font=FONT_LINK, align=ALIGN_LEFT)


def ecrire_entetes(ws, entetes: list[str], row: int,
                   fills: list | None = None) -> None:
    for j, h in enumerate(entetes, start=1):
        cell = ws.cell(row=row, column=j, value=h)
        fill = fills[j - 1] if fills and j - 1 < len(fills) else FILL_HEADER
        style_cell(cell, fill=fill, font=FONT_HEADER, align=ALIGN_CENTER)


def ecrire_df_dans_feuille(ws, df: pl.DataFrame, start_row: int = 4,
                            fills: list | None = None) -> None:
    ecrire_entetes(ws, df.columns, start_row, fills=fills)
    for i, row_data in enumerate(df.iter_rows(named=True), start=start_row + 1):
        for j, col in enumerate(df.columns, start=1):
            ws.cell(row=i, column=j, value=row_data[col])
    ws.freeze_panes = f"A{start_row + 1}"


def ajouter_lien_retour(ws, row: int = 2) -> None:
    cell = ws.cell(row=row, column=1,
                   value='=HYPERLINK("#SOMMAIRE!A1","← Retour au sommaire")')
    style_cell(cell, font=FONT_LINK, align=ALIGN_LEFT)


def ajuster_largeurs(ws, largeurs: dict[int, int]) -> None:
    for col_idx, width in largeurs.items():
        ws.column_dimensions[get_column_letter(col_idx)].width = width


# ============================================================
# FONCTIONS STATISTIQUES
# ============================================================

STAT_COLS = ["Effectif", "Revenu_moyen", "Revenu_p25", "Revenu_p75"]


def deflater(rev_nominal, ipc_t):
    """Revenu réel = Rev_nominal × (IPC_base / IPC_t). Retourne None si données manquantes."""
    if rev_nominal is None or ipc_t is None or ipc_t == 0:
        return None
    return round(rev_nominal * IPC_BASE / ipc_t)


def stat_pondere(sub: pl.DataFrame, col: str, eff_col: str = "Effectif") -> float | None:
    if col not in sub.columns or eff_col not in sub.columns:
        return None
    total = sub[eff_col].sum()
    if total == 0:
        return None
    return round(float((sub[eff_col] * sub[col]).sum() / total))


def stats_groupe(df: pl.DataFrame | None, col_groupe: str,
                 val_groupe: str) -> dict:
    """Statistiques agrégées (toutes périodes) pour un groupe donné.
    Effectif = moyenne mensuelle (évite de gonfler par le nombre de mois)."""
    if df is None or col_groupe not in df.columns:
        return {}
    sub = df.filter(pl.col(col_groupe) == val_groupe)
    if len(sub) == 0 or "Effectif" not in sub.columns or sub["Effectif"].sum() == 0:
        return {}
    return {
        "eff": int(round(sub["Effectif"].mean())),
        "rev": stat_pondere(sub, "Revenu_moyen"),
        "p25": stat_pondere(sub, "Revenu_p25"),
        "p75": stat_pondere(sub, "Revenu_p75"),
    }


def agreger_par(df: pl.DataFrame | None, group_cols: list[str]) -> pl.DataFrame:
    """Agrège df sur group_cols avec moyennes pondérées des stats."""
    if df is None or len(df) == 0:
        return pl.DataFrame()
    agg = [pl.col("Effectif").sum()]
    for col in ["Revenu_moyen", "Revenu_p25", "Revenu_p75"]:
        if col in df.columns:
            agg.append((pl.col("Effectif") * pl.col(col)).sum().alias(f"_n_{col}"))
    result = df.group_by(group_cols).agg(agg).sort(group_cols)
    for col in ["Revenu_moyen", "Revenu_p25", "Revenu_p75"]:
        if f"_n_{col}" in result.columns:
            result = result.with_columns(
                (pl.col(f"_n_{col}") / pl.col("Effectif")).round(0).alias(col)
            ).drop(f"_n_{col}")
    return result


def fusionner_m1_m2(df_m1: pl.DataFrame | None,
                    df_m2: pl.DataFrame | None,
                    cle_cols: list[str],
                    df_m3: pl.DataFrame | None = None) -> pl.DataFrame:
    """Joint M1, M2 (et M3 si fourni) sur cle_cols, ajoute suffixes _M1 / _M2 / _M3."""
    stat_c = ["Effectif", "Revenu_moyen", "Revenu_median",
              "Revenu_min", "Revenu_max", "Revenu_p25", "Revenu_p75"]

    def _prep(df, suffix):
        if df is None or len(df) == 0:
            return None
        keep = [c for c in cle_cols if c in df.columns]
        ren  = {c: f"{c}_{suffix}" for c in stat_c if c in df.columns}
        cols = keep + list(ren.keys())
        return df.select([c for c in cols if c in df.columns]).rename(ren)

    parts = [p for p in (_prep(df_m1, "M1"), _prep(df_m2, "M2"), _prep(df_m3, "M3"))
             if p is not None]
    if not parts:
        return pl.DataFrame()
    res = parts[0]
    for p in parts[1:]:
        valid_cle = [c for c in cle_cols if c in res.columns and c in p.columns]
        res = res.join(p, on=valid_cle, how="full", coalesce=True)
    valid_cle = [c for c in cle_cols if c in res.columns]
    return res.sort(valid_cle)


def fills_m1_m2(n_cle: int, cols: list[str]) -> list:
    """Génère la liste de fills (bleu M1, vert M2, orange M3) pour les colonnes."""
    fills = [FILL_HEADER] * n_cle
    for c in cols[n_cle:]:
        fills.append(FILL_HEADER_M1 if "_M1" in c else
                     FILL_HEADER_M3 if "_M3" in c else FILL_HEADER_M2)
    return fills


# ============================================================
# PRÉPARATION DES DONNÉES PAR SECTION
# ============================================================

print("🔨 Préparation des données...")


def extract_grand_groupe(code_citp) -> int | None:
    if code_citp is None:
        return None
    chiffres = re.sub(r"[^0-9]", "", str(code_citp))
    if not chiffres:
        return None
    g = int(chiffres[0])
    return g if 1 <= g <= 9 else None


# --- Section 1 : CITP (M1 + M2) ---
stats_citp: dict[int, dict] = {}
if dt_citp is not None and "Code_CITP" in dt_citp.columns:
    dt_citp = dt_citp.with_columns(
        pl.col("Code_CITP")
        .map_elements(extract_grand_groupe, return_dtype=pl.Int32)
        .alias("GRAND_GROUPE")
    )
if dt_citp_m2 is not None and "Code_CITP" in dt_citp_m2.columns:
    dt_citp_m2 = dt_citp_m2.with_columns(
        pl.col("Code_CITP")
        .map_elements(extract_grand_groupe, return_dtype=pl.Int32)
        .alias("GRAND_GROUPE")
    )
if dt_citp_m3 is not None and "Code_CITP" in dt_citp_m3.columns:
    dt_citp_m3 = dt_citp_m3.with_columns(
        pl.col("Code_CITP")
        .map_elements(extract_grand_groupe, return_dtype=pl.Int32)
        .alias("GRAND_GROUPE")
    )
for gg in range(1, 10):
    sub_m1 = (dt_citp.filter(pl.col("GRAND_GROUPE") == gg)
              if dt_citp is not None and "GRAND_GROUPE" in dt_citp.columns else pl.DataFrame())
    sub_m2 = (dt_citp_m2.filter(pl.col("GRAND_GROUPE") == gg)
              if dt_citp_m2 is not None and "GRAND_GROUPE" in dt_citp_m2.columns else pl.DataFrame())
    s1 = {
        "eff": int(sub_m1["Effectif"].sum()) if len(sub_m1) > 0 and "Effectif" in sub_m1.columns else 0,
        "rev": stat_pondere(sub_m1, "Revenu_moyen") if len(sub_m1) > 0 else None,
        "p25": stat_pondere(sub_m1, "Revenu_p25")   if len(sub_m1) > 0 else None,
        "p75": stat_pondere(sub_m1, "Revenu_p75")   if len(sub_m1) > 0 else None,
        "nb_metiers": sub_m1["Code_CITP"].n_unique() if len(sub_m1) > 0 else 0,
    }
    s2 = {
        "eff": int(sub_m2["Effectif"].sum()) if len(sub_m2) > 0 and "Effectif" in sub_m2.columns else 0,
        "rev": stat_pondere(sub_m2, "Revenu_moyen") if len(sub_m2) > 0 else None,
        "p25": stat_pondere(sub_m2, "Revenu_p25")   if len(sub_m2) > 0 else None,
        "p75": stat_pondere(sub_m2, "Revenu_p75")   if len(sub_m2) > 0 else None,
    }
    stats_citp[gg] = {
        "eff_m1": s1["eff"], "rev_m1": s1["rev"], "p25_m1": s1["p25"], "p75_m1": s1["p75"],
        "eff_m2": s2["eff"], "rev_m2": s2["rev"], "p25_m2": s2["p25"], "p75_m2": s2["p75"],
        "nb_metiers": s1["nb_metiers"],
    }
if dt_citp is not None:
    print(f"  ✓ CITP : {sum(1 for v in stats_citp.values() if v['eff_m1'] > 0)} grands groupes (M1)")

# --- Section 2 : Grade (M1 + M2) ---
all_grades: list[str] = []
stats_grade: dict[str, dict] = {}

grade_vals_m1 = (
    sorted(dt_grade["GRADE_PRINCIPAL"].drop_nulls().unique().to_list())
    if dt_grade is not None and "GRADE_PRINCIPAL" in dt_grade.columns else []
)
grade_vals_m2 = (
    sorted(dt_grade_m2["GRADE_PRINCIPAL"].drop_nulls().unique().to_list())
    if dt_grade_m2 is not None and "GRADE_PRINCIPAL" in dt_grade_m2.columns else []
)
all_grades = sorted(set(grade_vals_m1) | set(grade_vals_m2))

for g in all_grades:
    s1 = stats_groupe(dt_grade,    "GRADE_PRINCIPAL", g)
    s2 = stats_groupe(dt_grade_m2, "GRADE_PRINCIPAL", g)
    stats_grade[g] = {
        "eff_m1": s1.get("eff"), "rev_m1": s1.get("rev"),
        "p25_m1": s1.get("p25"), "p75_m1": s1.get("p75"),
        "eff_m2": s2.get("eff"), "rev_m2": s2.get("rev"),
        "p25_m2": s2.get("p25"), "p75_m2": s2.get("p75"),
    }
print(f"  ✓ Grade : {len(all_grades)} grades (M1: {len(grade_vals_m1)}, M2: {len(grade_vals_m2)})")

# --- Section 3 : Sexe (M1 + M2) ---
# M1 par sexe : agréger REVENU_Grade_Sexe (grade × sexe) → sexe
dt_sexe_m1_agg = pl.DataFrame()
if dt_grade_sexe is not None and "SEXE_STD" in dt_grade_sexe.columns:
    dt_sexe_m1_agg = agreger_par(dt_grade_sexe, ["ANNEE", "MOIS_NUM", "MOIS", "SEXE_STD"])

# M2 par sexe : utiliser REVENU_Total_Sexe (direct) — sinon replier sur barème×sexe
dt_sexe_m2_agg = pl.DataFrame()
if dt_sexe_m2 is not None and "SEXE_STD" in dt_sexe_m2.columns:
    dt_sexe_m2_agg = dt_sexe_m2
elif dt_bareme_sexe is not None and "SEXE_STD" in dt_bareme_sexe.columns:
    dt_sexe_m2_agg = agreger_par(dt_bareme_sexe, ["ANNEE", "MOIS_NUM", "MOIS", "SEXE_STD"])

all_sexes: list[str] = sorted(
    set(
        (dt_sexe_m1_agg["SEXE_STD"].unique().to_list() if len(dt_sexe_m1_agg) > 0 else []) +
        (dt_sexe_m2_agg["SEXE_STD"].unique().to_list() if len(dt_sexe_m2_agg) > 0 else [])
    )
)
stats_sexe: dict[str, dict] = {}
for sv in all_sexes:
    s1 = stats_groupe(dt_sexe_m1_agg, "SEXE_STD", sv)
    s2 = stats_groupe(dt_sexe_m2_agg, "SEXE_STD", sv)
    stats_sexe[sv] = {
        "eff_m1": s1.get("eff"), "rev_m1": s1.get("rev"),
        "p25_m1": s1.get("p25"), "p75_m1": s1.get("p75"),
        "eff_m2": s2.get("eff"), "rev_m2": s2.get("rev"),
        "p25_m2": s2.get("p25"), "p75_m2": s2.get("p75"),
    }
print(f"  ✓ Sexe : {len(all_sexes)} catégories")

# --- Section 4 : Multi-postes ---
grades_multi: list[str] = []
if dt_multi is not None and "GRADE_PRINCIPAL" in dt_multi.columns:
    grades_multi = sorted(dt_multi["GRADE_PRINCIPAL"].unique().to_list())
    print(f"  ✓ Multi-postes : {len(grades_multi)} grades")

# --- Section 5 : Barème (M1 + M2) ---
baremes_liste: list[str] = []
stats_bareme: dict[str, dict] = {}

bm1_vals = (
    sorted(dt_bareme_m1["bareme"].drop_nulls().unique().to_list())
    if dt_bareme_m1 is not None and "bareme" in dt_bareme_m1.columns else []
)
bm2_vals = (
    sorted(dt_bareme["bareme"].drop_nulls().unique().to_list())
    if dt_bareme is not None and "bareme" in dt_bareme.columns else []
)
baremes_liste = sorted(set(bm1_vals) | set(bm2_vals))

for b in baremes_liste:
    s1 = stats_groupe(dt_bareme_m1, "bareme", b)
    s2 = stats_groupe(dt_bareme,    "bareme", b)
    stats_bareme[b] = {
        "eff_m1": s1.get("eff"), "rev_m1": s1.get("rev"),
        "p25_m1": s1.get("p25"), "p75_m1": s1.get("p75"),
        "eff_m2": s2.get("eff"), "rev_m2": s2.get("rev"),
        "p25_m2": s2.get("p25"), "p75_m2": s2.get("p75"),
    }
if baremes_liste:
    print(f"  ✓ Barème : {len(baremes_liste)} grilles (M1: {len(bm1_vals)}, M2: {len(bm2_vals)})")
else:
    BAREME_DISPONIBLE = False
    print("  ℹ️  Feuilles barème absentes — section 5 ignorée")

if ANSTAT_DISPONIBLE:
    annees_ok = [a for a in ANNEES_ANSTAT if dt_anstat.get(a) is not None]
    print(f"  ✓ MODELE_ANSTAT : {annees_ok}")

print()

# ============================================================
# FONCTIONS POUR SOMMAIRES PAR ANNÉE
# ============================================================

def calculer_stats(annee: int) -> dict:
    """Recalcule les stats SOMMAIRE pour une seule année (filtre les DFs globaux)."""
    def _filt(df):
        if df is None or len(df) == 0 or "ANNEE" not in df.columns:
            return df
        return df.filter(pl.col("ANNEE") == annee)

    _citp     = _filt(dt_citp)      # dt_citp a déjà GRAND_GROUPE après la prépration globale
    _citp_m2  = _filt(dt_citp_m2)
    _grade    = _filt(dt_grade)
    _grade_m2 = _filt(dt_grade_m2)
    _gs       = _filt(dt_grade_sexe)
    _sv_m2    = _filt(dt_sexe_m2)
    _bsexe    = _filt(dt_bareme_sexe)
    _bm1      = _filt(dt_bareme_m1)
    _bm2      = _filt(dt_bareme)
    _tm1      = _filt(dt_sexe_trimestre_m1)
    _tm2      = _filt(dt_sexe_trimestre_m2)
    _citp_m3  = _filt(dt_citp_m3)
    _grade_m3 = _filt(dt_grade_m3)
    _sv_m3    = _filt(dt_sexe_m3)
    _bm3      = _filt(dt_bareme_m3)
    _tm3      = _filt(dt_sexe_trimestre_m3)

    def _s3(s):
        return {"eff_m3": s.get("eff"), "rev_m3": s.get("rev"),
                "p25_m3": s.get("p25"), "p75_m3": s.get("p75")}

    # --- CITP ---
    _stats_citp: dict[int, dict] = {}
    for gg in range(1, 10):
        sub1 = (_citp.filter(pl.col("GRAND_GROUPE") == gg)
                if _citp is not None and len(_citp) > 0 and "GRAND_GROUPE" in _citp.columns
                else pl.DataFrame())
        sub2 = (_citp_m2.filter(pl.col("GRAND_GROUPE") == gg)
                if _citp_m2 is not None and len(_citp_m2) > 0 and "GRAND_GROUPE" in _citp_m2.columns
                else pl.DataFrame())
        s1 = {
            "eff": int(round(sub1["Effectif"].mean())) if len(sub1) > 0 and "Effectif" in sub1.columns else 0,
            "rev": stat_pondere(sub1, "Revenu_moyen") if len(sub1) > 0 else None,
            "p25": stat_pondere(sub1, "Revenu_p25")   if len(sub1) > 0 else None,
            "p75": stat_pondere(sub1, "Revenu_p75")   if len(sub1) > 0 else None,
            "nb_metiers": sub1["Code_CITP"].n_unique()
                          if len(sub1) > 0 and "Code_CITP" in sub1.columns else 0,
        }
        s2 = {
            "eff": int(round(sub2["Effectif"].mean())) if len(sub2) > 0 and "Effectif" in sub2.columns else 0,
            "rev": stat_pondere(sub2, "Revenu_moyen") if len(sub2) > 0 else None,
            "p25": stat_pondere(sub2, "Revenu_p25")   if len(sub2) > 0 else None,
            "p75": stat_pondere(sub2, "Revenu_p75")   if len(sub2) > 0 else None,
        }
        sub3 = (_citp_m3.filter(pl.col("GRAND_GROUPE") == gg)
                if _citp_m3 is not None and len(_citp_m3) > 0 and "GRAND_GROUPE" in _citp_m3.columns
                else pl.DataFrame())
        s3 = {
            "eff": int(round(sub3["Effectif"].mean())) if len(sub3) > 0 and "Effectif" in sub3.columns else 0,
            "rev": stat_pondere(sub3, "Revenu_moyen") if len(sub3) > 0 else None,
            "p25": stat_pondere(sub3, "Revenu_p25")   if len(sub3) > 0 else None,
            "p75": stat_pondere(sub3, "Revenu_p75")   if len(sub3) > 0 else None,
        }
        _stats_citp[gg] = {
            "eff_m1": s1["eff"], "rev_m1": s1["rev"], "p25_m1": s1["p25"], "p75_m1": s1["p75"],
            "eff_m2": s2["eff"], "rev_m2": s2["rev"], "p25_m2": s2["p25"], "p75_m2": s2["p75"],
            **_s3(s3),
            "nb_metiers": s1["nb_metiers"],
        }

    # --- Grade ---
    _gv1 = (sorted(_grade["GRADE_PRINCIPAL"].drop_nulls().unique().to_list())
            if _grade is not None and len(_grade) > 0 and "GRADE_PRINCIPAL" in _grade.columns else [])
    _gv2 = (sorted(_grade_m2["GRADE_PRINCIPAL"].drop_nulls().unique().to_list())
            if _grade_m2 is not None and len(_grade_m2) > 0 and "GRADE_PRINCIPAL" in _grade_m2.columns else [])
    _all_g = sorted(set(_gv1) | set(_gv2))
    _stats_g: dict[str, dict] = {}
    for g in _all_g:
        s1 = stats_groupe(_grade,    "GRADE_PRINCIPAL", g)
        s2 = stats_groupe(_grade_m2, "GRADE_PRINCIPAL", g)
        _stats_g[g] = {
            "eff_m1": s1.get("eff"), "rev_m1": s1.get("rev"),
            "p25_m1": s1.get("p25"), "p75_m1": s1.get("p75"),
            "eff_m2": s2.get("eff"), "rev_m2": s2.get("rev"),
            "p25_m2": s2.get("p25"), "p75_m2": s2.get("p75"),
            **_s3(stats_groupe(_grade_m3, "GRADE_PRINCIPAL", g)),
        }

    # --- Sexe ---
    _sm1_agg = (agreger_par(_gs, ["ANNEE", "MOIS_NUM", "MOIS", "SEXE_STD"])
                if _gs is not None and len(_gs) > 0 and "SEXE_STD" in _gs.columns
                else pl.DataFrame())
    if _sv_m2 is not None and len(_sv_m2) > 0 and "SEXE_STD" in _sv_m2.columns:
        _sm2_agg = _sv_m2
    elif _bsexe is not None and len(_bsexe) > 0 and "SEXE_STD" in _bsexe.columns:
        _sm2_agg = agreger_par(_bsexe, ["ANNEE", "MOIS_NUM", "MOIS", "SEXE_STD"])
    else:
        _sm2_agg = pl.DataFrame()
    _all_s = sorted(set(
        (_sm1_agg["SEXE_STD"].unique().to_list() if len(_sm1_agg) > 0 else []) +
        (_sm2_agg["SEXE_STD"].unique().to_list() if len(_sm2_agg) > 0 else [])
    ))
    _stats_s: dict[str, dict] = {}
    for sv in _all_s:
        s1 = stats_groupe(_sm1_agg, "SEXE_STD", sv)
        s2 = stats_groupe(_sm2_agg, "SEXE_STD", sv)
        _stats_s[sv] = {
            "eff_m1": s1.get("eff"), "rev_m1": s1.get("rev"),
            "p25_m1": s1.get("p25"), "p75_m1": s1.get("p75"),
            "eff_m2": s2.get("eff"), "rev_m2": s2.get("rev"),
            "p25_m2": s2.get("p25"), "p75_m2": s2.get("p75"),
            **_s3(stats_groupe(_sv_m3, "SEXE_STD", sv)),
        }

    # --- Barème ---
    _bv1 = (sorted(_bm1["bareme"].drop_nulls().unique().to_list())
            if _bm1 is not None and len(_bm1) > 0 and "bareme" in _bm1.columns else [])
    _bv2 = (sorted(_bm2["bareme"].drop_nulls().unique().to_list())
            if _bm2 is not None and len(_bm2) > 0 and "bareme" in _bm2.columns else [])
    _bl = sorted(set(_bv1) | set(_bv2))
    _stats_b: dict[str, dict] = {}
    for b in _bl:
        s1 = stats_groupe(_bm1, "bareme", b)
        s2 = stats_groupe(_bm2, "bareme", b)
        _stats_b[b] = {
            "eff_m1": s1.get("eff"), "rev_m1": s1.get("rev"),
            "p25_m1": s1.get("p25"), "p75_m1": s1.get("p75"),
            "eff_m2": s2.get("eff"), "rev_m2": s2.get("rev"),
            "p25_m2": s2.get("p25"), "p75_m2": s2.get("p75"),
            **_s3(stats_groupe(_bm3, "bareme", b)),
        }

    return {
        "stats_citp":    _stats_citp,
        "stats_grade":   _stats_g,
        "all_grades":    _all_g,
        "stats_sexe":    _stats_s,
        "all_sexes":     _all_s,
        "stats_bareme":  _stats_b,
        "baremes_liste": _bl,
        "trim_m1":       _tm1,
        "trim_m2":       _tm2,
        "trim_m3":       _tm3,
    }


def _ecrire_bloc(ws, row: int, col: int, s: dict, m: str, ipc) -> None:
    """Écrit Effectif, Rev. moyen, Rev. réel, P25, P75 de la méthode m (m1/m2/m3) à partir de col."""
    ws.cell(row=row, column=col,     value=s.get(f"eff_{m}"))
    ws.cell(row=row, column=col + 1, value=s.get(f"rev_{m}"))
    ws.cell(row=row, column=col + 2, value=deflater(s.get(f"rev_{m}"), ipc))
    ws.cell(row=row, column=col + 3, value=s.get(f"p25_{m}"))
    ws.cell(row=row, column=col + 4, value=s.get(f"p75_{m}"))


def _ecrire_3_blocs(ws, row: int, col: int, s: dict, ipc) -> None:
    for i, m in enumerate(("m1", "m2", "m3")):
        _ecrire_bloc(ws, row, col + 5 * i, s, m, ipc)


def ecrire_sommaire_ws(ws, titre: str, d: dict, bareme_dispo: bool, ipc_t: float | None = None) -> None:
    """Écrit les sections 1-5 + 4b d'un SOMMAIRE (M1, M2, M3 côte à côte).
    ipc_t : IPC moyen de l'année — utilisé pour calculer le revenu réel (Rev_réel = Rev × IPC_BASE / ipc_t)."""
    _stats_citp   = d["stats_citp"]
    _all_grades   = d["all_grades"]
    _stats_grade  = d["stats_grade"]
    _all_sexes    = d["all_sexes"]
    _stats_sexe   = d["stats_sexe"]
    _stats_bareme = d["stats_bareme"]
    _baremes      = d["baremes_liste"]
    _tm1          = d["trim_m1"]
    _tm2          = d["trim_m2"]
    _tm3          = d["trim_m3"]

    merge_titre(ws, titre, row=1, n_cols=N_COLS, fill=FILL_TITLE, font=FONT_TITLE)
    ws.merge_cells(f"A2:{get_column_letter(N_COLS)}2")
    _def_m3 = "M3 = M2 hors magistrats, greffiers, corps préfectoral, corps diplomatique et police"
    if ipc_t:
        _leg = (f"Bleu = M1 | Vert = M2 | Orange = M3 ({_def_m3}) | couleur claire = revenu réel  "
                f"[Rev_nominal × (IHPC_base / IHPC_t) = Rev × ({IPC_BASE:.2f} / {ipc_t:.2f})]")
    else:
        _leg = (f"Bleu = M1 (montant_brut_sans_ind_sans_alloc) | Vert = M2 (montant_brut_total) | "
                f"Orange = M3 ({_def_m3}) | IPC non disponible — revenu réel absent")
    ws.cell(row=2, column=1, value=_leg).font = Font(italic=True, size=10)

    crow = 4

    def _eff(s):
        return any((s.get(f"eff_{m}") or 0) > 0 for m in ("m1", "m2", "m3"))

    # Section 1 : CITP
    if any(_eff(v) for v in _stats_citp.values()):
        merge_titre(ws, "📊 SECTION 1 : CLASSIFICATION PAR GRAND GROUPE CITP",
                    row=crow, n_cols=N_COLS, fill=FILL_SUBTITLE, font=FONT_SUBTITLE)
        crow += 1
        ecrire_entetes(ws, ENTETES_CITP, crow, fills=FILLS_CITP)
        crow += 1
        for gg in range(1, 10):
            s = _stats_citp.get(gg, {})
            if not _eff(s):
                continue
            ws.cell(row=crow, column=1, value=gg)
            ws.cell(row=crow, column=2, value=CITP_GRANDS_GROUPES[gg])
            _ecrire_3_blocs(ws, crow, 3, s, ipc_t)
            ws.cell(row=crow, column=18, value=s.get("nb_metiers"))
            crow += 1
        crow += 2

    # Section 2 : Grade
    if _all_grades:
        merge_titre(ws, "📊 SECTION 2 : CLASSIFICATION PAR GRADE",
                    row=crow, n_cols=N_COLS, fill=FILL_SUBTITLE, font=FONT_SUBTITLE)
        crow += 1
        ecrire_entetes(ws, ENTETES_GRADE, crow, fills=FILLS_GRADE)
        crow += 1
        for g in sorted(_stats_grade.keys()):
            cat = (
                "Catégorie A" if g.startswith("A") else
                "Catégorie B" if g.startswith("B") else
                "Catégorie C" if g.startswith("C") else
                "Catégorie D" if g.startswith("D") else "Autre"
            )
            ws.cell(row=crow, column=1, value=g)
            ws.cell(row=crow, column=2, value=cat)
            _ecrire_3_blocs(ws, crow, 3, _stats_grade[g], ipc_t)
            crow += 1
        crow += 2

    # Section 3 : Sexe
    if _all_sexes:
        merge_titre(ws, "📊 SECTION 3 : CLASSIFICATION PAR SEXE",
                    row=crow, n_cols=N_COLS, fill=FILL_SUBTITLE, font=FONT_SUBTITLE)
        crow += 1
        ecrire_entetes(ws, ENTETES_SEXE, crow, fills=FILLS_SEXE)
        crow += 1
        for sv in sorted(_all_sexes):
            desc = (
                "Agents masculins" if sv == "Homme" else
                "Agents féminins"  if sv == "Femme" else "Non renseigné"
            )
            ws.cell(row=crow, column=1, value=sv)
            ws.cell(row=crow, column=2, value=desc)
            _ecrire_3_blocs(ws, crow, 3, _stats_sexe[sv], ipc_t)
            crow += 1
        crow += 2

    # Section 4b : Sexe × Trimestre
    def _gt(df, an, trim, sx):
        if df is None or len(df) == 0 or "ANNEE" not in df.columns:
            return {}
        sub = df.filter(
            (pl.col("ANNEE") == an) &
            (pl.col("Trimestre") == trim) &
            (pl.col("SEXE") == sx)
        )
        if len(sub) == 0 or "Effectif" not in sub.columns:
            return {}
        return {
            "eff": int(round(sub["Effectif"].mean())),
            "rev": stat_pondere(sub, "Revenu_moyen"),
            "p25": stat_pondere(sub, "Revenu_p25"),
            "p75": stat_pondere(sub, "Revenu_p75"),
        }

    _ann_t: set[int] = set()
    for _t in (_tm1, _tm2, _tm3):
        if _t is not None and len(_t) > 0 and "ANNEE" in _t.columns:
            _ann_t |= set(_t["ANNEE"].drop_nulls().to_list())
    if _ann_t:
        merge_titre(ws, "📊 SECTION 4b : SALAIRE PAR SEXE ET TRIMESTRE",
                    row=crow, n_cols=N_COLS, fill=FILL_SUBTITLE, font=FONT_SUBTITLE)
        crow += 1
        ecrire_entetes(ws, ENTETES_TRIMESTRE, crow, fills=FILLS_TRIMESTRE)
        crow += 1
        for _an2 in sorted(_ann_t):
            for _trim in ["T1", "T2", "T3", "T4"]:
                for _sx in ["Ensemble", "Femme", "Homme"]:
                    s = {}
                    for m, _t in (("m1", _tm1), ("m2", _tm2), ("m3", _tm3)):
                        for k, v in _gt(_t, _an2, _trim, _sx).items():
                            s[f"{k}_{m}"] = v
                    if not s:
                        continue
                    ws.cell(row=crow, column=1, value=_an2)
                    ws.cell(row=crow, column=2, value=_trim)
                    ws.cell(row=crow, column=3, value=_sx)
                    _ecrire_3_blocs(ws, crow, 4, s, ipc_trimestriel.get((_an2, _trim)))
                    crow += 1
        crow += 2

    # Section 5 : Barème
    if bareme_dispo and _baremes:
        merge_titre(ws, "📊 SECTION 5 : CLASSIFICATION PAR BARÈME / GRILLE SALARIALE",
                    row=crow, n_cols=N_COLS, fill=FILL_SUBTITLE, font=FONT_SUBTITLE)
        crow += 1
        ecrire_entetes(ws, ENTETES_BAREME, crow, fills=FILLS_BAREME)
        crow += 1
        for b in _baremes:
            ws.cell(row=crow, column=1, value=b)
            _ecrire_3_blocs(ws, crow, 2, _stats_bareme[b], ipc_t)
            ws.cell(row=crow, column=17, value="→ par grade")
            ws.cell(row=crow, column=18, value="→ par sexe")
            crow += 1
        crow += 2

    ajuster_largeurs(ws, {1: 38, 2: 20, **{i: 15 for i in range(3, N_COLS + 1)}})


# ============================================================
# COLONNES SOMMAIRE (M1 + M2 + M3, P25/P75)
# ============================================================

_REEL = "(FCFA 2024-T2)"


def _entetes_bloc(m: str) -> list[str]:
    return [f"Effectif ({m})", f"Rev. moyen ({m})", f"Rev. réel {m}\n{_REEL}", f"P25 ({m})", f"P75 ({m})"]


_ENTETES_3_BLOCS = _entetes_bloc("M1") + _entetes_bloc("M2") + _entetes_bloc("M3")

ENTETES_CITP      = ["Code", "Grand Groupe CITP"] + _ENTETES_3_BLOCS + ["Nb métiers"]
ENTETES_GRADE     = ["Grade", "Description"] + _ENTETES_3_BLOCS
ENTETES_SEXE      = ["Sexe", "Description"] + _ENTETES_3_BLOCS
ENTETES_TRIMESTRE = ["Année", "Trimestre", "Sexe"] + _ENTETES_3_BLOCS
ENTETES_BAREME    = ["Barème / Grille salariale"] + _ENTETES_3_BLOCS + ["Détail grade", "Détail sexe"]

N_COLS = 18  # max colonnes dans le fichier (3 blocs M1 / M2 / M3 de 5 colonnes)

FILL_HEADER_REEL    = PatternFill("solid", fgColor="9DC3E6")  # bleu clair (M1 réel)
FILL_HEADER_REEL_M2 = PatternFill("solid", fgColor="A9D18E")  # vert clair (M2 réel)
FILL_HEADER_REEL_M3 = PatternFill("solid", fgColor="F8CBAD")  # orange clair (M3 réel)

_FILLS_3_BLOCS = (
    [FILL_HEADER_M1, FILL_HEADER_M1, FILL_HEADER_REEL,    FILL_HEADER_M1, FILL_HEADER_M1] +
    [FILL_HEADER_M2, FILL_HEADER_M2, FILL_HEADER_REEL_M2, FILL_HEADER_M2, FILL_HEADER_M2] +
    [FILL_HEADER_M3, FILL_HEADER_M3, FILL_HEADER_REEL_M3, FILL_HEADER_M3, FILL_HEADER_M3]
)
FILLS_CITP      = [FILL_HEADER] * 2 + _FILLS_3_BLOCS + [FILL_HEADER]
FILLS_GRADE     = [FILL_HEADER] * 2 + _FILLS_3_BLOCS
FILLS_SEXE      = FILLS_GRADE
FILLS_TRIMESTRE = [FILL_HEADER] * 3 + _FILLS_3_BLOCS
FILLS_BAREME    = [FILL_HEADER] * 1 + _FILLS_3_BLOCS + [FILL_HEADER] * 2

# ============================================================
# CRÉATION DU WORKBOOK
# ============================================================

print("📄 Création du workbook...")
wb = Workbook()
wb.remove(wb.active)


# ── Feuille IHPC ─────────────────────────────────────────────
ws_ipc = wb.create_sheet("IHPC_DONNEES")
merge_titre(ws_ipc, f"INDICE HARMONISÉ DES PRIX À LA CONSOMMATION (national) — base T2 2024 = {IPC_BASE:.2f}",
            row=1, n_cols=4, fill=FILL_TITLE, font=FONT_TITLE)
ws_ipc.cell(row=2, column=1,
            value=f"IHPC_base = moyenne (avril + mai + juin 2024) = (102.81 + 103.84 + 104.56) / 3 = {IPC_BASE:.2f}")
ws_ipc.cell(row=2, column=1).font = Font(italic=True, size=10)

# En-têtes données mensuelles
FILL_IPC = PatternFill("solid", fgColor="4472C4")
for col, txt in [(1, "Année"), (2, "Mois"), (3, "IHPC mensuel"), (4, "IHPC moyen annuel")]:
    c = ws_ipc.cell(row=4, column=col, value=txt)
    style_cell(c, fill=FILL_IPC, font=FONT_HEADER, align=ALIGN_CENTER)

_ipc_sorted = sorted(ipc_dict.items())  # [(annee, mois), valeur]
_mois_noms  = ["","Janv.","Févr.","Mars","Avr.","Mai","Juin",
               "Juil.","Août","Sept.","Oct.","Nov.","Déc."]
_row_ipc = 5
_an_precedente = None
for (an, mois), val in _ipc_sorted:
    ws_ipc.cell(row=_row_ipc, column=1, value=an)
    ws_ipc.cell(row=_row_ipc, column=2, value=_mois_noms[mois] if mois <= 12 else mois)
    ws_ipc.cell(row=_row_ipc, column=3, value=val)
    if an != _an_precedente:
        _moy = ipc_annuel.get(an)
        ws_ipc.cell(row=_row_ipc, column=4, value=_moy)
        _an_precedente = an
    _row_ipc += 1

# Résumé annuel séparé
_row_ipc += 1
merge_titre(ws_ipc, "RÉSUMÉ : IHPC moyen annuel (utilisé pour la déflation)", row=_row_ipc, n_cols=4,
            fill=FILL_SUBTITLE, font=FONT_SUBTITLE)
_row_ipc += 1
for col, txt in [(1, "Année"), (2, "IHPC moyen annuel"), (3, "IHPC_base"), (4, "Facteur (base / t)")]:
    c = ws_ipc.cell(row=_row_ipc, column=col, value=txt)
    style_cell(c, fill=FILL_IPC, font=FONT_HEADER, align=ALIGN_CENTER)
_row_ipc += 1
for an, moy in sorted(ipc_annuel.items()):
    ws_ipc.cell(row=_row_ipc, column=1, value=an)
    ws_ipc.cell(row=_row_ipc, column=2, value=moy)
    ws_ipc.cell(row=_row_ipc, column=3, value=round(IPC_BASE, 2))
    ws_ipc.cell(row=_row_ipc, column=4, value=round(IPC_BASE / moy, 4) if moy else None)
    _row_ipc += 1

# Moyennes trimestrielles
_row_ipc += 1
merge_titre(ws_ipc, "RÉSUMÉ : IHPC moyen trimestriel (utilisé pour la déflation des trimestres)", row=_row_ipc, n_cols=4,
            fill=FILL_SUBTITLE, font=FONT_SUBTITLE)
_row_ipc += 1
for col, txt in [(1, "Année"), (2, "Trimestre"), (3, "IHPC moyen trimestriel"), (4, "Facteur (base / t)")]:
    c = ws_ipc.cell(row=_row_ipc, column=col, value=txt)
    style_cell(c, fill=FILL_IPC, font=FONT_HEADER, align=ALIGN_CENTER)
_row_ipc += 1
for (an, trim), moy in sorted(ipc_trimestriel.items()):
    ws_ipc.cell(row=_row_ipc, column=1, value=an)
    ws_ipc.cell(row=_row_ipc, column=2, value=trim)
    ws_ipc.cell(row=_row_ipc, column=3, value=moy)
    ws_ipc.cell(row=_row_ipc, column=4, value=round(IPC_BASE / moy, 4) if moy else None)
    _row_ipc += 1

ajuster_largeurs(ws_ipc, {1: 12, 2: 12, 3: 22, 4: 22})
print("  ✓ IHPC_DONNEES")

# ── SOMMAIRES par année ───────────────────────────────────────
print("📋 Création des SOMMAIRES par année...")
for _an_som in ANNEES_ANSTAT:
    _ws_an = wb.create_sheet(f"SOMMAIRE_{_an_som}")
    _d_an  = calculer_stats(_an_som)
    _titre_an = (
        f"INDICATEURS SALAIRES {_an_som} — "
        f"{'Salaire brut' if MODE_SALAIRE_BRUT else 'Revenu salarial'} | "
        f"{'avec zéros' if INCLURE_ZEROS else 'sans zéros'}"
    )
    ecrire_sommaire_ws(_ws_an, _titre_an, _d_an, BAREME_DISPONIBLE, ipc_t=ipc_annuel.get(_an_som))
    print(f"  ✓ SOMMAIRE_{_an_som}")
print()

# ============================================================
# FEUILLES DE DÉTAIL
# ============================================================

print("📄 Création des feuilles détail...")

# ── Section 1 : CITP (M1 + M2 côte à côte) ──────────────────
for gg in range(1, 10):
    sheet_name = f"CITP_GG{gg}"
    sub_m1 = (
        dt_citp.filter(pl.col("GRAND_GROUPE") == gg).drop("GRAND_GROUPE")
        if dt_citp is not None and "GRAND_GROUPE" in dt_citp.columns else None
    )
    sub_m2 = (
        dt_citp_m2.filter(pl.col("GRAND_GROUPE") == gg).drop("GRAND_GROUPE")
        if dt_citp_m2 is not None and "GRAND_GROUPE" in dt_citp_m2.columns else None
    )
    sub_m3 = (
        dt_citp_m3.filter(pl.col("GRAND_GROUPE") == gg).drop("GRAND_GROUPE")
        if dt_citp_m3 is not None and "GRAND_GROUPE" in dt_citp_m3.columns else None
    )
    cle_citp = ["ANNEE", "MOIS_NUM", "MOIS", "Code_CITP", "Metier_CITP"]
    data = fusionner_m1_m2(sub_m1, sub_m2, cle_citp, sub_m3)
    if len(data) == 0:
        continue
    ws = wb.create_sheet(sheet_name)
    merge_titre(ws, CITP_GRANDS_GROUPES[gg], row=1, n_cols=max(len(data.columns), 4))
    f_list = fills_m1_m2(len(cle_citp), data.columns)
    ecrire_df_dans_feuille(ws, data, start_row=4, fills=f_list)
    ajuster_largeurs(ws, {1: 20, 2: 40, **{i: 14 for i in range(3, len(data.columns) + 1)}})
    print(f"  ✓ {sheet_name}")

# ── Section 2 : Grade (M1 + M2 côte à côte) ──────────────────
for g in sorted(all_grades):
    sheet_name = f"GRADE_{g}"
    sub_m1 = (
        dt_grade.filter(pl.col("GRADE_PRINCIPAL") == g)
        if dt_grade is not None and "GRADE_PRINCIPAL" in dt_grade.columns
        else None
    )
    sub_m2 = (
        dt_grade_m2.filter(pl.col("GRADE_PRINCIPAL") == g)
        if dt_grade_m2 is not None and "GRADE_PRINCIPAL" in dt_grade_m2.columns
        else None
    )
    sub_m3 = (
        dt_grade_m3.filter(pl.col("GRADE_PRINCIPAL") == g)
        if dt_grade_m3 is not None and "GRADE_PRINCIPAL" in dt_grade_m3.columns
        else None
    )
    cle = ["ANNEE", "MOIS_NUM", "MOIS", "GRADE_PRINCIPAL"]
    data = fusionner_m1_m2(sub_m1, sub_m2, cle, sub_m3)
    if len(data) == 0:
        continue
    ws = wb.create_sheet(sheet_name)
    merge_titre(ws, f"GRADE {g} — M1 (sans alloc. familiale), M2 (total) et M3 (total hors corps spécifiques)",
                row=1, n_cols=max(len(data.columns), 4))
    f_list = fills_m1_m2(len(cle), data.columns)
    ecrire_df_dans_feuille(ws, data, start_row=4, fills=f_list)
    ajuster_largeurs(ws, {i: 14 for i in range(1, len(data.columns) + 1)})
    print(f"  ✓ {sheet_name}")

# ── Section 3 : Sexe (M1 + M2 côte à côte) ──────────────────
for sv in all_sexes:
    sheet_name = f"SEXE_{sv}"
    cle = ["ANNEE", "MOIS_NUM", "MOIS", "SEXE_STD"]
    sub_m1 = (
        dt_sexe_m1_agg.filter(pl.col("SEXE_STD") == sv)
        if len(dt_sexe_m1_agg) > 0 else None
    )
    sub_m2 = (
        dt_sexe_m2_agg.filter(pl.col("SEXE_STD") == sv)
        if len(dt_sexe_m2_agg) > 0 else None
    )
    sub_m3 = (
        dt_sexe_m3.filter(pl.col("SEXE_STD") == sv)
        if dt_sexe_m3 is not None and "SEXE_STD" in dt_sexe_m3.columns else None
    )
    data = fusionner_m1_m2(sub_m1, sub_m2, cle, sub_m3)
    if len(data) == 0:
        continue
    ws = wb.create_sheet(sheet_name)
    merge_titre(ws, f"SEXE : {sv} — M1, M2 et M3 côte à côte",
                row=1, n_cols=max(len(data.columns), 4))
    f_list = fills_m1_m2(len(cle), data.columns)
    ecrire_df_dans_feuille(ws, data, start_row=4, fills=f_list)
    ajuster_largeurs(ws, {i: 14 for i in range(1, len(data.columns) + 1)})
    print(f"  ✓ {sheet_name}")

# ── Section 4 : Multi-postes ──────────────────────────────────
if dt_multi is not None:
    for g in grades_multi:
        sheet_name = f"MULTI_{g}"
        data = dt_multi.filter(pl.col("GRADE_PRINCIPAL") == g)
        ws = wb.create_sheet(sheet_name)
        merge_titre(ws, f"MULTI-POSTES : {g}", row=1, n_cols=max(len(data.columns), 4))
        ecrire_df_dans_feuille(ws, data, start_row=4)
        ajuster_largeurs(ws, {i: 14 for i in range(1, len(data.columns) + 1)})
        print(f"  ✓ {sheet_name}")

# ── Section 4b : Sexe × Trimestre (M1 + M2) ──────────────────
if dt_sexe_trimestre_m1 is not None or dt_sexe_trimestre_m2 is not None:
    _annees_t: list[int] = sorted({
        *(dt_sexe_trimestre_m1["ANNEE"].drop_nulls().to_list() if dt_sexe_trimestre_m1 is not None and "ANNEE" in dt_sexe_trimestre_m1.columns else []),
        *(dt_sexe_trimestre_m2["ANNEE"].drop_nulls().to_list() if dt_sexe_trimestre_m2 is not None and "ANNEE" in dt_sexe_trimestre_m2.columns else []),
    })
    for _an in _annees_t:
        for _trim in ["T1", "T2", "T3", "T4"]:
            sheet_name = f"TRIM_{_an}_{_trim}"
            _sub1 = (
                dt_sexe_trimestre_m1.filter((pl.col("ANNEE") == _an) & (pl.col("Trimestre") == _trim))
                if dt_sexe_trimestre_m1 is not None else None
            )
            _sub2 = (
                dt_sexe_trimestre_m2.filter((pl.col("ANNEE") == _an) & (pl.col("Trimestre") == _trim))
                if dt_sexe_trimestre_m2 is not None else None
            )
            _sub3 = (
                dt_sexe_trimestre_m3.filter((pl.col("ANNEE") == _an) & (pl.col("Trimestre") == _trim))
                if dt_sexe_trimestre_m3 is not None else None
            )
            cle_trim = ["ANNEE", "Trimestre", "SEXE"]
            data = fusionner_m1_m2(_sub1, _sub2, cle_trim, _sub3)
            if len(data) == 0:
                continue
            ws = wb.create_sheet(sheet_name)
            merge_titre(ws, f"SEXE × TRIMESTRE — {_an} {_trim} (M1, M2 et M3)",
                        row=1, n_cols=max(len(data.columns), 4))
            f_list = fills_m1_m2(len(cle_trim), data.columns)
            ecrire_df_dans_feuille(ws, data, start_row=4, fills=f_list)
            ajuster_largeurs(ws, {i: 14 for i in range(1, len(data.columns) + 1)})
            print(f"  ✓ {sheet_name}")

# ── Feuilles par année ────────────────────────────────────────
for _an in ANNEES_ANSTAT:
    sheet_name = f"ANNEE_{_an}"
    ws = wb.create_sheet(sheet_name)
    merge_titre(ws, f"SYNTHÈSE {_an} — M1, M2 et M3 côte à côte",
                row=1, n_cols=N_COLS, fill=FILL_SUBTITLE, font=FONT_SUBTITLE)
    _r = 4

    def _section_annee(ws, titre, df_m1, df_m2, cle, row_start, df_m3=None):
        if df_m1 is None and df_m2 is None and df_m3 is None:
            return row_start
        _sub1 = df_m1.filter(pl.col("ANNEE") == _an) if df_m1 is not None and "ANNEE" in df_m1.columns else None
        _sub2 = df_m2.filter(pl.col("ANNEE") == _an) if df_m2 is not None and "ANNEE" in df_m2.columns else None
        _sub3 = df_m3.filter(pl.col("ANNEE") == _an) if df_m3 is not None and "ANNEE" in df_m3.columns else None
        data = fusionner_m1_m2(_sub1, _sub2, cle, _sub3)
        if len(data) == 0:
            return row_start
        merge_titre(ws, titre, row=row_start, n_cols=max(len(data.columns), 4),
                    fill=FILL_HEADER, font=Font(color="FFFFFF", bold=True, size=11))
        ecrire_df_dans_feuille(ws, data, start_row=row_start + 1,
                               fills=fills_m1_m2(len(cle), data.columns))
        return row_start + 1 + len(data) + 2

    _citp_m3_sans_gg = (dt_citp_m3.drop("GRAND_GROUPE")
                        if dt_citp_m3 is not None and "GRAND_GROUPE" in dt_citp_m3.columns else dt_citp_m3)
    _r = _section_annee(ws, "Par grade",  dt_grade,    dt_grade_m2,   ["ANNEE", "MOIS_NUM", "MOIS", "GRADE_PRINCIPAL"], _r, dt_grade_m3)
    _r = _section_annee(ws, "Par sexe",   dt_sexe_m1_agg, dt_sexe_m2_agg, ["ANNEE", "MOIS_NUM", "MOIS", "SEXE_STD"], _r, dt_sexe_m3)
    if BAREME_DISPONIBLE:
        _r = _section_annee(ws, "Par barème", dt_bareme_m1, dt_bareme, ["ANNEE", "MOIS_NUM", "MOIS", "bareme"], _r, dt_bareme_m3)
    _r = _section_annee(ws, "Par CITP",   dt_citp,     dt_citp_m2,    ["ANNEE", "MOIS_NUM", "MOIS", "Code_CITP", "Metier_CITP"], _r, _citp_m3_sans_gg)
    ajuster_largeurs(ws, {i: 14 for i in range(1, N_COLS + 1)})
    print(f"  ✓ {sheet_name}")

# ── Section 5 : Barème (M1 + M2 côte à côte) ─────────────────
if BAREME_DISPONIBLE and baremes_liste:
    cle_bareme = ["ANNEE", "MOIS_NUM", "MOIS", "bareme"]

    for b in baremes_liste:
        slug       = re.sub(r"[^A-Za-z0-9]", "_", b)
        sheet_name = f"BAREME_{slug[:25]}"
        sub_m1 = (
            dt_bareme_m1.filter(pl.col("bareme") == b)
            if dt_bareme_m1 is not None and "bareme" in dt_bareme_m1.columns
            else None
        )
        sub_m2 = (
            dt_bareme.filter(pl.col("bareme") == b)
            if dt_bareme is not None and "bareme" in dt_bareme.columns
            else None
        )
        sub_m3 = (
            dt_bareme_m3.filter(pl.col("bareme") == b)
            if dt_bareme_m3 is not None and "bareme" in dt_bareme_m3.columns
            else None
        )
        data = fusionner_m1_m2(sub_m1, sub_m2, cle_bareme, sub_m3)
        if len(data) == 0:
            continue
        ws = wb.create_sheet(sheet_name)
        merge_titre(ws, f"BARÈME : {b} — M1, M2 et M3 côte à côte",
                    row=1, n_cols=max(len(data.columns), 4))
        f_list = fills_m1_m2(len(cle_bareme), data.columns)
        ecrire_df_dans_feuille(ws, data, start_row=4, fills=f_list)
        ajuster_largeurs(ws, {i: 14 for i in range(1, len(data.columns) + 1)})
        print(f"  ✓ {sheet_name}")

    # Barème × Grade (M1 + M2 côte à côte)
    cle_bg = ["ANNEE", "MOIS_NUM", "MOIS", "bareme", "GRADE_PRINCIPAL"]
    for b in baremes_liste:
        slug       = re.sub(r"[^A-Za-z0-9]", "_", b)
        sheet_name = f"BAREME_G_{slug[:20]}"
        sub_m1 = (
            dt_bareme_grade_m1.filter(pl.col("bareme") == b)
            if dt_bareme_grade_m1 is not None and "bareme" in dt_bareme_grade_m1.columns
            else None
        )
        sub_m2 = (
            dt_bareme_grade.filter(pl.col("bareme") == b)
            if dt_bareme_grade is not None and "bareme" in dt_bareme_grade.columns
            else None
        )
        sub_m3 = (
            dt_bareme_grade_m3.filter(pl.col("bareme") == b)
            if dt_bareme_grade_m3 is not None and "bareme" in dt_bareme_grade_m3.columns
            else None
        )
        data = fusionner_m1_m2(sub_m1, sub_m2, cle_bg, sub_m3)
        if len(data) == 0:
            continue
        ws = wb.create_sheet(sheet_name)
        merge_titre(ws, f"BARÈME × GRADE : {b} — M1, M2 et M3 côte à côte",
                    row=1, n_cols=max(len(data.columns), 4))
        f_list = fills_m1_m2(len(cle_bg), data.columns)
        ecrire_df_dans_feuille(ws, data, start_row=4, fills=f_list)
        ajuster_largeurs(ws, {i: 14 for i in range(1, len(data.columns) + 1)})
        print(f"  ✓ {sheet_name}")

    # Barème × Sexe (M1 + M2 côte à côte)
    cle_bs = ["ANNEE", "MOIS_NUM", "MOIS", "bareme", "SEXE_STD"]
    for b in baremes_liste:
        slug       = re.sub(r"[^A-Za-z0-9]", "_", b)
        sheet_name = f"BAREME_S_{slug[:20]}"
        sub_m1 = (
            dt_bareme_sexe_m1.filter(pl.col("bareme") == b)
            if dt_bareme_sexe_m1 is not None and "bareme" in dt_bareme_sexe_m1.columns
            else None
        )
        sub_m2 = (
            dt_bareme_sexe.filter(pl.col("bareme") == b)
            if dt_bareme_sexe is not None and "bareme" in dt_bareme_sexe.columns
            else None
        )
        sub_m3 = (
            dt_bareme_sexe_m3.filter(pl.col("bareme") == b)
            if dt_bareme_sexe_m3 is not None and "bareme" in dt_bareme_sexe_m3.columns
            else None
        )
        data = fusionner_m1_m2(sub_m1, sub_m2, cle_bs, sub_m3)
        if len(data) == 0:
            continue
        ws = wb.create_sheet(sheet_name)
        merge_titre(ws, f"BARÈME × SEXE : {b} — M1, M2 et M3 côte à côte",
                    row=1, n_cols=max(len(data.columns), 4))
        f_list = fills_m1_m2(len(cle_bs), data.columns)
        ecrire_df_dans_feuille(ws, data, start_row=4, fills=f_list)
        ajuster_largeurs(ws, {i: 14 for i in range(1, len(data.columns) + 1)})
        print(f"  ✓ {sheet_name}")

# ── Annexe ANSTAT ─────────────────────────────────────────────
if ANSTAT_DISPONIBLE:
    for annee in ANNEES_ANSTAT:
        df_anstat = dt_anstat.get(annee)
        if df_anstat is None or len(df_anstat) == 0:
            continue
        sheet_name = f"MODELE_ANSTAT_{annee}"
        ws = wb.create_sheet(sheet_name)
        merge_titre(
            ws, f"MODÈLE DE DONNÉES DE SALAIRE DES FONCTIONNAIRES — {annee}",
            row=1, n_cols=max(len(df_anstat.columns), 4),
            fill=FILL_ANSTAT, font=Font(color="FFFFFF", bold=True, size=12),
        )
        ecrire_df_dans_feuille(ws, df_anstat, start_row=4)
        ajuster_largeurs(ws, {1: 55, 2: 22, 3: 22, 4: 22})
        print(f"  ✓ MODELE_ANSTAT_{annee} ({len(df_anstat)} lignes)")

print()

# ============================================================
# SAUVEGARDE
# ============================================================

print("💾 Sauvegarde...")
buf_out    = io.BytesIO()
wb.save(buf_out)
buf_out.seek(0)
xlsx_bytes = buf_out.getvalue()

s3.put_object(
    Bucket=BUCKET_STAGING,
    Key=OUTPUT_KEY,
    Body=xlsx_bytes,
    ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

taille_mb = len(xlsx_bytes) / 1024**2
print(f"\n✅ Fichier créé : s3://{BUCKET_STAGING}/{OUTPUT_KEY}")
print(f"   Taille   : {taille_mb:.1f} MB")
print(f"   Feuilles : {len(wb.sheetnames)}\n")
print("=" * 70)
print("✅ TERMINÉ")
print("=" * 70 + "\n")
print("📊 STRUCTURE :")
print("  SOMMAIRE : sections 1-5 + 4b + par année — M1+M2+M3, P25/P75 côte à côte")
print("  Feuilles CITP_GGx    : M1, M2, M3 fusionnés (bleu / vert / orange)")
print("  Feuilles GRADE_xx    : M1, M2, M3 fusionnés (bleu / vert / orange)")
print("  Feuilles SEXE_xx     : M1, M2, M3 fusionnés (bleu / vert / orange)")
print("  Feuilles TRIM_xxxx_Tx: Sexe × Trimestre M1, M2, M3")
print("  Feuilles BAREME_xx   : M1, M2, M3 fusionnés")
print("  Feuilles BAREME_G_xx / BAREME_S_xx : détail grade / sexe M1, M2, M3")
print(f"  Feuilles ANNEE_xxxx  : synthèse annuelle {ANNEE_MIN}–{ANNEE_MAX} (M1+M2+M3)")
print("  M3 = M2 hors MAGISTRAT, GREFFE, CORPS PREF, CORPS DIPL et police")
print(f"  Feuilles SOMMAIRE_xxxx : sommaire filtré par année")

# ============================================================
# CONTRÔLE M2 ≥ M1 (LOG UNIQUEMENT)
# ============================================================

print()
print("=" * 70)
print("🔍 CONTRÔLE : Revenu_moyen M2 ≥ M1 sur toutes les désagrégations")
print("=" * 70)

_controle_pairs = [
    ("Grade",            dt_grade,            dt_grade_m2,         ["ANNEE","MOIS_NUM","MOIS","GRADE_PRINCIPAL"]),
    ("Grade × Sexe",     dt_grade_sexe,       dt_grade_sexe_m2,    ["ANNEE","MOIS_NUM","MOIS","GRADE_PRINCIPAL","SEXE_STD"]),
    ("Sexe",             dt_sexe_m1_agg,      dt_sexe_m2_agg,      ["ANNEE","MOIS_NUM","MOIS","SEXE_STD"]),
    ("CITP",             dt_citp,             dt_citp_m2,          ["ANNEE","MOIS_NUM","MOIS","Code_CITP","Metier_CITP"]),
    ("Barème",           dt_bareme_m1,        dt_bareme,           ["ANNEE","MOIS_NUM","MOIS","bareme"]),
    ("Barème × Grade",   dt_bareme_grade_m1,  dt_bareme_grade,     ["ANNEE","MOIS_NUM","MOIS","bareme","GRADE_PRINCIPAL"]),
    ("Barème × Sexe",    dt_bareme_sexe_m1,   dt_bareme_sexe,      ["ANNEE","MOIS_NUM","MOIS","bareme","SEXE_STD"]),
    ("Sexe × Trimestre", dt_sexe_trimestre_m1,dt_sexe_trimestre_m2,["ANNEE","Trimestre","SEXE"]),
]

_total_anomalies = 0
_total_paires    = 0

for _nom_ctrl, _df_m1_ctrl, _df_m2_ctrl, _cle_ctrl in _controle_pairs:
    if _df_m1_ctrl is None or _df_m2_ctrl is None:
        print(f"  ⏭  {_nom_ctrl} : données manquantes — ignoré")
        continue
    if isinstance(_df_m1_ctrl, pl.DataFrame) and len(_df_m1_ctrl) == 0:
        print(f"  ⏭  {_nom_ctrl} : DataFrame M1 vide — ignoré")
        continue
    if isinstance(_df_m2_ctrl, pl.DataFrame) and len(_df_m2_ctrl) == 0:
        print(f"  ⏭  {_nom_ctrl} : DataFrame M2 vide — ignoré")
        continue

    _cle_ok = [c for c in _cle_ctrl
               if c in _df_m1_ctrl.columns and c in _df_m2_ctrl.columns]
    if not _cle_ok:
        print(f"  ⏭  {_nom_ctrl} : colonnes clé absentes dans l'un des DFs — ignoré")
        continue

    try:
        _fused_ctrl = fusionner_m1_m2(_df_m1_ctrl, _df_m2_ctrl, _cle_ok)
    except Exception as _e:
        print(f"  ⏭  {_nom_ctrl} : erreur fusion ({_e}) — ignoré")
        continue

    if len(_fused_ctrl) == 0:
        print(f"  ⏭  {_nom_ctrl} : jointure vide — ignoré")
        continue

    if "Revenu_moyen_M1" not in _fused_ctrl.columns or "Revenu_moyen_M2" not in _fused_ctrl.columns:
        print(f"  ⏭  {_nom_ctrl} : colonnes Revenu_moyen absentes après fusion — ignoré")
        continue

    _check_ctrl = _fused_ctrl.filter(
        pl.col("Revenu_moyen_M1").is_not_null() &
        pl.col("Revenu_moyen_M2").is_not_null()
    )
    _n_comp = len(_check_ctrl)
    _total_paires += _n_comp

    _anomalies_ctrl = _check_ctrl.filter(
        pl.col("Revenu_moyen_M2") < pl.col("Revenu_moyen_M1")
    )
    _n_anom = len(_anomalies_ctrl)
    _total_anomalies += _n_anom

    if _n_anom == 0:
        print(f"  ✓ OK  {_nom_ctrl} ({_n_comp} lignes comparées)")
    else:
        print(f"  ⚠️  {_nom_ctrl} : {_n_anom} anomalie(s) sur {_n_comp} lignes")
        for _row in _anomalies_ctrl.sort(_cle_ok).to_dicts():
            _keys_str = "  ".join(f"{k}={_row[k]}" for k in _cle_ok if k in _row)
            _rm1 = _row.get("Revenu_moyen_M1")
            _rm2 = _row.get("Revenu_moyen_M2")
            _pct = round((_rm2 - _rm1) / _rm1 * 100, 1) if _rm1 and _rm1 != 0 else None
            _pct_str = f"  écart={_pct:+.1f}%" if _pct is not None else ""
            print(f"       {_keys_str}  →  M1={_rm1:,.0f}  M2={_rm2:,.0f}{_pct_str}")

print()
if _total_anomalies == 0:
    print(f"✅ CONTRÔLE M2≥M1 : PASS — {_total_paires} lignes vérifiées, aucune anomalie")
else:
    print(f"❌ CONTRÔLE M2≥M1 : FAIL — {_total_anomalies} anomalie(s) sur {_total_paires} lignes")
print()
