# ============================================================
# PANEL ADMIN — DIAGNOSTIC : 3 PÉRIODES MANQUANTES
# ============================================================
# Les périodes 022021, 042025, 062025 sont présentes en bronze
# mais absentes du panel. Ce script lit chaque fichier bronze
# et simule les étapes de filtrage du 03 pour trouver à quel
# moment les lignes disparaissent.
#
# Étapes testées :
#   A. Lecture brute du bronze          → combien de lignes ?
#   B. Après filtrage des situations    → combien restent ?
#   C. Distribution des situations      → quelles valeurs ?
#   D. Nulls sur colonnes clés          → matricule, montant_brut
#   E. Comparaison avec un mois normal  → même année, mois voisin
#
# Dépendances :
#   pip install polars boto3 python-dotenv
# ============================================================

import io
import re
import unicodedata
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import polars as pl

load_dotenv(".env")

MINIO_ENDPOINT   = "http://192.168.1.230:30137"
MINIO_ACCESS_KEY = "datalab-team"
MINIO_SECRET_KEY = "minio-datalabteam123"

# LORSQU'ON TRAVAILLE SUR JHUB
# MINIO_ENDPOINT   = "http://minio.mon-namespace.svc.cluster.local:80"
# MINIO_ACCESS_KEY = "datalab-team"
# MINIO_SECRET_KEY = "minio-datalabteam123"

BUCKET_BRONZE  = "bronze"
BUCKET_STAGING = "staging"
PREFIX_BRONZE  = "panel_admin"
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

def normaliser_situation(situation) -> str:
    """Identique à l'étape 03."""
    if not situation or str(situation).strip() == "":
        return "autre"
    sit = str(situation).strip().upper()
    sit = unicodedata.normalize("NFKD", sit)
    sit = "".join(c for c in sit if not unicodedata.combining(c))
    import re as _re
    sit = _re.sub(r"\s+", " ", sit).strip()
    situations_valides = {
        "en_activite":      ["EN ACTIVITE", "ACTIVITE"],
        "regul_indemnites": ["REGUL. INDEMNITES", "REGUL INDEMNITES",
                             "REGULARISATION INDEMNITES"],
        "demi_solde":       ["DEMI-SOLDE", "DEMI SOLDE", "DEMISOLDE", "1/2 SOLDE"],
    }
    for categorie, valeurs in situations_valides.items():
        if sit in valeurs:
            return categorie
    return "autre"

SITUATIONS_GARDEES = {"en_activite", "regul_indemnites", "demi_solde"}

# ============================================================
# PÉRIODES À ANALYSER + UN MOIS TÉMOIN PAR PÉRIODE
# ============================================================

PERIODES = {
    "022021": "012021",   # mois manquant → mois témoin (voisin normal)
    "042025": "032025",
    "062025": "052025",
}

print("=" * 70)
print("DIAGNOSTIC : PÉRIODES MANQUANTES DANS LE PANEL")
print("=" * 70)

rapport_lignes = []

for periode_bug, periode_temoin in PERIODES.items():
    for periode, est_bug in [(periode_bug, True), (periode_temoin, False)]:
        label = "⚠️  MANQUANT" if est_bug else "   TÉMOIN  "
        key   = f"{PREFIX_BRONZE}/{periode}.parquet"

        print()
        print(f"{'='*70}")
        print(f"{label}  [{periode}]")
        print(f"{'='*70}")

        try:
            df = lire_parquet_s3(BUCKET_BRONZE, key)
        except Exception as e:
            print(f"  ✗ Impossible de lire {key} : {e}")
            continue

        nb_brut = len(df)
        print(f"\nA. Lignes brutes dans bronze    : {nb_brut:,}")

        # --- B. Colonne situation administrative (pas matrimoniale) ---
        # Priorité : correspondance exacte "SITUATION"
        # Exclusion explicite de SITUATION_MATRIMONIALE et variantes
        EXCLUSIONS = {"matrimoniale", "matr"}
        toutes_sit = [c for c in df.columns
                      if re.search(r"situation", c, re.IGNORECASE)]
        if len(toutes_sit) > 1:
            print(f"   Colonnes 'situation' disponibles : {toutes_sit}")
        col_sit = next(
            (c for c in df.columns if c.upper() == "SITUATION"),
            None,
        ) or next(
            (c for c in df.columns
             if re.search(r"situation", c, re.IGNORECASE)
             and not any(x in c.lower() for x in EXCLUSIONS)
             and "normalis" not in c.lower()),
            None,
        )
        if col_sit:
            print(f"   Colonne situation retenue    : '{col_sit}'")

        if col_sit is None:
            print(f"   ✗ Colonne situation introuvable")
            print(f"   Colonnes disponibles : {df.columns}")
            rapport_lignes.append({
                "periode": periode,
                "est_bug": est_bug,
                "nb_brut": nb_brut,
                "col_situation": None,
                "nb_apres_filtre": None,
                "pct_garde": None,
                "situation_top1": None,
            })
            continue

        print(f"   Colonne situation trouvée  : '{col_sit}'")

        # --- C. Distribution des situations AVANT normalisation ---
        dist_brut = (
            df.group_by(col_sit)
            .agg(pl.len().alias("n"))
            .sort("n", descending=True)
        )
        print(f"\nC. Distribution situations (brut, avant normalisation) :")
        for row in dist_brut.iter_rows(named=True):
            pct = 100 * row["n"] / nb_brut
            print(f"   {str(row[col_sit]):<40}  {row['n']:>8,}  ({pct:.1f}%)")

        # --- B. Filtrage situations (simulation étape 03) ---
        df = df.with_columns(
            pl.col(col_sit)
            .map_elements(normaliser_situation, return_dtype=pl.Utf8)
            .alias("_sit_norm")
        )

        # Distribution après normalisation
        dist_norm = (
            df.group_by("_sit_norm")
            .agg(pl.len().alias("n"))
            .sort("n", descending=True)
        )
        print(f"\nB. Distribution situations (après normalisation étape 03) :")
        for row in dist_norm.iter_rows(named=True):
            pct   = 100 * row["n"] / nb_brut
            garde = "✓ GARDÉ" if row["_sit_norm"] in SITUATIONS_GARDEES else "✗ filtré"
            print(f"   {row['_sit_norm']:<30}  {row['n']:>8,}  ({pct:.1f}%)  {garde}")

        df_filtre = df.filter(pl.col("_sit_norm").is_in(SITUATIONS_GARDEES))
        nb_apres  = len(df_filtre)
        pct_garde = 100 * nb_apres / nb_brut if nb_brut > 0 else 0

        print(f"\n   → Après filtrage : {nb_apres:,} / {nb_brut:,}  ({pct_garde:.1f}% conservé)")

        if nb_apres == 0:
            print(f"   🚨 CAUSE IDENTIFIÉE : toutes les lignes filtrées par situation !")

        # --- D. Nulls sur colonnes clés ---
        print(f"\nD. Nulls sur colonnes clés (après filtrage) :")
        if nb_apres > 0:
            for col in ["matricule", "MATRICULE", "montant_brut", "MONTANT_BRUT",
                        "mois_annee", "PERIODE"]:
                col_found = next((c for c in df_filtre.columns
                                  if c.lower() == col.lower()), None)
                if col_found:
                    n_null = int(df_filtre[col_found].is_null().sum())
                    pct_null = 100 * n_null / nb_apres
                    flag = "  ⚠️" if pct_null > 10 else ""
                    print(f"   {col_found:<30} : {n_null:>6,} nulls  ({pct_null:.1f}%){flag}")

        # Rapport
        top_sit = dist_brut.row(0, named=True)[col_sit] if len(dist_brut) > 0 else None
        rapport_lignes.append({
            "periode":        periode,
            "est_bug":        est_bug,
            "nb_brut":        nb_brut,
            "col_situation":  col_sit,
            "nb_apres_filtre": nb_apres,
            "pct_garde":      round(pct_garde, 1),
            "situation_top1": str(top_sit),
        })

# ============================================================
# SYNTHÈSE COMPARATIVE
# ============================================================

print()
print("=" * 70)
print("SYNTHÈSE COMPARATIVE")
print("=" * 70)
print()
print(f"{'Période':<10} {'Statut':<12} {'Brut':>10} {'Après filtre':>14} {'% gardé':>9}")
print("-" * 60)
for r in rapport_lignes:
    statut = "⚠️ MANQUANT" if r["est_bug"] else "  témoin"
    apres  = str(r["nb_apres_filtre"]) if r["nb_apres_filtre"] is not None else "N/A"
    pct    = str(r["pct_garde"])       if r["pct_garde"]       is not None else "N/A"
    print(f"{r['periode']:<10} {statut:<12} {r['nb_brut']:>10,} {apres:>14}    {pct:>6}%")

# ============================================================
# SAUVEGARDE
# ============================================================

print()
df_rapport = pl.DataFrame(rapport_lignes)
s3.put_object(
    Bucket=BUCKET_STAGING,
    Key=f"{PREFIX_EXPORTS}/diag_periodes_manquantes_detail.csv",
    Body=df_rapport.write_csv().encode("utf-8"),
)
print(f"✓ Rapport détaillé : "
      f"s3://{BUCKET_STAGING}/{PREFIX_EXPORTS}/diag_periodes_manquantes_detail.csv")
print()
print("=" * 70)
print("ACTION CORRECTIVE")
print("=" * 70)
print("""
Si pct_garde ≈ 0% sur les périodes manquantes mais ~95%+ sur les témoins :
  → Le filtre 'normaliser_situation' élimine tout sur ces mois-là
  → Les valeurs de situation dans ces fichiers sources sont inhabituelles
  → Corriger le mapping dans normaliser_situation() du script 03
  → Relancer : python orchestrateur.py --depuis 03

Si pct_garde est normal mais période toujours absente :
  → Problème dans le 06 (compiler_panel) — vérifier la déduplication
  → Relancer : python orchestrateur.py --depuis 06
""")