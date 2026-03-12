# ============================================================
# PANEL ADMIN — DIAGNOSTIC : PÉRIODES MANQUANTES DANS SILVER
# ============================================================
# Les périodes 022021, 042025, 062025 sont présentes en bronze
# et passent le filtre du 03 (96-100% conservé).
# Ce script vérifie si elles sont présentes dans les fichiers
# silver annuels — ce qui détermine si le bug est dans le 03
# ou plus en amont dans la construction du silver.
#
# Dépendances :
#   pip install polars boto3 python-dotenv
# ============================================================

import io
import os
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

BUCKET_SILVER = "silver"
PREFIX_SILVER = "panel_admin"

s3 = boto3.client(
    "s3",
    endpoint_url          = MINIO_ENDPOINT,
    aws_access_key_id     = MINIO_ACCESS_KEY,
    aws_secret_access_key = MINIO_SECRET_KEY,
    config                = Config(signature_version="s3v4"),
    region_name           = "us-east-1",
    verify                = False,
)

# ============================================================
# PÉRIODES À VÉRIFIER : période manquante → fichier silver
# ============================================================

CIBLES = {
    "022021": "2021.parquet",
    "042025": "2025.parquet",
    "062025": "2025.parquet",
}

print("=" * 70)
print("DIAGNOSTIC : PÉRIODES MANQUANTES DANS LE SILVER")
print("=" * 70)
print()

# Cache pour ne pas lire deux fois le même fichier
silver_cache: dict[str, pl.DataFrame] = {}

resultats = []

for periode, fichier in CIBLES.items():
    cle = f"{PREFIX_SILVER}/{fichier}"

    if fichier not in silver_cache:
        print(f"Lecture silver/{fichier}...", end=" ", flush=True)
        try:
            buf = io.BytesIO()
            s3.download_fileobj(BUCKET_SILVER, cle, buf)
            buf.seek(0)
            silver_cache[fichier] = pl.read_parquet(buf)
            print(f"✓  {len(silver_cache[fichier]):,} lignes × {len(silver_cache[fichier].columns)} cols")
        except Exception as e:
            print(f"✗ ERREUR : {e}")
            silver_cache[fichier] = None

    df = silver_cache[fichier]
    if df is None:
        print(f"  ⚠️  Impossible de lire {fichier}, vérification ignorée.\n")
        continue

    # Chercher la colonne période
    col_periode = next(
        (c for c in df.columns if c.lower() in {"mois_annee", "periode", "mois_annee_str"}),
        None,
    )

    if col_periode is None:
        print(f"  ✗ Colonne période introuvable dans {fichier}")
        print(f"    Colonnes disponibles : {df.columns}")
        continue

    periodes_silver = set(df[col_periode].drop_nulls().unique().to_list())
    present         = periode in periodes_silver

    print()
    print(f"  {'⚠️  MANQUANT' if not present else '✓  PRÉSENT  '}  [{periode}]  dans silver/{fichier}")

    if present:
        sous = df.filter(pl.col(col_periode) == periode)
        print(f"    Lignes         : {len(sous):,}")
        if "matricule" in sous.columns:
            print(f"    Matricules uniq : {sous['matricule'].n_unique():,}")
        if "montant_brut" in sous.columns:
            brut = sous["montant_brut"].drop_nulls()
            print(f"    Brut moyen     : {brut.mean():,.0f}" if len(brut) > 0 else "    Brut : aucune valeur")
    else:
        periodes_triees = sorted(periodes_silver)
        print(f"    Périodes présentes dans silver/{fichier} ({len(periodes_triees)}) :")
        for p in periodes_triees:
            print(f"      {p}")

    resultats.append({
        "periode":         periode,
        "fichier_silver":  fichier,
        "present_silver":  present,
        "nb_periodes_silver": len(periodes_silver),
    })

# ============================================================
# CONCLUSION
# ============================================================

print()
print("=" * 70)
print("CONCLUSION")
print("=" * 70)
print()

tous_absents  = all(not r["present_silver"] for r in resultats)
tous_presents = all(r["present_silver"]     for r in resultats)

if tous_absents:
    print("🚨 Les 3 périodes sont ABSENTES du silver.")
    print()
    print("   Le bug est dans le script 03 (bronze_to_silver).")
    print("   Le filtrage par situation passe bien (96-100% conservé),")
    print("   mais quelque chose d'autre cause la perte à l'écriture silver.")
    print()
    print("   Pistes à investiguer dans le 03 :")
    print("     · Filtre de date ou d'année qui exclut ces périodes")
    print("     · Écriture silver par année : vérifier si 2021 et 2025")
    print("       sont bien inclus dans la liste des années traitées")
    print("     · Crash silencieux sur ces mois spécifiques")
    print()
    print("   ACTION : relancer depuis le 03")
    print("     python orchestrateur.py --depuis 03")

elif tous_presents:
    print("✓ Les 3 périodes sont PRÉSENTES dans le silver.")
    print()
    print("  Le bug est dans le script 06 (compiler_panel).")
    print("  Les données arrivent correctement jusqu'au silver")
    print("  mais disparaissent à la compilation du panel.")
    print()
    print("  Piste la plus probable dans le 06 :")
    print("    · La déduplication unique(subset=['matricule','mois_annee'])")
    print("      avec maintain_order=True dépend de l'ordre de lecture —")
    print("      si ces périodes arrivent en double depuis deux sources,")
    print("      l'une des deux est supprimée.")
    print()
    print("  ACTION : relancer depuis le 06")
    print("    python orchestrateur.py --depuis 06")

else:
    print("⚠️  Résultats mixtes — certaines périodes présentes, d'autres non.")
    for r in resultats:
        statut = "✓ présent" if r["present_silver"] else "✗ absent "
        print(f"  {r['periode']} dans {r['fichier_silver']} : {statut}")
    print()
    print("  Les périodes absentes du silver → relancer depuis le 03.")
    print("  Les périodes présentes dans le silver → investiguer le 06.")