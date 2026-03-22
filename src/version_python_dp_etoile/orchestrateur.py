# ============================================================
# PANEL ADMIN — ORCHESTRATEUR PRINCIPAL
# ============================================================
# Lance l'ensemble du pipeline de consolidation et d'analyse
# de la masse salariale administrative 2015-2025.
#
# Pipeline :
#   01  Pré-analyse des colonnes (staging)
#   02  Staging → Bronze  (Excel → Parquet brut)
#   03  Bronze → Silver   (mapping, normalisation, ANSTAT)
#   03b Imputation grades Silver (cascade emploi P1/P2/P3)
#   03c Imputation salaires Silver (LOCF + médiane cascade)
#   04  Validation Silver (contrôles qualité)
#   05  Silver → Gold     (agrégations masse salariale + effectifs)
#   06  Compilation panel individu × période
#   07  Calcul des indicateurs salaires
#   08  Fichier Excel avec sommaire structuré
#   09  Chargement panel Gold → PostgreSQL (alimente Superset)
#
# Modes d'exécution :
#   python orchestrateur.py                → pipeline complet
#   python orchestrateur.py --depuis 03    → reprend à partir de l'étape 03
#   python orchestrateur.py --etapes 06 07 08   → étapes spécifiques uniquement
#   python orchestrateur.py --dry-run      → affiche le plan sans exécuter
#
# En cas d'échec d'une étape, le pipeline s'arrête et affiche
# un résumé des étapes réussies / échouées.
#
# Dépendances :
#   pip install polars fastexcel boto3 python-dotenv openpyxl
# ============================================================

import argparse
import subprocess
import sys
import time
import os
from datetime import datetime, timedelta

# ============================================================
# DÉFINITION DES ÉTAPES
# ============================================================

ETAPES = [
    {
        "id":          "01",
        "nom":         "Pré-analyse des colonnes",
        "script":      "01_pre_analyse.py",
        "description": "Analyse la structure des fichiers Excel en staging "
                       "(colonnes F1/F2, codes numériques, variations entre périodes)",
        "optionnelle": False,
    },
    {
        "id":          "02",
        "nom":         "Staging → Bronze",
        "script":      "02_staging_to_bronze.py",
        "description": "Convertit les fichiers Excel mensuels en Parquet Bronze",
        "optionnelle": False,
    },
    {
        "id":          "03",
        "nom":         "Bronze → Silver",
        "script":      "03_bronze_to_silver.py",
        "description": "Mapping colonnes, normalisation situations, enrichissement "
                       "ANSTAT, suivi taux matching par période",
        "optionnelle": False,
    },
    {
        "id":          "03b",
        "nom":         "Imputation grades Silver",
        "script":      "03b_imputer_grades_silver.py",
        "description": "Grade modal par cascade emploi (P1:mois × P2:année × P3:global). "
                       "Ajoute GRADE et GRADE_SOURCE dans le Silver.",
        "optionnelle": False,
    },
    {
        "id":          "03c",
        "nom":         "Imputation salaires Silver",
        "script":      "03c_imputer_salaires_silver.py",
        "description": "LOCF par cle_unique puis médiane cascade "
                       "(CODE_EMPLOI × GRADE × CODE_ORGANISME × mois_annee). "
                       "Ajoute montant_brut_SOURCE et montant_net_SOURCE dans le Silver.",
        "optionnelle": False,
    },
    {
        "id":          "04",
        "nom":         "Validation Silver",
        "script":      "04_validation_silver.py",
        "description": "Contrôles qualité : complétude, NA, doublons, "
                       "cohérence montants, distribution situations",
        "optionnelle": True,   # Peut être sautée si on veut aller vite
    },
    {
        "id":          "05",
        "nom":         "Silver → Gold",
        "script":      "05_silver_to_gold.py",
        "description": "Agrégations mensuelles par organisme × situation",
        "optionnelle": False,
    },
    {
        "id":          "06",
        "nom":         "Compilation panel",
        "script":      "06_compiler_panel.py",
        "description": "Construction du panel individu × période, "
                       "déduplication et tri chronologique",
        "optionnelle": False,
    },
    {
        "id":          "07",
        "nom":         "Calcul indicateurs",
        "script":      "07_calcul_indicateur.py",
        "description": "Indicateurs salaires : P25/P75, grade, sexe, CITP, "
                       "multi-postes, salaire brut par composantes",
        "optionnelle": False,
    },
    {
        "id":          "08",
        "nom":         "Excel avec sommaire",
        "script":      "08_creation_fichier_excel_avec_sommaire.py",
        "description": "Fichier Excel navigable avec sommaire 4 sections "
                       "(CITP, Grade, Sexe, Multi-postes)",
        "optionnelle": False,
    },
    {
        "id":          "09",
        "nom":         "Chargement panel → PostgreSQL",
        "script":      "09_charger_panel_pg.py",
        "description": "Charge panel_complet.parquet (Gold) dans la table panel "
                       "de PostgreSQL via DuckDB. Prérequis pour le dashboard Superset.",
        "optionnelle": False,
    },
]

IDS_ETAPES = [e["id"] for e in ETAPES]

# ============================================================
# UTILITAIRES
# ============================================================

def fmt_duree(secondes: float) -> str:
    td = timedelta(seconds=int(secondes))
    h, rem = divmod(td.seconds, 3600)
    m, s   = divmod(rem, 60)
    if h > 0:
        return f"{h}h{m:02d}m{s:02d}s"
    if m > 0:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def separateur(char: str = "=", largeur: int = 70) -> str:
    return char * largeur


def bandeau(titre: str) -> None:
    print()
    print(separateur())
    print(f"  {titre}")
    print(separateur())
    print()


def afficher_plan(etapes_a_lancer: list[dict], dry_run: bool = False) -> None:
    mode = "DRY-RUN — " if dry_run else ""
    bandeau(f"{mode}PLAN D'EXÉCUTION")
    for i, e in enumerate(etapes_a_lancer, start=1):
        opt = "  (optionnelle)" if e["optionnelle"] else ""
        print(f"  {i:>2}. [{e['id']:>3}] {e['nom']}{opt}")
        print(f"        {e['description']}")
        print()


def verifier_scripts(etapes: list[dict], repertoire: str) -> list[str]:
    """Vérifie que tous les scripts existent."""
    manquants = []
    for e in etapes:
        chemin = os.path.join(repertoire, e["script"])
        if not os.path.isfile(chemin):
            manquants.append(e["script"])
    return manquants

# ============================================================
# ARGUMENTS EN LIGNE DE COMMANDE
# ============================================================

parser = argparse.ArgumentParser(
    description="Orchestrateur du pipeline Panel Admin 2015-2025",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Exemples :
  python orchestrateur.py                        # pipeline complet
  python orchestrateur.py --depuis 03            # reprend à l'étape 03
  python orchestrateur.py --etapes 03b 03c 04    # étapes spécifiques
  python orchestrateur.py --dry-run              # affiche le plan sans exécuter
  python orchestrateur.py --depuis 07 --dry-run
    """,
)
parser.add_argument(
    "--depuis",
    metavar="ID",
    help=f"Reprend à partir de l'étape indiquée. IDs disponibles : {', '.join(IDS_ETAPES)}",
)
parser.add_argument(
    "--etapes",
    nargs="+",
    metavar="ID",
    help="Lance uniquement les étapes spécifiées (ex: --etapes 06b 07 08)",
)
parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Affiche le plan sans exécuter les scripts",
)
parser.add_argument(
    "--repertoire",
    metavar="DIR",
    default=os.path.dirname(os.path.abspath(__file__)),
    help="Répertoire contenant les scripts (défaut : même dossier que cet orchestrateur)",
)

args = parser.parse_args()

# ============================================================
# SÉLECTION DES ÉTAPES
# ============================================================

if args.etapes:
    # Étapes spécifiques
    ids_demandes = args.etapes
    ids_invalides = [i for i in ids_demandes if i not in IDS_ETAPES]
    if ids_invalides:
        print(f"❌ IDs d'étapes inconnus : {ids_invalides}")
        print(f"   IDs valides : {IDS_ETAPES}")
        sys.exit(1)
    etapes_selectionnees = [e for e in ETAPES if e["id"] in ids_demandes]
    # Respecter l'ordre naturel du pipeline
    etapes_selectionnees.sort(key=lambda e: IDS_ETAPES.index(e["id"]))

elif args.depuis:
    # À partir d'une étape
    if args.depuis not in IDS_ETAPES:
        print(f"❌ ID d'étape inconnu : '{args.depuis}'")
        print(f"   IDs valides : {IDS_ETAPES}")
        sys.exit(1)
    idx_depart = IDS_ETAPES.index(args.depuis)
    etapes_selectionnees = ETAPES[idx_depart:]

else:
    # Pipeline complet
    etapes_selectionnees = ETAPES

# ============================================================
# VÉRIFICATION DES SCRIPTS
# ============================================================

manquants = verifier_scripts(etapes_selectionnees, args.repertoire)
if manquants and not args.dry_run:
    print()
    print("❌ Scripts introuvables dans :", args.repertoire)
    for m in manquants:
        print(f"   • {m}")
    print()
    print("Vérifiez le paramètre --repertoire ou placez les scripts dans le même dossier.")
    sys.exit(1)

# ============================================================
# AFFICHAGE DU PLAN
# ============================================================

print()
print(separateur())
print("  PANEL ADMIN — ORCHESTRATEUR PIPELINE 2015-2025")
print(f"  Démarrage : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Répertoire : {args.repertoire}")
print(separateur())

afficher_plan(etapes_selectionnees, dry_run=args.dry_run)

if args.dry_run:
    print("ℹ️  Mode DRY-RUN : aucun script n'a été exécuté.")
    sys.exit(0)

# ============================================================
# EXÉCUTION
# ============================================================

resultats = []
debut_global = time.time()

for etape in etapes_selectionnees:
    chemin_script = os.path.join(args.repertoire, etape["script"])
    debut_etape   = time.time()

    print(separateur("-"))
    print(f"  ▶  [{etape['id']:>3}] {etape['nom']}")
    print(f"       {datetime.now().strftime('%H:%M:%S')} — {etape['script']}")
    print(separateur("-"))
    print()

    try:
        proc = subprocess.run(
            [sys.executable, chemin_script],
            cwd=args.repertoire,
        )
        duree = time.time() - debut_etape

        if proc.returncode == 0:
            statut = "✅ SUCCÈS"
            print()
            print(f"  {statut}  [{etape['id']:>3}] {etape['nom']}  ({fmt_duree(duree)})")
            resultats.append({
                "id":     etape["id"],
                "nom":    etape["nom"],
                "statut": "SUCCÈS",
                "duree":  duree,
                "code":   0,
            })

        else:
            statut = "❌ ÉCHEC"
            print()
            print(f"  {statut}  [{etape['id']:>3}] {etape['nom']}  "
                  f"(code retour : {proc.returncode}  {fmt_duree(duree)})")
            resultats.append({
                "id":     etape["id"],
                "nom":    etape["nom"],
                "statut": "ÉCHEC",
                "duree":  duree,
                "code":   proc.returncode,
            })

            # Arrêt du pipeline sur échec d'une étape non optionnelle
            if not etape["optionnelle"]:
                print()
                print(f"  ⛔  Étape obligatoire en échec — arrêt du pipeline.")
                break
            else:
                print()
                print(f"  ⚠️   Étape optionnelle en échec — pipeline continue.")

    except Exception as exc:
        duree = time.time() - debut_etape
        print()
        print(f"  ❌ EXCEPTION  [{etape['id']:>3}] {etape['nom']} : {exc}")
        resultats.append({
            "id":     etape["id"],
            "nom":    etape["nom"],
            "statut": "EXCEPTION",
            "duree":  duree,
            "code":   -1,
        })
        if not etape["optionnelle"]:
            print(f"  ⛔  Étape obligatoire en exception — arrêt du pipeline.")
            break
        else:
            print(f"  ⚠️   Étape optionnelle en exception — pipeline continue.")

# ============================================================
# RÉSUMÉ FINAL
# ============================================================

duree_totale = time.time() - debut_global

bandeau("RÉSUMÉ D'EXÉCUTION")

ids_lances   = {r["id"] for r in resultats}
ids_sautes   = [e["id"] for e in etapes_selectionnees if e["id"] not in ids_lances]

nb_succes    = sum(1 for r in resultats if r["statut"] == "SUCCÈS")
nb_echec     = sum(1 for r in resultats if r["statut"] in ("ÉCHEC", "EXCEPTION"))

for r in resultats:
    icone = "✅" if r["statut"] == "SUCCÈS" else "❌"
    print(f"  {icone} [{r['id']:>3}] {r['nom']:<35} {fmt_duree(r['duree']):>8}")

if ids_sautes:
    for eid in ids_sautes:
        e = next(et for et in ETAPES if et["id"] == eid)
        print(f"  ⏭️  [{eid:>3}] {e['nom']:<35} {'non lancée':>8}")

print()
print(f"  Durée totale   : {fmt_duree(duree_totale)}")
print(f"  Étapes réussies : {nb_succes} / {len(resultats)}")

if nb_echec == 0 and not ids_sautes:
    print()
    print("  🎉  Pipeline terminé avec succès.")
elif nb_echec == 0 and ids_sautes:
    print()
    print(f"  ⚠️   Pipeline interrompu après échec. "
          f"{len(ids_sautes)} étape(s) non lancée(s).")
    print(f"       Pour reprendre : python orchestrateur.py --depuis {ids_sautes[0]}")
else:
    print()
    print(f"  ❌  Pipeline terminé avec {nb_echec} échec(s).")
    id_premier_echec = next(r["id"] for r in resultats if r["statut"] != "SUCCÈS")
    print(f"       Pour relancer depuis l'échec : "
          f"python orchestrateur.py --depuis {id_premier_echec}")

print()
print(separateur())
print(f"  Fin : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(separateur())
print()
