# ============================================================
# PANEL ADMIN — ORCHESTRATEUR BRANCHE No_allocation_Familiale
# ============================================================
# Lance le pipeline d'analyse de la masse salariale
# sans allocation familiale (code poste 421 exclu).
#
# Pipeline :
#   06   Compilation panel individu × période
#        → gold/panel_admin/panel_YYYY.parquet + panel_complet.parquet
#   07   Calcul statuts barème M1 + M2
#        → gold/panel_admin/base_avec_statut.parquet
#   07a  Imputation hors-barème (forward/backward fill)
#        → gold/panel_admin/base_avec_statut_corrigee.parquet
#   10   Correction des trajectoires depuis panel_complet.parquet
#        → gold/panel_admin/trajectoire-pro/panel_trajectoire_clean.parquet
#   11   Salaire par expérience des observations conformes M1/M2
#        → staging/panel_admin/exports_gold/salaire_experience.xlsx
#   07b  Calcul indicateurs salaires (conformes M1 + M2)
#        → staging/.../indicateurs_salaire_brut_2024_2025_avec_zeros.xlsx
#   08   Fichier Excel avec sommaire structuré (5 sections + ANSTAT)
#        → staging/.../indicateurs_salaire_brut_2024_2025_SOMMAIRE.xlsx
#   09   Chargement panel Gold → PostgreSQL (alimente Superset)
#        → table `panel` dans datalab_db
#        Script présent dans ce dossier (09_charger_panel_pg.py)
#   L'ordre après 07a suit les dépendances de 11.
#
# Modes d'exécution :
#   python orchestrateur.py                        → pipeline complet
#   python orchestrateur.py --depuis 07            → reprend à partir de l'étape 07
#   python orchestrateur.py --etapes 07b 08        → étapes spécifiques uniquement
#   python orchestrateur.py --dry-run              → affiche le plan sans exécuter
#
# En cas d'échec d'une étape, le pipeline s'arrête et affiche
# un résumé des étapes réussies / échouées.
#
# Dépendances :
#   pip install polars boto3 python-dotenv openpyxl
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
        "id":          "06",
        "nom":         "Compilation panel",
        "script":      "06_compiler_panel.py",
        "description": "Construction du panel individu × période. "
                       "Calcule montant_brut_total, montant_brut_sans_ind_sans_alloc "
                       "(124+228+271+425, sans alloc. familiale 421), NB_POSTES. "
                       "Produit panel_YYYY.parquet + panel_complet.parquet.",
        "optionnelle": False,
    },
    {
        "id":          "07",
        "nom":         "Calcul statuts barème",
        "script":      "07_calcul_indicateur.py",
        "description": "M1 : Statut (montant_brut_sans_ind_sans_alloc vs [min_bareme, max_bareme]). "
                       "M2 : Statut_Total (montant_brut_total vs [min_total, max_total]). "
                       "Toutes années ; référentiel anstat_2025 — cluster secteur × grade. "
                       "Produit base_avec_statut.parquet.",
        "optionnelle": False,
    },
    {
        "id":          "07a",
        "nom":         "Imputation hors-barème",
        "script":      "07a_imputer_hors_bareme.py",
        "description": "Forward/backward fill M1 + M2 pour les agents avec 1–5 mois "
                       "hors-barème. Restaure la valeur originale si l'imputation "
                       "dépasse les bornes. Produit base_avec_statut_corrigee.parquet.",
        "optionnelle": False,
    },
    {
        "id":          "10",
        "nom":         "Correction trajectoires",
        "script":      "10_correction_gold_trajectoire.py",
        "description": "Corrige prise_service dans panel_complet.parquet et produit "
                       "panel_trajectoire_clean.parquet.",
        "optionnelle": False,
    },
    {
        "id":          "11",
        "nom":         "Salaire × années d'expérience",
        "script":      "11_analyse_salaire_experience.py",
        "description": "Joint le panel trajectoire corrigé (10) et les statuts/"
                       "montants corrigés (07a). Analyse M1 et M2 conformes au barème.",
        "optionnelle": False,
    },
    {
        "id":          "07b",
        "nom":         "Calcul indicateurs salaires",
        "script":      "07b_calcul_indicateurs.py",
        "description": "M1 (Statut conforme) : indicateurs par grade / grade×sexe / CITP / "
                       "multi-postes sur montant_brut_sans_ind_sans_alloc. "
                       "M2 (Statut_Total conforme) : indicateurs par barème / barème×grade / "
                       "barème×sexe sur montant_brut_total. "
                       "Produit indicateurs_salaire_brut_2024_2025_avec_zeros.xlsx.",
        "optionnelle": False,
    },
    {
        "id":          "08",
        "nom":         "Excel avec sommaire structuré",
        "script":      "08_creation_fichier_excel_avec_sommaire.py",
        "description": "Fichier Excel navigable avec sommaire 5 sections + annexe ANSTAT : "
                       "CITP, Grade, Sexe, Multi-postes, Barème/Grille salariale. "
                       "Produit indicateurs_salaire_brut_2024_2025_SOMMAIRE.xlsx.",
        "optionnelle": False,
    },
    {
        "id":          "09",
        "nom":         "Chargement panel → PostgreSQL",
        "script":      "09_charger_panel_pg.py",
        "description": "Charge panel_complet.parquet (Gold) dans la table `panel` "
                       "de PostgreSQL (datalab_db) via DuckDB. "
                       "Prérequis pour le dashboard Superset.",
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
    description="Orchestrateur pipeline No_allocation_Familiale (étapes 06 → 11)",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Exemples :
  python orchestrateur_No_AF.py                   # pipeline complet (06 → 11)
  python orchestrateur.py --depuis 07            # reprend à l'étape 07
  python orchestrateur.py --etapes 07b 08        # étapes spécifiques
  python orchestrateur.py --dry-run              # affiche le plan sans exécuter
  python orchestrateur.py --depuis 07a --dry-run
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
    help="Lance uniquement les étapes spécifiées (ex: --etapes 07b 08)",
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
    ids_demandes  = args.etapes
    ids_invalides = [i for i in ids_demandes if i not in IDS_ETAPES]
    if ids_invalides:
        print(f"❌ IDs d'étapes inconnus : {ids_invalides}")
        print(f"   IDs valides : {IDS_ETAPES}")
        sys.exit(1)
    etapes_selectionnees = [e for e in ETAPES if e["id"] in ids_demandes]
    etapes_selectionnees.sort(key=lambda e: IDS_ETAPES.index(e["id"]))

elif args.depuis:
    if args.depuis not in IDS_ETAPES:
        print(f"❌ ID d'étape inconnu : '{args.depuis}'")
        print(f"   IDs valides : {IDS_ETAPES}")
        sys.exit(1)
    idx_depart           = IDS_ETAPES.index(args.depuis)
    etapes_selectionnees = ETAPES[idx_depart:]

else:
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
print("  PANEL ADMIN — ORCHESTRATEUR No_allocation_Familiale (06 → 11)")
print(f"  Démarrage  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Répertoire : {args.repertoire}")
print(separateur())

afficher_plan(etapes_selectionnees, dry_run=args.dry_run)

if args.dry_run:
    print("ℹ️  Mode DRY-RUN : aucun script n'a été exécuté.")
    sys.exit(0)

# ============================================================
# EXÉCUTION
# ============================================================

resultats    = []
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
        proc  = subprocess.run(
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

ids_lances = {r["id"] for r in resultats}
ids_sautes = [e["id"] for e in etapes_selectionnees if e["id"] not in ids_lances]

nb_succes  = sum(1 for r in resultats if r["statut"] == "SUCCÈS")
nb_echec   = sum(1 for r in resultats if r["statut"] in ("ÉCHEC", "EXCEPTION"))

for r in resultats:
    icone = "✅" if r["statut"] == "SUCCÈS" else "❌"
    print(f"  {icone} [{r['id']:>3}] {r['nom']:<35} {fmt_duree(r['duree']):>8}")

if ids_sautes:
    for eid in ids_sautes:
        e = next(et for et in ETAPES if et["id"] == eid)
        print(f"  ⏭️  [{eid:>3}] {e['nom']:<35} {'non lancée':>8}")

print()
print(f"  Durée totale    : {fmt_duree(duree_totale)}")
print(f"  Étapes réussies : {nb_succes} / {len(resultats)}")

if nb_echec == 0 and not ids_sautes:
    print()
    print("  🎉  Pipeline terminé avec succès.")
    print()
    print("  Fichiers et tables produits :")
    print("    gold/panel_admin/panel_complet.parquet")
    print("    gold/panel_admin/base_avec_statut_corrigee.parquet")
    print("    gold/panel_admin/trajectoire-pro/panel_trajectoire_clean.parquet")
    print("    staging/panel_admin/exports_gold/salaire_experience.xlsx")
    print("    staging/panel_admin/exports_gold/indicateurs_salaire_brut_2024_2025_SOMMAIRE.xlsx")
    print("    PostgreSQL : datalab_db.panel  (alimenté par l'étape 09)")
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
