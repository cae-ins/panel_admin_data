# ============================================================
# PANEL ADMIN — ORCHESTRATEUR PRINCIPAL INDUSTRIALISÉ
# ============================================================
# Lance l'ensemble du pipeline de consolidation et d'analyse
# de la masse salariale administrative 2015-2025.
#
# Cette version industrialisée utilise les scripts optimisés
# du dossier src/pipeline/.
#
# Pipeline :
#   00  Upload staging    (Local -> MinIO)
#   01  Pré-analyse       (Analyse colonnes Staging)
#   02  Staging -> Bronze (Excel -> Parquet)
#   03  Bronze -> Silver  (Mapping & Normalisation)
#   03b Imputation Grades (Cascade emploi)
#   03c Imputation Salaires (LOCF + Médiane)
#   04  Validation Silver (Contrôles Qualité)
#   05  Silver -> Gold    (Agrégations mensuelles)
#   06  Compilation Panel (Panel Individus + Logique No AF)
#   07  Calcul Statuts    (Conformité Barème M1 + M2)
#   07a Imputation Hors-B (Correction temporelle)
#   07b Indicateurs       (Excel technique)
#   08  Rapport Sommaire  (Excel interactif final)
#   09  Chargement PG     (Gold -> PostgreSQL)
#
# Modes d'exécution :
#   python orchestrateur.py                → pipeline complet
#   python orchestrateur.py --depuis 06    → reprend à partir du panel
#   python orchestrateur.py --etapes 07 08 → étapes spécifiques
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

# Dossier contenant les scripts du pipeline
PIPELINE_DIR = "src/pipeline"

ETAPES = [
    {
        "id":          "00",
        "nom":         "Upload Staging",
        "script":      "00_upload_staging.py",
        "description": "Upload des sources Excel locales vers le staging MinIO.",
        "optionnelle": True,
    },
    {
        "id":          "01",
        "nom":         "Pré-analyse",
        "script":      "01_pre_analyse.py",
        "description": "Analyse la structure des fichiers Excel en staging.",
        "optionnelle": False,
    },
    {
        "id":          "02",
        "nom":         "Staging -> Bronze",
        "script":      "02_staging_to_bronze.py",
        "description": "Convertit les fichiers Excel mensuels en Parquet Bronze.",
        "optionnelle": False,
    },
    {
        "id":          "03",
        "nom":         "Bronze -> Silver",
        "script":      "03_bronze_to_silver.py",
        "description": "Mapping colonnes, normalisation et enrichissement ANSTAT.",
        "optionnelle": False,
    },
    {
        "id":          "03b",
        "nom":         "Imputation Grades Silver",
        "script":      "03b_imputer_grades_silver.py",
        "description": "Grade modal par cascade emploi (P1/P2/P3).",
        "optionnelle": False,
    },
    {
        "id":          "03c",
        "nom":         "Imputation Salaires Silver",
        "script":      "03c_imputer_salaires_silver.py",
        "description": "Complétion des salaires par LOCF et médiane cascade.",
        "optionnelle": False,
    },
    {
        "id":          "04",
        "nom":         "Validation Silver",
        "script":      "04_validation_silver.py",
        "description": "Contrôles qualité : complétude, NA, doublons, montants.",
        "optionnelle": True,
    },
    {
        "id":          "05",
        "nom":         "Silver -> Gold",
        "script":      "05_silver_to_gold.py",
        "description": "Agrégations mensuelles par organisme x situation.",
        "optionnelle": False,
    },
    {
        "id":          "06",
        "nom":         "Compilation Panel",
        "script":      "06_compiler_panel.py",
        "description": "Construction du panel individus avec logique No AF (excl. 421).",
        "optionnelle": False,
    },
    {
        "id":          "07",
        "nom":         "Calcul Statuts Barème",
        "script":      "07_calcul_indicateur.py",
        "description": "Calcul de la conformité au barème (M1 et M2).",
        "optionnelle": False,
    },
    {
        "id":          "07a",
        "nom":         "Imputation Hors-Barème",
        "script":      "07a_imputer_hors_bareme.py",
        "description": "Forward/backward fill pour les agents temporairement hors-barème.",
        "optionnelle": False,
    },
    {
        "id":          "07b",
        "nom":         "Calcul Indicateurs Salaires",
        "script":      "07b_calcul_indicateurs.py",
        "description": "Génération des indicateurs statistiques complets (Excel technique).",
        "optionnelle": False,
    },
    {
        "id":          "08",
        "nom":         "Rapport Excel Sommaire",
        "script":      "08_creation_fichier_excel_avec_sommaire.py",
        "description": "Production du fichier Excel final navigable avec sommaire.",
        "optionnelle": False,
    },
    {
        "id":          "09",
        "nom":         "Chargement PostgreSQL",
        "script":      "09_charger_panel_pg.py",
        "description": "Charge le panel Gold final vers PostgreSQL (Superset).",
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
    if h > 0: return f"{h}h{m:02d}m{s:02d}s"
    if m > 0: return f"{m}m{s:02d}s"
    return f"{s}s"

def separateur(char: str = "=", largeur: int = 70) -> str:
    return char * largeur

def verifier_scripts(repertoire: str) -> list[str]:
    manquants = []
    for e in ETAPES:
        chemin = os.path.join(repertoire, e["script"])
        if not os.path.isfile(chemin):
            manquants.append(e["script"])
    return manquants

# ============================================================
# EXÉCUTION
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Orchestrateur Industrialisé du Pipeline Panel Admin 2015-2025",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--depuis", help="Reprendre à partir d'un ID d'étape.")
    parser.add_argument("--etapes", nargs="+", help="Lancer uniquement ces étapes.")
    parser.add_argument("--dry-run", action="store_true", help="Afficher le plan sans exécuter.")
    args = parser.parse_args()

    # Sélection des étapes
    if args.etapes:
        selection = [e for e in ETAPES if e["id"] in args.etapes]
        selection.sort(key=lambda e: IDS_ETAPES.index(e["id"]))
    elif args.depuis:
        if args.depuis not in IDS_ETAPES:
            print(f"❌ ID d'étape inconnu : {args.depuis}")
            sys.exit(1)
        idx = IDS_ETAPES.index(args.depuis)
        selection = ETAPES[idx:]
    else:
        selection = ETAPES

    # Vérification
    manquants = verifier_scripts(PIPELINE_DIR)
    if manquants:
        print(f"❌ Scripts manquants dans {PIPELINE_DIR} : {manquants}")
        sys.exit(1)

    print()
    print(separateur())
    print("  PANEL ADMIN — ORCHESTRATEUR PIPELINE INDUSTRIALISÉ")
    print(f"  Démarrage : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Pipeline  : {PIPELINE_DIR}")
    print(separateur())
    print()

    if args.dry_run:
        print("📋 PLAN D'EXÉCUTION (DRY-RUN) :")
        for e in selection:
            print(f"  [{e['id']:>3}] {e['nom']}")
        sys.exit(0)

    debut_global = time.time()
    resultats = []

    for etape in selection:
        chemin = os.path.join(PIPELINE_DIR, etape["script"])
        print(separateur("-"))
        print(f"▶ [{etape['id']:>3}] {etape['nom']}")
        print(separateur("-"))
        
        debut = time.time()
        try:
            # Exécution du script avec le même interpréteur Python
            res = subprocess.run([sys.executable, etape["script"]], cwd=PIPELINE_DIR)
            duree = time.time() - debut
            
            if res.returncode == 0:
                print(f"\n✅ Succès : {etape['nom']} ({fmt_duree(duree)})")
                resultats.append({"id": etape["id"], "nom": etape["nom"], "statut": "OK", "duree": duree})
            else:
                print(f"\n❌ ÉCHEC : {etape['nom']} (code {res.returncode})")
                resultats.append({"id": etape["id"], "nom": etape["nom"], "statut": "ERREUR", "duree": duree})
                if not etape["optionnelle"]:
                    print("\n🛑 Arrêt du pipeline (étape obligatoire).")
                    break
        except Exception as e:
            print(f"\n💥 ERREUR CRITIQUE : {e}")
            break

    print(f"\n{separateur()}")
    print("  RÉSUMÉ FINAL")
    print(separateur())
    for r in resultats:
        icone = "✅" if r["statut"] == "OK" else "❌"
        print(f"  {icone} [{r['id']:>3}] {r['nom']:<35} {fmt_duree(r['duree']):>10}")
    
    print(f"\n  Durée totale : {fmt_duree(time.time() - debut_global)}")
    print(separateur())
    print()
