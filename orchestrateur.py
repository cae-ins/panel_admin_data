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
#   10  Validation SQL    (Vues Superset -> PostgreSQL)
#
# Modes d'exécution :
#   python orchestrateur.py                → pipeline complet
#   python orchestrateur.py --depuis 06    → reprend à partir du panel
#   python orchestrateur.py --etapes 07 08 → étapes spécifiques
# ============================================================

import argparse
import json
import subprocess
import sys
import time
import os
from datetime import datetime, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent / "src" / "pipeline"))
from common.logging_config import setup_pipeline_logging
from common.s3_client import create_s3_client

# ============================================================
# DÉFINITION DES ÉTAPES
# ============================================================

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
    {
        "id":          "10",
        "nom":         "Validation SQL Superset",
        "script":      "10_validate_superset_views.py",
        "description": "Applique et teste les vues Superset directement dans PostgreSQL.",
        "optionnelle": False,
    },
]

IDS_ETAPES = [e["id"] for e in ETAPES]

STATE_FILE = Path("reports") / ".pipeline_state.json"

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


def verifier_scripts(repertoire: str) -> list[str]:
    manquants = []
    for e in ETAPES:
        chemin = os.path.join(repertoire, e["script"])
        if not os.path.isfile(chemin):
            manquants.append(e["script"])
    return manquants


def charger_etat() -> set[str]:
    """Retourne les IDs d'étapes complétées lors du run précédent."""
    if not STATE_FILE.exists():
        return set()
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        return set(data.get("completed", []))
    except Exception:
        return set()


def sauvegarder_etat(completed: set[str]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(
        json.dumps({"date": datetime.now().isoformat(), "completed": sorted(completed)}, indent=2),
        encoding="utf-8",
    )


def effacer_etat() -> None:
    if STATE_FILE.exists():
        STATE_FILE.unlink()


def uploader_logs_minio(
    log_file: Path | None,
    rapport: Path | None,
    debut_dt: datetime,
) -> None:
    """Upload non-bloquant du log et du rapport vers MinIO après un run."""
    bucket = os.getenv("BUCKET_STAGING", "staging")
    prefix = "panel_admin/logs/" + debut_dt.strftime("%Y-%m") + "/" + debut_dt.strftime("run_%Y%m%d_%H%M%S")
    fichiers = [(log_file, f"{prefix}.log"), (rapport, f"{prefix}.md")]
    fichiers = [(src, key) for src, key in fichiers if src and Path(src).exists()]
    if not fichiers:
        return
    try:
        s3 = create_s3_client()
        for src, key in fichiers:
            s3.upload_file(str(src), bucket, key)
            logger.info(f"Log archivé : s3://{bucket}/{key}")
    except Exception as exc:
        logger.warning(f"Upload logs MinIO ignoré (non bloquant) : {exc}")


def generer_rapport_md(
    resultats: list[dict],
    duree_totale: float,
    debut_dt: datetime,
    args,
    log_file,
    pipeline_dir: str,
) -> Path:
    """Écrit un rapport horodaté dans reports/YYYY-MM/run_YYYYMMDD_HHMMSS.md."""
    ts   = debut_dt.strftime("%Y%m%d_%H%M%S")
    mois = debut_dt.strftime("%Y-%m")

    rapport_dir = Path("reports") / mois
    rapport_dir.mkdir(parents=True, exist_ok=True)
    rapport_path = rapport_dir / f"run_{ts}.md"

    echecs = [r for r in resultats if r["statut"] == "ERREUR"]
    if not echecs:
        statut_global = "SUCCES COMPLET"
    elif len(echecs) < sum(1 for r in resultats if r["statut"] != "SKIP"):
        statut_global = f"PARTIEL ({len(echecs)} echec(s))"
    else:
        statut_global = "ECHEC"

    if getattr(args, "force", False):
        mode = "`--force`"
    elif getattr(args, "resume", False):
        mode = "`--resume`"
    elif args.etapes:
        mode = f"`--etapes {' '.join(args.etapes)}`"
    elif args.depuis:
        mode = f"`--depuis {args.depuis}`"
    else:
        mode = "complet"

    lines = [
        "# Panel Admin — Rapport de run",
        "",
        f"**Date** : {debut_dt.strftime('%Y-%m-%d %H:%M:%S')}  ",
        f"**Duree totale** : {fmt_duree(duree_totale)}  ",
        f"**Statut** : {statut_global}",
        "",
        "## Etapes executees",
        "",
        "| ID    | Etape                               | Statut  | Duree    |",
        "|-------|-------------------------------------|---------|----------|",
    ]

    for r in resultats:
        statut = r["statut"]  # OK | SKIP | ERREUR
        duree_str = "-" if statut == "SKIP" else fmt_duree(r["duree"])
        lines.append(
            f"| {r['id']:<5} | {r['nom']:<35} | {statut:<7} | {duree_str:>8} |"
        )

    lines += [
        "",
        "## Parametres",
        "",
        f"- Pipeline : `{pipeline_dir}`",
        f"- Mode : {mode}",
    ]

    if log_file:
        lines.append(f"- Log : `{log_file}`")

    if echecs:
        lines += [
            "",
            "## Echecs",
            "",
        ]
        for r in echecs:
            lines.append(f"- **[{r['id']}] {r['nom']}** — duree : {fmt_duree(r['duree'])}")

    rapport_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return rapport_path


# ============================================================
# EXÉCUTION
# ============================================================

if __name__ == "__main__":
    logger, log_file = setup_pipeline_logging("orchestrateur")

    parser = argparse.ArgumentParser(
        description="Orchestrateur Industrialisé du Pipeline Panel Admin 2015-2025",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--depuis", help="Reprendre à partir d'un ID d'étape.")
    parser.add_argument("--etapes", nargs="+", help="Lancer uniquement ces étapes.")
    parser.add_argument("--dry-run", action="store_true", help="Afficher le plan sans exécuter.")
    parser.add_argument("--resume", action="store_true", help="Sauter les étapes déjà complétées lors du run précédent.")
    parser.add_argument("--force", action="store_true", help="Effacer l'état précédent et tout re-exécuter.")
    args = parser.parse_args()

    # Sélection des étapes
    if args.etapes:
        selection = [e for e in ETAPES if e["id"] in args.etapes]
        selection.sort(key=lambda e: IDS_ETAPES.index(e["id"]))
    elif args.depuis:
        if args.depuis not in IDS_ETAPES:
            logger.error("ID d'étape inconnu", extra={"step_id": args.depuis})
            sys.exit(1)
        idx = IDS_ETAPES.index(args.depuis)
        selection = ETAPES[idx:]
    else:
        selection = ETAPES

    # Gestion de l'état idempotent
    if args.force:
        effacer_etat()
        logger.info("--force : état du run précédent effacé")

    etapes_completees = charger_etat() if args.resume else set()
    if etapes_completees:
        logger.info(
            "--resume : étapes déjà complétées",
            extra={"skipped": ",".join(sorted(etapes_completees))},
        )

    # Vérification des scripts
    manquants = verifier_scripts(PIPELINE_DIR)
    if manquants:
        logger.error(
            "Scripts manquants",
            extra={"pipeline_dir": PIPELINE_DIR, "missing_scripts": ",".join(manquants)},
        )
        sys.exit(1)

    logger.info(
        "Démarrage du pipeline",
        extra={
            "pipeline_dir": PIPELINE_DIR,
            "selected_steps": ",".join(e["id"] for e in selection),
            "dry_run": args.dry_run,
            "resume": args.resume,
            "force": args.force,
            "log_file": str(log_file) if log_file else None,
        },
    )

    if args.dry_run:
        logger.info("Dry-run — plan d'exécution :")
        for e in selection:
            logger.info(f"  [{e['id']:>3}] {e['nom']}")
        logger.info("Dry-run terminé", extra={"step_count": len(selection)})
        sys.exit(0)

    debut_dt     = datetime.now()
    debut_global = time.time()
    resultats    = []

    for etape in selection:
        # Skip si déjà complétée (mode --resume)
        if etape["id"] in etapes_completees:
            logger.info(
                "Étape ignorée (déjà complétée)",
                extra={"step_id": etape["id"], "step_name": etape["nom"]},
            )
            resultats.append({"id": etape["id"], "nom": etape["nom"], "statut": "SKIP", "duree": 0.0})
            continue

        logger.info(
            "Début étape",
            extra={"step_id": etape["id"], "step_name": etape["nom"], "script": etape["script"]},
        )
        debut = time.time()
        try:
            res   = subprocess.run([sys.executable, etape["script"]], cwd=PIPELINE_DIR)
            duree = time.time() - debut

            if res.returncode == 0:
                resultats.append({"id": etape["id"], "nom": etape["nom"], "statut": "OK", "duree": duree})
                etapes_completees.add(etape["id"])
                sauvegarder_etat(etapes_completees)
                logger.info(
                    "Succès étape",
                    extra={
                        "step_id": etape["id"],
                        "step_name": etape["nom"],
                        "duration_seconds": round(duree, 3),
                        "return_code": res.returncode,
                    },
                )
            else:
                resultats.append({"id": etape["id"], "nom": etape["nom"], "statut": "ERREUR", "duree": duree})
                logger.error(
                    "Échec étape",
                    extra={
                        "step_id": etape["id"],
                        "step_name": etape["nom"],
                        "duration_seconds": round(duree, 3),
                        "return_code": res.returncode,
                        "optional": etape["optionnelle"],
                    },
                )
                if not etape["optionnelle"]:
                    logger.error(
                        "Arrêt du pipeline après échec obligatoire",
                        extra={"step_id": etape["id"], "step_name": etape["nom"]},
                    )
                    break

        except Exception:
            logger.exception(
                "Erreur critique pendant l'étape",
                extra={"step_id": etape["id"], "step_name": etape["nom"]},
            )
            break

    duree_totale = time.time() - debut_global

    # Effacer l'état si le run est complet sans erreur (réinitialisation propre)
    if not any(r["statut"] == "ERREUR" for r in resultats):
        effacer_etat()

    logger.info("=" * 70)
    logger.info("RÉSUMÉ FINAL")
    logger.info("=" * 70)
    for r in resultats:
        duree_str = "-" if r["statut"] == "SKIP" else fmt_duree(r["duree"])
        logger.info(f"  [{r['id']:>3}] {r['nom']:<35} {r['statut']:<8} {duree_str:>10}")
    logger.info(
        "Fin du pipeline",
        extra={
            "duration_seconds": round(duree_totale, 3),
            "ok_steps":      sum(1 for r in resultats if r["statut"] == "OK"),
            "skipped_steps": sum(1 for r in resultats if r["statut"] == "SKIP"),
            "failed_steps":  sum(1 for r in resultats if r["statut"] == "ERREUR"),
        },
    )

    rapport = generer_rapport_md(resultats, duree_totale, debut_dt, args, log_file, PIPELINE_DIR)
    logger.info(f"Rapport généré : {rapport}")
    uploader_logs_minio(log_file, rapport, debut_dt)
