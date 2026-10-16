# ============================================================
# PANEL ADMIN - ETAPE 10 : VALIDATION POSTGRESQL / SUPERSET
# ============================================================
# Applique superset/superset_views.sql dans PostgreSQL puis verifie que
# chaque vue attendue peut etre interrogee. Cette etape detecte les erreurs
# SQL que les tests Python ne peuvent pas couvrir.
#
# Variables d'environnement requises :
#   PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
#
# Variables optionnelles :
#   PG_SCHEMA                 schema cible, defaut public
#   PG_TABLE                  table panel cible, defaut panel
#   SUPERSET_SQL_FILE         chemin du fichier SQL, defaut superset/superset_views.sql
#   ALLOW_EMPTY_SUPERSET_VIEWS=1 pour autoriser des vues vides
# ============================================================

from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2
from psycopg2 import sql

from common.logging_config import setup_pipeline_logging
from common.s3_client import get_bool_env, load_pipeline_env


load_pipeline_env()
logger, _ = setup_pipeline_logging("10_validate_superset_views")

REPO_ROOT = Path(__file__).resolve().parents[2]

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")
PG_TABLE = os.getenv("PG_TABLE", "panel")

SQL_FILE = Path(
    os.getenv("SUPERSET_SQL_FILE", str(REPO_ROOT / "superset" / "superset_views.sql"))
)

ALLOW_EMPTY = get_bool_env("ALLOW_EMPTY_SUPERSET_VIEWS", False)

REQUIRED_COLUMNS = {
    "matricule",
    "SEXE_STD",
    "sexe",
    "GRADE",
    "Code_CITP",
    "Metier_CITP",
    "annee",
    "mois",
    "organisme",
    "emploi",
    "bareme",
    "bareme_detail",
    "NB_POSTES",
    "montant_brut",
    "montant_net",
    "montant_brut_sans_ind_sans_alloc",
    "montant_brut_total",
    "Statut",
    "Statut_Total",
    "CODE_ORGANISME",
}

VIEWS_TO_VALIDATE = [
    "v_panel_base",
    "mv_panel_base",
    "v_salaire_moyen_sexe",
    "v_indicateurs_global",
    "v_indicateurs_citp",
    "v_indicateurs_citp_gg",
    "v_indicateurs_grade",
    "v_indicateurs_sexe_grade",
    "v_ecart_salarial_hf",
    "v_indicateurs_bareme",
    "v_indicateurs_bareme_grade",
    "v_indicateurs_bareme_sexe",
    "v_multipostes_grade",
    "v_taux_multipostes",
    "v_indicateurs_annuels",
]

NON_EMPTY_VIEWS = [
    "mv_panel_base",
    "v_indicateurs_global",
    "v_indicateurs_grade",
    "v_indicateurs_bareme",
]


def fail(message: str) -> None:
    logger.error(message)
    sys.exit(1)


def quote_relation(schema: str, relation: str) -> sql.Composed:
    return sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(relation))


def main() -> int:
    missing_env = [
        name
        for name, value in {
            "PG_HOST": PG_HOST,
            "PG_DB": PG_DB,
            "PG_USER": PG_USER,
            "PG_PASSWORD": PG_PASSWORD,
        }.items()
        if not value
    ]
    if missing_env:
        fail(f"Variables PostgreSQL manquantes : {', '.join(missing_env)}")

    if not SQL_FILE.exists():
        fail(f"Fichier SQL introuvable : {SQL_FILE}")

    logger.info("=" * 70)
    logger.info("VALIDATION POSTGRESQL / SUPERSET")
    logger.info("=" * 70)
    logger.info(f"Cible : {PG_HOST}:{PG_PORT}/{PG_DB}.{PG_SCHEMA}.{PG_TABLE}")
    logger.info(f"SQL   : {SQL_FILE}")

    sql_text = SQL_FILE.read_text(encoding="utf-8")

    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=int(PG_PORT),
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
            connect_timeout=int(os.getenv("PG_CONNECT_TIMEOUT", "15")),
        )
    except Exception as exc:
        fail(f"Connexion PostgreSQL impossible : {exc}")

    conn.autocommit = False
    cur = conn.cursor()

    try:
        cur.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(PG_SCHEMA))
        )

        cur.execute("SELECT to_regclass(%s)", (f"{PG_SCHEMA}.{PG_TABLE}",))
        if cur.fetchone()[0] is None:
            raise RuntimeError(f"Table source introuvable : {PG_SCHEMA}.{PG_TABLE}")

        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            """,
            (PG_SCHEMA, PG_TABLE),
        )
        existing_columns = {row[0] for row in cur.fetchall()}
        missing_columns = sorted(REQUIRED_COLUMNS - existing_columns)
        if missing_columns:
            raise RuntimeError(
                "Colonnes requises absentes de la table PostgreSQL : "
                + ", ".join(missing_columns)
            )

        logger.info("Application du fichier SQL Superset...")
        cur.execute(sql_text)

        logger.info("Validation des vues par SELECT COUNT(*)...")
        counts: dict[str, int] = {}
        for view_name in VIEWS_TO_VALIDATE:
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {}").format(
                    quote_relation(PG_SCHEMA, view_name)
                )
            )
            counts[view_name] = int(cur.fetchone()[0])
            logger.info(f"  {view_name:<32} {counts[view_name]:>12,} lignes")

        if not ALLOW_EMPTY:
            empty_views = [name for name in NON_EMPTY_VIEWS if counts.get(name, 0) == 0]
            if empty_views:
                raise RuntimeError(
                    "Vues obligatoires vides : "
                    + ", ".join(empty_views)
                    + ". Utiliser ALLOW_EMPTY_SUPERSET_VIEWS=1 uniquement pour un test structurel."
                )

        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE statut_bareme = 'Conforme au barème') AS m1_conformes,
                COUNT(*) FILTER (WHERE statut_total_bareme = 'Conforme au barème') AS m2_conformes
            FROM mv_panel_base
            """
        )
        m1_conformes, m2_conformes = cur.fetchone()
        logger.info(f"Conformes M1 dans mv_panel_base : {m1_conformes:,}")
        logger.info(f"Conformes M2 dans mv_panel_base : {m2_conformes:,}")

        if not ALLOW_EMPTY and (m1_conformes == 0 or m2_conformes == 0):
            raise RuntimeError(
                "Aucun conforme M1 ou M2 detecte dans mv_panel_base. "
                "Verifier les libelles de statut et l'encodage des accents."
            )

        conn.commit()
        logger.info("Validation Superset reussie")
        return 0

    except Exception as exc:
        conn.rollback()
        logger.error(f"Validation Superset echouee : {exc}")
        return 1
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
