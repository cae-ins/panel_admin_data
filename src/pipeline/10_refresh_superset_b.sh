#!/bin/bash
# ============================================================
# refresh_superset.sh  (v2 — sans kubectl, connexion directe)
# Panel Admin — Rafraîchissement des vues Superset
#
# Usage : ./refresh_superset.sh
# ============================================================

set -euo pipefail

# ── Configuration ────────────────────────────────────────────
# JHub (in-cluster) :
PG_HOST="postgres-service.postgres.svc.cluster.local"
# Local (port-forward kubectl) :
# PG_HOST="localhost"
PG_PORT="5432"
PG_DB="datalab_db"
PG_USER="datalab"
export PGPASSWORD="datalab123"

SQL_FILE="$(dirname "$0")/superset_views.sql"

# ── Helpers ──────────────────────────────────────────────────
_filter() {
    grep -v "collation version mismatch" \
    | grep -v "^DETAIL:" \
    | grep -v "^HINT:" \
    | grep -v "^WARNING:" \
    || true
}
pg()  { psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -c "$1" 2>&1 | _filter; }
pgf() { psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -f "$1" 2>&1 | _filter; }
pgt() { psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -tAc "$1" 2>/dev/null | tr -d '[:space:]'; }

echo "=============================================="
echo " Panel Admin — Refresh Superset"
echo " $(date '+%Y-%m-%d %H:%M:%S')"
echo "=============================================="

# ── Étape 1 : Tester la connexion ────────────────────────────
echo ""
echo "[1/4] Connexion PostgreSQL : ${PG_HOST}:${PG_PORT}/${PG_DB}..."
if ! pg "SELECT 1;" > /dev/null; then
    echo "      ✗ Impossible de joindre PostgreSQL"
    exit 1
fi
echo "      ✓ Connecté"

# ── Étape 2 : Vérifier que la table panel est présente ───────
echo ""
echo "[2/4] Vérification table panel..."
TABLE_EXISTS=$(pgt "SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema='public' AND table_name='panel';")
if [ "$TABLE_EXISTS" = "0" ]; then
    echo "      ✗ Table panel absente — lancer d'abord 09_charger_panel_pg.py"
    exit 1
fi
TABLE_ROWS=$(pgt "SELECT COUNT(*) FROM panel;")
echo "      ✓ Table panel : ${TABLE_ROWS} lignes"

# ── Étape 3 : Création ou refresh de mv_panel_base ───────────
echo ""
echo "[3/4] Gestion des vues matérialisées..."
MV_EXISTS=$(pgt "SELECT COUNT(*) FROM pg_matviews WHERE matviewname='mv_panel_base';")

if [ "$MV_EXISTS" = "0" ]; then
    echo "      → mv_panel_base absente (DROP CASCADE précédent détecté)"
    echo "      → Création complète depuis superset_views.sql..."
    if [ ! -f "$SQL_FILE" ]; then
        echo "      ✗ Fichier introuvable : $SQL_FILE"
        exit 1
    fi
    pgf "$SQL_FILE"
    echo "      ✓ Toutes les vues créées"
else
    echo "      → Vue matérialisée existante, rafraîchissement..."
    pg "REFRESH MATERIALIZED VIEW mv_panel_base;"
    echo "      ✓ mv_panel_base rafraîchie"
fi

# ── Étape 4 : Vérifications post-refresh ─────────────────────
echo ""
echo "[4/4] Vérifications..."
pg "SELECT
    COUNT(*)                                                   AS total_lignes,
    MIN(date_mois)                                             AS periode_debut,
    MAX(date_mois)                                             AS periode_fin,
    COUNT(DISTINCT annee || '-' || LPAD(mois::text,2,'0'))     AS nb_mois
FROM mv_panel_base;"

echo ""
echo "      Mapping barème (dernier mois disponible) :"
pg "WITH dernier AS (
    SELECT annee, mois FROM mv_panel_base
    ORDER BY annee DESC, mois DESC LIMIT 1
)
SELECT bareme, COUNT(DISTINCT matricule) AS agents
FROM mv_panel_base
WHERE annee = (SELECT annee FROM dernier)
  AND mois  = (SELECT mois  FROM dernier)
GROUP BY bareme ORDER BY agents DESC;"

echo ""
echo "=============================================="
echo " ✓ Rafraîchissement terminé"
echo "   Le dashboard Superset est à jour."
echo "=============================================="