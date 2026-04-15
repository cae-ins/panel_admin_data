#!/bin/bash
# ============================================================
# refresh_superset.sh
# Panel Admin — Rafraîchissement des vues Superset
#
# Usage : ./refresh_superset.sh
# Déclenchement : après chaque nouveau fichier mensuel chargé
#   dans la table `panel` (via le script 06_compiler_panel.py)
#
# Ce script :
#   1. Rafraîchit la vue matérialisée mv_panel_base
#   2. Vérifie le nombre de lignes et le mois le plus récent
#   3. Affiche un résumé du mapping barème
# ============================================================

set -euo pipefail

# ── Configuration ────────────────────────────────────────────
NAMESPACE="postgres"
POD=$(kubectl get pod -n "$NAMESPACE" -l app=postgres \
      -o jsonpath='{.items[0].metadata.name}')
DB="datalab_db"
USER="datalab"
SQL_FILE="$(dirname "$0")/superset_views.sql"

# ── Helpers ──────────────────────────────────────────────────
pg() {
    kubectl exec -it "$POD" -n "$NAMESPACE" -- \
        psql -U "$USER" -d "$DB" -c "$1" 2>&1 | grep -v "collation version mismatch" \
                                                | grep -v "DETAIL:" \
                                                | grep -v "HINT:" \
                                                | grep -v "WARNING:"
}

pgf() {
    kubectl exec -it "$POD" -n "$NAMESPACE" -- \
        psql -U "$USER" -d "$DB" -f "$1" 2>&1 | grep -v "collation version mismatch" \
                                               | grep -v "DETAIL:" \
                                               | grep -v "HINT:" \
                                               | grep -v "WARNING:"
}

echo "=============================================="
echo " Panel Admin — Refresh Superset"
echo " $(date '+%Y-%m-%d %H:%M:%S')"
echo "=============================================="

# ── Étape 1 : Vérifier que le pod est disponible ─────────────
echo ""
echo "[1/4] Pod PostgreSQL : $POD"

# ── Étape 2 : Première installation ou refresh ? ─────────────
echo ""
echo "[2/4] Vérification de mv_panel_base..."

MV_EXISTS=$(kubectl exec -it "$POD" -n "$NAMESPACE" -- \
    psql -U "$USER" -d "$DB" -tAc \
    "SELECT COUNT(*) FROM pg_matviews WHERE matviewname='mv_panel_base';" \
    2>/dev/null | tr -d '[:space:]')

if [ "$MV_EXISTS" = "0" ]; then
    echo "      → Première installation, création complète des vues..."
    # Copier et exécuter le fichier SQL complet
    kubectl cp "$SQL_FILE" "$POD":/tmp/superset_views.sql -n "$NAMESPACE"
    pgf /tmp/superset_views.sql
    echo "      ✓ Toutes les vues créées"
else
    echo "      → Vue matérialisée existante, rafraîchissement..."
    pg "REFRESH MATERIALIZED VIEW mv_panel_base;"
    echo "      ✓ mv_panel_base rafraîchie"
fi

# ── Étape 3 : Vérifications post-refresh ─────────────────────
echo ""
echo "[3/4] Vérifications..."

pg "SELECT
    COUNT(*)                          AS total_lignes,
    MIN(date_mois)                    AS periode_debut,
    MAX(date_mois)                    AS periode_fin,
    COUNT(DISTINCT annee || '-' || LPAD(mois::text,2,'0')) AS nb_mois
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

# ── Étape 4 : Résumé ─────────────────────────────────────────
echo ""
echo "[4/4] Rafraîchissement terminé ✓"
echo "      Le dashboard Superset est à jour."
echo "      (Les vues lisent mv_panel_base — pas besoin de reconfigurer Superset)"
echo "=============================================="
