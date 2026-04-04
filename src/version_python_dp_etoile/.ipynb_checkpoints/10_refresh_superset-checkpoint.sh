#!/bin/bash
# ============================================================
# refresh_superset.sh  (v2 — compatible DROP CASCADE)
# Panel Admin — Rafraîchissement des vues Superset
#
# Usage : ./refresh_superset.sh
# Déclenchement : après chaque chargement de la table `panel`
#   (étape 09_charger_panel_pg.py)
#
# Ce script :
#   1. Détecte si mv_panel_base existe encore
#      → Si oui  : REFRESH MATERIALIZED VIEW
#      → Si non  : crée toutes les vues depuis superset_views.sql
#        (cas normal après un DROP CASCADE déclenché par l'étape 09)
#   2. Vérifie le nombre de lignes et la couverture temporelle
#   3. Affiche le mapping barème du dernier mois
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
# Filtre les warnings bénins de collation
_filter() {
    grep -v "collation version mismatch" \
    | grep -v "^DETAIL:" \
    | grep -v "^HINT:" \
    | grep -v "^WARNING:"
}

pg() {
    kubectl exec -it "$POD" -n "$NAMESPACE" -- \
        psql -U "$USER" -d "$DB" -c "$1" 2>&1 | _filter
}

pgf() {
    kubectl exec -it "$POD" -n "$NAMESPACE" -- \
        psql -U "$USER" -d "$DB" -f "$1" 2>&1 | _filter
}

pgt() {
    # Renvoie une valeur scalaire propre (sans header ni trailing spaces)
    kubectl exec -it "$POD" -n "$NAMESPACE" -- \
        psql -U "$USER" -d "$DB" -tAc "$1" 2>/dev/null \
        | tr -d '[:space:]'
}

echo "=============================================="
echo " Panel Admin — Refresh Superset (v2)"
echo " $(date '+%Y-%m-%d %H:%M:%S')"
echo "=============================================="

# ── Étape 1 : Pod disponible ─────────────────────────────────
echo ""
echo "[1/4] Pod PostgreSQL : $POD"
kubectl get pod "$POD" -n "$NAMESPACE" --no-headers | grep -q "Running" \
    && echo "      ✓ Pod Running" \
    || { echo "      ✗ Pod non disponible"; exit 1; }

# ── Étape 2 : Vérifier que la table panel est présente ───────
echo ""
echo "[2/4] Vérification table panel et vues..."

TABLE_EXISTS=$(pgt "SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema='public' AND table_name='panel';")

if [ "$TABLE_EXISTS" = "0" ]; then
    echo "      ✗ Table panel absente — étape 09 requise d'abord"
    exit 1
fi

TABLE_ROWS=$(pgt "SELECT COUNT(*) FROM panel;")
echo "      ✓ Table panel présente ($TABLE_ROWS lignes brutes)"

# ── Étape 3 : Première installation ou refresh ? ─────────────
echo ""
echo "[3/4] Gestion des vues matérialisées..."

MV_EXISTS=$(pgt "SELECT COUNT(*) FROM pg_matviews
                 WHERE matviewname='mv_panel_base';")

if [ "$MV_EXISTS" = "0" ]; then
    echo "      → mv_panel_base absente (DROP CASCADE précédent détecté)"
    echo "      → Création complète depuis superset_views.sql..."

    # Copier le fichier SQL dans le pod
    kubectl cp "$SQL_FILE" "$POD":/tmp/superset_views.sql -n "$NAMESPACE"

    # Exécuter — superset_views.sql contient déjà les DROP IF EXISTS en tête
    pgf /tmp/superset_views.sql

    echo "      ✓ Toutes les vues créées :"
    echo "          • v_panel_base"
    echo "          • mv_panel_base (+ 6 index)"
    echo "          • v_indicateurs_global / citp / citp_gg / grade"
    echo "          • v_indicateurs_sexe_grade / v_ecart_salarial_hf"
    echo "          • v_indicateurs_bareme / bareme_grade / bareme_sexe"
    echo "          • v_multipostes_grade / v_taux_multipostes"
    echo "          • v_indicateurs_annuels"
else
    echo "      → mv_panel_base existante, rafraîchissement..."
    pg "REFRESH MATERIALIZED VIEW mv_panel_base;"
    echo "      ✓ mv_panel_base rafraîchie"
fi

# ── Étape 4 : Vérifications post-refresh ─────────────────────
echo ""
echo "[4/4] Vérifications post-refresh..."

echo ""
echo "      ┌─ Couverture temporelle ─────────────────────────────"
pg "SELECT
    COUNT(*)                                                      AS total_lignes,
    MIN(date_mois)                                                AS periode_debut,
    MAX(date_mois)                                                AS periode_fin,
    COUNT(DISTINCT annee || '-' || LPAD(mois::text,2,'0'))        AS nb_mois
FROM mv_panel_base;"

echo ""
echo "      ┌─ Mapping barème (dernier mois disponible) ──────────"
pg "WITH dernier AS (
    SELECT annee, mois FROM mv_panel_base
    ORDER BY annee DESC, mois DESC LIMIT 1
)
SELECT
    bareme,
    COUNT(DISTINCT matricule)                             AS agents,
    ROUND(AVG(montant_brut)::numeric, 0)                  AS brut_moyen
FROM mv_panel_base
WHERE annee = (SELECT annee FROM dernier)
  AND mois  = (SELECT mois  FROM dernier)
GROUP BY bareme
ORDER BY agents DESC;"

echo ""
echo "      ┌─ Cohérence vues Superset ───────────────────────────"
pg "SELECT
    'v_indicateurs_global'  AS vue, COUNT(*) AS lignes FROM v_indicateurs_global
UNION ALL SELECT 'v_indicateurs_grade',      COUNT(*) FROM v_indicateurs_grade
UNION ALL SELECT 'v_indicateurs_bareme',     COUNT(*) FROM v_indicateurs_bareme
UNION ALL SELECT 'v_ecart_salarial_hf',      COUNT(*) FROM v_ecart_salarial_hf
UNION ALL SELECT 'v_indicateurs_annuels',    COUNT(*) FROM v_indicateurs_annuels
ORDER BY vue;"

# ── Résumé final ─────────────────────────────────────────────
echo ""
echo "=============================================="
echo " ✓ Rafraîchissement terminé"
echo "   Le dashboard Superset est à jour."
echo "   Toutes les vues lisent mv_panel_base —"
echo "   aucune reconfiguration Superset requise."
echo "=============================================="