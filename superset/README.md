# Superset — Dashboard Salaires Fonction Publique

## Contenu
- `superset_views.sql` : vues PostgreSQL alimentant le dashboard Superset

## Première installation
```bash
psql -U datalab -d datalab_db -f superset_views.sql
```

## Refresh (après chaque nouveau mois)
```bash
# Depuis srv-datalab, après exécution du pipeline complet (étapes 01→08) :

# Étape 09 — Charger le panel Gold dans PostgreSQL
python src/version_python_dp_etoile/09_charger_panel_pg.py

# Étape 10 — Rafraîchir les vues matérialisées Superset
bash src/version_python_dp_etoile/10_refresh_superset.sh
```

## Vues créées
| Vue | Description |
|-----|-------------|
| mv_panel_base | Vue matérialisée de base (indexée) |
| v_indicateurs_global | Évolution mensuelle globale |
| v_indicateurs_grade | Par grade (A3→D2) |
| v_indicateurs_citp_gg | Par grand groupe CITP |
| v_indicateurs_sexe_grade | Croisement sexe × grade |
| v_ecart_salarial_hf | Écart salarial H/F |
| v_indicateurs_bareme | Par corps (ENS, Police, Magistrat...) |
| v_indicateurs_bareme_grade | Corps × grade |
| v_indicateurs_bareme_sexe | Corps × sexe |
| v_multipostes_grade | Multi-postes par grade |
| v_taux_multipostes | Taux d'agents multi-postés |
| v_indicateurs_annuels | KPIs annuels 2015-2025 |
