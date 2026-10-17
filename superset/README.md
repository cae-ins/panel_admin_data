# Superset - Dashboard salaires fonction publique

## Contenu

- `superset_views.sql` : vues PostgreSQL alignees sur la methodologie ANSTAT/DDI.

## Prerequis

La table PostgreSQL `${PG_SCHEMA:-public}.${PG_TABLE:-panel}` doit etre chargee par :

```bash
python src/pipeline/09_charger_panel_pg.py
```

Elle doit contenir la base Gold enrichie avec les colonnes de statut M1/M2 :
`Statut`, `Statut_Total`, `montant_brut_sans_ind_sans_alloc` et `montant_brut_total`.

## Validation PostgreSQL

La validation applique le fichier SQL dans PostgreSQL, puis execute un `SELECT COUNT(*)`
sur chaque vue attendue. Elle echoue si une colonne manque, si une vue ne compile pas,
ou si les vues principales sont vides.

```bash
python src/pipeline/10_validate_superset_views.py
```

Pour un test purement structurel sur une base vide :

```bash
ALLOW_EMPTY_SUPERSET_VIEWS=1 python src/pipeline/10_validate_superset_views.py
```

## Execution complete

```bash
python orchestrateur.py --depuis 09
```

Cette commande charge le panel dans PostgreSQL puis valide les vues Superset.

## Vues creees

| Vue | Description |
|-----|-------------|
| `v_panel_base` | Vue de base methodologique |
| `mv_panel_base` | Vue materialisee de base, indexee |
| `v_salaire_moyen_sexe` | M1 par sexe |
| `v_indicateurs_global` | Evolution mensuelle M1 |
| `v_indicateurs_grade` | M1 par grade |
| `v_indicateurs_citp` | M1 par code CITP |
| `v_indicateurs_citp_gg` | M1 par grand groupe CITP |
| `v_indicateurs_sexe_grade` | M1 sexe x grade |
| `v_ecart_salarial_hf` | Ecart salarial H/F |
| `v_indicateurs_bareme` | M2 par secteur de bareme |
| `v_indicateurs_bareme_grade` | M2 bareme x grade |
| `v_indicateurs_bareme_sexe` | M2 bareme x sexe |
| `v_multipostes_grade` | M1 multi-postes par grade |
| `v_taux_multipostes` | Taux d'agents multi-postes |
| `v_indicateurs_annuels` | Indicateurs annuels M2 |
