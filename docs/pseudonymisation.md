# Pseudonymisation des identifiants CNPS

L'utilitaire [pseudonymisation_excel.py](../src/utils/pseudonymisation_excel.py)
lit une colonne d'identifiants dans un CSV ou un classeur Excel, déduplique les
valeurs et produit une table de correspondance CSV.

Le calcul courant appelle `generer_id_anstat` dans
[pseudonymisation_cnps_anstat.py](../src/utils/pseudonymisation_cnps_anstat.py) et
utilise HMAC-SHA256. Une même clé et un même identifiant donnent le même
pseudonyme. La variante PBKDF2 du module n'est pas utilisée par ce convertisseur.

## Exécution depuis la racine du dépôt

Définir `ANSTAT_SECRET_KEY` dans l'environnement avec la clé persistante prévue
pour le projet, puis lancer :

```bash
python -m src.utils.pseudonymisation_excel donnees.xlsx --column CNPS --out mapping.csv
```

Adapter le nom de colonne à celui du fichier. L'option `--env-var` permet de lire
une autre variable d'environnement. `--allow-generate` est réservée aux essais
locaux : une clé temporaire ne permet pas de conserver les correspondances entre
exécutions.

La sortie contient les colonnes `original` et `pseudonyme`. La clé et cette table
de correspondance restent hors du dépôt ; le fichier de correspondance contient
encore les identifiants d'origine.

## Vérification

```bash
python -m pytest tests/test_pseudonymisation.py tests/test_pseudonymisation_excel.py
```
