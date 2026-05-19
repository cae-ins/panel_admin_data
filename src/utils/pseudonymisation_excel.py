"""Small CLI utility to pseudonymise CNPS values from a spreadsheet and
output a mapping file.

Supports CSV or Excel (.xls/.xlsx) input. Output is a CSV with two columns:
original, pseudonyme

Usage (example):
  python tools/pseudonymisation_excel.py input.xlsx --column cnps --out mapping.csv

This script uses the functions in the repository and expects the secret key to
be configured in the ANSTAT_SECRET_KEY environment variable. For local testing
you can set allow_generate=True to create a temporary key.
"""
import argparse
import csv
import os
from typing import Iterable, Tuple

try:
    import pandas as pd
except Exception:  # pragma: no cover - optional dependency
    pd = None

from pseudonymisation_cnps_anstat import (
    charger_cle_secrete_depuis_env,
    generer_id_anstat,
)


def read_values_from_file(path: str, column: str) -> Iterable[str]:
    """Read values from CSV or Excel file.

    - If pandas is installed, uses it to read Excel/CSV and extract the column.
    - Otherwise, if path ends with .csv, uses csv.reader from stdlib.
    """
    ext = os.path.splitext(path)[1].lower()
    if pd is not None:
        df = pd.read_excel(path) if ext in ('.xls', '.xlsx') else pd.read_csv(path)
        if column not in df.columns:
            raise KeyError(f"La colonne '{column}' n'existe pas dans le fichier")
        return (str(v) for v in df[column].dropna().astype(str).tolist())

    if ext == '.csv':
        with open(path, newline='', encoding='utf-8') as fh:
            reader = csv.DictReader(fh)
            if column not in reader.fieldnames:
                raise KeyError(f"La colonne '{column}' n'existe pas dans le fichier")
            return (row[column] for row in reader if row.get(column) is not None)

    raise RuntimeError("Pandas non disponible et le format demandé n'est pas CSV."
                       " Installez pandas pour lire Excel ou fournissez un CSV.")


def generate_mapping(values: Iterable[str], key: str) -> Iterable[Tuple[str, str]]:
    seen = set()
    for v in values:
        orig = v.strip()
        if not orig:
            continue
        if orig in seen:
            continue
        seen.add(orig)
        yield orig, generer_id_anstat(orig, key)


def write_mapping_csv(path: str, mapping: Iterable[Tuple[str, str]]):
    with open(path, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.writer(fh)
        writer.writerow(['original', 'pseudonyme'])
        for row in mapping:
            writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(description="Pseudonymiser une colonne CNPS d'un fichier")
    parser.add_argument('input', help='Fichier d’entrée: .csv, .xls, .xlsx')
    parser.add_argument('--column', default='cnps', help='Nom de la colonne contenant les CNPS')
    parser.add_argument('--out', default='mapping.csv', help='Fichier de sortie (CSV)')
    parser.add_argument('--env-var', default='ANSTAT_SECRET_KEY', help='Nom de la variable d’environnement secret')
    parser.add_argument('--allow-generate', action='store_true', help='Permet de générer une clé temporaire si la var env est absente (USE FOR LOCAL TESTS ONLY)')

    args = parser.parse_args()

    key = charger_cle_secrete_depuis_env(args.env_var, allow_generate=args.allow_generate)

    values = read_values_from_file(args.input, args.column)
    mapping = list(generate_mapping(values, key))
    write_mapping_csv(args.out, mapping)
    print(f"Écrit mapping dans {args.out} ({len(mapping)} entrées)")


if __name__ == '__main__':
    main()
