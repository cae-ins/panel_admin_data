#!/usr/bin/env python3
"""Lire les exports CNPS et afficher les vrais chiffres"""

import pandas as pd
from pathlib import Path

export_file = Path("C:/Users/f.migone/Desktop/projects/actif/cnps_treatment/data/medallion/staging/cnps/exports_gold/indicateurs_cnps.xlsx")

print(f"Lecture: {export_file.name}\n")

try:
    # Lire le fichier
    xls = pd.ExcelFile(str(export_file))
    print(f"Feuilles: {xls.sheet_names}\n")

    for sheet in xls.sheet_names:
        print(f"\n{'='*80}")
        print(f"FEUILLE: {sheet}")
        print(f"{'='*80}\n")

        df = pd.read_excel(str(export_file), sheet_name=sheet)
        print(df.to_string())
        print(f"\nShape: {df.shape}")

except Exception as e:
    print(f"Erreur: {e}")
    import traceback
    traceback.print_exc()
