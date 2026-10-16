#!/usr/bin/env python3
"""Analyse rapide des données CNPS"""

import openpyxl
from pathlib import Path
from collections import defaultdict

SALAIRE_VITAL = 152000

# Chercher le fichier Excel le plus récent
folder = Path("C:/Users/f.migone/Desktop/projects/actif/cnps_treatment/data/raw/excel/")
files = sorted(folder.glob("*.xlsx"), reverse=True)

if not files:
    print("❌ Aucun fichier Excel trouvé")
    exit(1)

cnps_file = files[0]
print(f"Fichier: {cnps_file.name}\n")

try:
    # Ouvrir le fichier
    wb = openpyxl.load_workbook(str(cnps_file), data_only=True, read_only=True)
    ws = wb.active

    # Lire l'en-tête
    header = [cell.value for cell in ws[1]]
    print(f"Colonnes: {header}\n")

    # Trouver les indices des colonnes utiles
    salaire_idx = None
    grade_idx = None
    emploi_idx = None

    for i, col in enumerate(header):
        if col and isinstance(col, str):
            col_lower = col.lower()
            if 'salaire' in col_lower or 'montant' in col_lower or 'traitement' in col_lower:
                salaire_idx = i
            if 'grade' in col_lower or 'classe' in col_lower or 'categor' in col_lower:
                grade_idx = i
            if 'emploi' in col_lower or 'poste' in col_lower or 'libelle' in col_lower:
                emploi_idx = i

    print(f"Colonne salaire (idx {salaire_idx}): {header[salaire_idx] if salaire_idx else 'Non trouvée'}")
    print(f"Colonne grade (idx {grade_idx}): {header[grade_idx] if grade_idx else 'Non trouvée'}")
    print(f"Colonne emploi (idx {emploi_idx}): {header[emploi_idx] if emploi_idx else 'Non trouvée'}\n")

    # Collecter les données
    sous_vital = []
    total = 0

    for row in ws.iter_rows(min_row=2, values_only=True):
        total += 1
        if salaire_idx is not None:
            try:
                sal = float(row[salaire_idx] or 0)
                if sal < SALAIRE_VITAL:
                    sous_vital.append({
                        'salaire': sal,
                        'grade': row[grade_idx] if grade_idx else None,
                        'emploi': row[emploi_idx] if emploi_idx else None,
                    })
            except (ValueError, TypeError):
                pass

    wb.close()

    print(f"Total employés: {total:,}")
    print(f"Sous salaire vital: {len(sous_vital):,} ({100*len(sous_vital)/total:.2f}%)\n")

    # Analyse par grade
    if grade_idx is not None:
        grade_dist = defaultdict(lambda: {'count': 0, 'salaires': []})
        for emp in sous_vital:
            grade = emp['grade'] or 'Non spécifié'
            grade_dist[grade]['count'] += 1
            grade_dist[grade]['salaires'].append(emp['salaire'])

        print("🏆 DISTRIBUTION PAR GRADE (sous vital):\n")
        for grade in sorted(grade_dist.keys(), key=lambda g: grade_dist[g]['count'], reverse=True):
            stats = grade_dist[grade]
            sal_list = stats['salaires']
            print(f"  {grade}:")
            print(f"    Effectif: {stats['count']}")
            print(f"    Salaire moyen: {sum(sal_list)/len(sal_list):,.0f} FBu")
            print(f"    Salaire min/max: {min(sal_list):,.0f} / {max(sal_list):,.0f} FBu")
            print()

    # Analyse par emploi
    if emploi_idx is not None:
        emploi_dist = defaultdict(lambda: {'count': 0, 'salaires': []})
        for emp in sous_vital:
            emploi = str(emp['emploi'] or 'Non spécifié')[:60]  # Tronquer pour l'affichage
            emploi_dist[emploi]['count'] += 1
            emploi_dist[emploi]['salaires'].append(emp['salaire'])

        print("\n💼 TOP 10 EMPLOIS (sous vital):\n")
        for i, emploi in enumerate(sorted(emploi_dist.keys(), key=lambda e: emploi_dist[e]['count'], reverse=True)[:10], 1):
            stats = emploi_dist[emploi]
            sal_list = stats['salaires']
            print(f"  {i}. {emploi}:")
            print(f"     Effectif: {stats['count']}")
            print(f"     Salaire moyen: {sum(sal_list)/len(sal_list):,.0f} FBu")
            print()

except Exception as e:
    print(f"Erreur: {e}")
    import traceback
    traceback.print_exc()
