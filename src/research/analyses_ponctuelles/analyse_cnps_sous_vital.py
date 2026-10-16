#!/usr/bin/env python3
"""
Analyse des grades et postes des employés CNPS en dessous du salaire vital (152K FBu)
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
from collections import Counter

SALAIRE_VITAL = 152000

# Utiliser le dernier mois disponible
cnps_file = Path("C:/Users/f.migone/Desktop/projects/actif/cnps_treatment/data/raw/excel/11_2025.xlsx")

print("=" * 90)
print("ANALYSE CNPS: GRADES ET POSTES SOUS LE SEUIL VITAL (152K FBu)")
print("=" * 90)

if not cnps_file.exists():
    print(f"❌ Fichier non trouvé: {cnps_file}")
    # Chercher le fichier le plus récent
    folder = Path("C:/Users/f.migone/Desktop/projects/actif/cnps_treatment/data/raw/excel/")
    files = sorted(folder.glob("*.xlsx"), reverse=True)
    if files:
        cnps_file = files[0]
        print(f"📁 Utilisation du fichier le plus récent: {cnps_file.name}")
    else:
        print("❌ Aucun fichier Excel trouvé")
        exit(1)

print(f"\n📂 Lecture: {cnps_file.name}")

try:
    # Lire le fichier Excel
    xls = pd.ExcelFile(str(cnps_file))
    print(f"✓ Feuilles disponibles: {xls.sheet_names}")

    # Essayer de trouver la bonne feuille
    sheet_names = [s for s in xls.sheet_names if s.lower() not in ['summary', 'résumé']]
    if not sheet_names:
        sheet_names = xls.sheet_names

    df = pd.read_excel(str(cnps_file), sheet_name=sheet_names[0], dtype={'SALAIRE': float})

    print(f"\n📋 Données chargées: {df.shape[0]:,} lignes × {df.shape[1]} colonnes")
    print(f"\n🔍 Colonnes disponibles:")
    for i, col in enumerate(df.columns):
        print(f"   {i+1:2d}. {col}")

    print(f"\n📊 Aperçu des données:")
    print(df.head(10).to_string())

    # Trouver la colonne salaire
    salaire_col = None
    for col in ["SALAIRE", "salaire", "SALAIRE_BRUT", "MONTANT", "TRAITEMENT"]:
        if col in df.columns:
            salaire_col = col
            break

    if not salaire_col:
        print(f"\n⚠️ Colonne salaire non trouvée. Colonnes disponibles: {df.columns.tolist()}")
        salaire_col = df.columns[0]  # Utiliser la première colonne comme fallback

    print(f"\n💰 Colonne utilisée pour les salaires: {salaire_col}")

    # Convertir en numérique si nécessaire
    df[salaire_col] = pd.to_numeric(df[salaire_col], errors='coerce')

    # Filtrer les salaires sous le vital
    df_sous_vital = df[df[salaire_col] < SALAIRE_VITAL].copy()

    print(f"\n📊 RÉSULTATS:")
    print(f"   Total employés: {df.shape[0]:,}")
    print(f"   Sous salaire vital ({SALAIRE_VITAL:,}): {df_sous_vital.shape[0]:,} ({100*df_sous_vital.shape[0]/df.shape[0]:.2f}%)")

    # Analyse par grade
    grade_cols = [col for col in df.columns if 'grade' in col.lower() or 'classe' in col.lower() or 'categor' in col.lower()]

    if grade_cols:
        grade_col = grade_cols[0]
        print(f"\n🏆 DISTRIBUTION PAR GRADE (sous vital):")
        print(f"   Colonne utilisée: {grade_col}\n")

        grade_stats = df_sous_vital.groupby(grade_col, dropna=False).agg({
            salaire_col: ['count', 'mean', 'min', 'max', 'std']
        }).round(0)

        grade_stats.columns = ['Effectif', 'Sal_Moyen', 'Sal_Min', 'Sal_Max', 'Écart-Type']
        grade_stats = grade_stats.sort_values('Effectif', ascending=False)

        print(grade_stats.to_string())

        print(f"\n   Top 5 grades (par effectif sous vital):")
        for i, (grade, row) in enumerate(grade_stats.head(5).iterrows(), 1):
            print(f"   {i}. {grade}: {int(row['Effectif'])} employés, salaire moyen {row['Sal_Moyen']:,.0f} FBu")

    else:
        print("\n⚠️ Aucune colonne de grade trouvée")

    # Analyse par emploi/poste
    emploi_cols = [col for col in df.columns if 'emploi' in col.lower() or 'poste' in col.lower() or 'fonction' in col.lower() or 'libelle' in col.lower()]

    if emploi_cols:
        emploi_col = emploi_cols[0]
        print(f"\n💼 TOP 15 EMPLOIS/POSTES (sous vital):")
        print(f"   Colonne utilisée: {emploi_col}\n")

        emploi_stats = df_sous_vital.groupby(emploi_col, dropna=False).agg({
            salaire_col: ['count', 'mean', 'min', 'max']
        }).round(0)

        emploi_stats.columns = ['Effectif', 'Sal_Moyen', 'Sal_Min', 'Sal_Max']
        emploi_stats = emploi_stats.sort_values('Effectif', ascending=False).head(15)

        print(emploi_stats.to_string())

        print(f"\n   Top 5 emplois (par effectif):")
        for i, (emploi, row) in enumerate(emploi_stats.head(5).iterrows(), 1):
            print(f"   {i}. {emploi}: {int(row['Effectif'])} employés, salaire moyen {row['Sal_Moyen']:,.0f} FBu")

    else:
        print("\n⚠️ Aucune colonne d'emploi trouvée")

    # Statistiques générales
    print(f"\n📈 STATISTIQUES SALARIALES (employés sous vital):")
    print(f"   Effectif: {df_sous_vital.shape[0]:,}")
    print(f"   Salaire moyen: {df_sous_vital[salaire_col].mean():,.0f} FBu")
    print(f"   Salaire médian: {df_sous_vital[salaire_col].median():,.0f} FBu")
    print(f"   Salaire min: {df_sous_vital[salaire_col].min():,.0f} FBu")
    print(f"   Salaire max: {df_sous_vital[salaire_col].max():,.0f} FBu")
    print(f"   Écart-type: {df_sous_vital[salaire_col].std():,.0f} FBu")

    # Distribution des salaires
    print(f"\n📊 DISTRIBUTION DES SALAIRES (sous vital):")
    bins = [0, 50000, 100000, 150000, 200000]
    hist, _ = np.histogram(df_sous_vital[salaire_col].dropna(), bins=bins)
    for i in range(len(bins)-1):
        pct = 100*hist[i]/df_sous_vital.shape[0]
        print(f"   {bins[i]:,} - {bins[i+1]:,} FBu: {int(hist[i])} employés ({pct:.1f}%)")

    print(f"\n✓ Analyse terminée!")

except Exception as e:
    print(f"❌ Erreur: {e}")
    import traceback
    traceback.print_exc()
