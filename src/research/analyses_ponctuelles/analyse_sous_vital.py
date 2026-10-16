#!/usr/bin/env python3
"""
Analyse des grades et postes des employés en dessous du salaire vital (152K FBu)
"""

import polars as pl
import json
from pathlib import Path
from collections import defaultdict

# Chemins
base_path = Path("consolidation_solde_2015_2025/03_data_output/base_finale")
parquet_file = base_path / "base_consolidee_2015_2025.parquet"

SALAIRE_VITAL = 152000

print("=" * 80)
print("ANALYSE: SALAIRES SOUS LE SEUIL VITAL (152K FBu)")
print("=" * 80)

# Lecture des données
print("\n📂 Lecture des données consolidées...")
try:
    df = pl.read_parquet(str(parquet_file))
    print(f"✓ {df.shape[0]:,} lignes chargées")
    print(f"✓ Colonnes: {', '.join(df.columns)}")
except Exception as e:
    print(f"❌ Erreur: {e}")
    print("\n→ Colonnes disponibles dans le fichier:")
    # Essayer de lire avec une limite pour voir la structure
    df = pl.read_parquet(str(parquet_file), n_rows=1)
    print(df.columns)
    exit(1)

# Afficher les premières lignes et les types
print("\n📋 Aperçu des données:")
print(df.head(3).to_string())
print("\nTypes de colonnes:")
print(df.schema)

# Filtrer les salaires sous le vital (si colonne SALAIRE_BRUT ou similaire existe)
salaire_col = None
for col in ["SALAIRE_BRUT", "salaire_brut", "SALAIRE", "salaire", "SALAIRE_MENSUEL", "MONTANT"]:
    if col in df.columns:
        salaire_col = col
        break

if salaire_col:
    print(f"\n💰 Filtrage par salaire (colonne: {salaire_col})...")
    df_sous_vital = df.filter(pl.col(salaire_col) < SALAIRE_VITAL)

    print(f"\n📊 RÉSULTATS:")
    print(f"   Total employés: {df.shape[0]:,}")
    print(f"   Sous salaire vital: {df_sous_vital.shape[0]:,} ({100*df_sous_vital.shape[0]/df.shape[0]:.2f}%)")

    # Analyse par grade
    if "GRADE" in df_sous_vital.columns or "grade" in df_sous_vital.columns:
        grade_col = "GRADE" if "GRADE" in df_sous_vital.columns else "grade"
        print(f"\n🏆 DISTRIBUTION PAR GRADE (sous vital):")
        grade_dist = df_sous_vital.group_by(grade_col).agg(
            pl.len().alias("count"),
            pl.col(salaire_col).mean().alias("sal_moyen"),
            pl.col(salaire_col).min().alias("sal_min"),
            pl.col(salaire_col).max().alias("sal_max"),
        ).sort("count", descending=True)

        print(grade_dist.to_string())

        # Export JSON
        grade_json = grade_dist.to_dicts()
        with open("grades_sous_vital.json", "w", encoding="utf-8") as f:
            json.dump(grade_json, f, indent=2, ensure_ascii=False)
        print("\n✓ Résultats exportés: grades_sous_vital.json")

    # Analyse par emploi/poste
    if "EMPLOI" in df_sous_vital.columns or "emploi" in df_sous_vital.columns:
        emploi_col = "EMPLOI" if "EMPLOI" in df_sous_vital.columns else "emploi"
        print(f"\n💼 TOP 20 EMPLOIS (sous vital):")
        emploi_dist = df_sous_vital.group_by(emploi_col).agg(
            pl.len().alias("count"),
            pl.col(salaire_col).mean().alias("sal_moyen"),
        ).sort("count", descending=True).head(20)

        print(emploi_dist.to_string())

        # Export JSON
        emploi_json = emploi_dist.to_dicts()
        with open("emplois_sous_vital.json", "w", encoding="utf-8") as f:
            json.dump(emploi_json, f, indent=2, ensure_ascii=False)
        print("\n✓ Résultats exportés: emplois_sous_vital.json")

    # Stats générales
    print(f"\n📈 STATISTIQUES SALARIALES (sous vital):")
    stats = df_sous_vital.select(pl.col(salaire_col).describe())
    print(stats.to_string())

else:
    print(f"❌ Aucune colonne de salaire trouvée. Colonnes disponibles: {df.columns}")

print("\n✓ Analyse terminée!\n")
