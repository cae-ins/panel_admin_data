**STATUT DEFINITIF**





\# Filtrer selon les conditions

filtered\_df = panel\_solde\_df\[

&nbsp;   (panel\_solde\_df\["Statut\_def\_mois"] == "Cas particulier") 

]



\# Chemin et nom du fichier Excel de sortie

output\_file = "Cas\_particuliers\_stat\_def\_mois.xlsx"



\# Afficher les colonnes grade et grade 2

filtered\_df\[\["MATRICULE", "DATE\_COLLECTE", "EMPLOI\_NORM", "GRADE", "GRADE 2"]].to\_excel(output\_file, index=False)



print(f"Fichier exporté : {output\_file}")









\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*

\# Filtrer selon les conditions

filtered\_df = panel\_solde\_df\[

&nbsp;   (panel\_solde\_df\["Statut\_def\_annee"] == "Cas particulier") 

]



\# Chemin et nom du fichier Excel de sortie

output\_file = "Cas\_particuliers\_stat\_def\_annee.xlsx"



\# Afficher les colonnes grade et grade 2

filtered\_df\[\["EMPLOI\_NORM", "GRADE", "GRADE 2"]].to\_excel(output\_file, index=False)



print(f"Fichier exporté : {output\_file}")



\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*



\# Étape 1 : Compter le nombre de statuts distincts par matricule

statut\_par\_matricule = (

&nbsp;   panel\_solde\_df.groupby("MATRICULE")\["Statut\_def\_mois"]

&nbsp;   .nunique()

&nbsp;   .reset\_index(name="nb\_statuts\_distincts")

)



\# Étape 2 : Filtrer les matricules ayant plusieurs statuts

problemes\_statut = statut\_par\_matricule\[statut\_par\_matricule\["nb\_statuts\_distincts"] > 1]



\# Étape 3 : Nombre de matricules concernés

nb\_matricules = problemes\_statut.shape\[0]

print(f"Nombre de matricules ayant plusieurs statuts : {nb\_matricules}")



\# Étape 4 : Extraire les lignes correspondantes et exporter

if nb\_matricules > 0:

&nbsp;   detail\_problemes = panel\_solde\_df\[

&nbsp;       panel\_solde\_df\["MATRICULE"].isin(problemes\_statut\["MATRICULE"])

&nbsp;   ]

&nbsp;   export\_df = detail\_problemes\[\["MATRICULE","DATE\_COLLECTE", "Statut\_def\_mois"]]

&nbsp;   output\_file = "matricules\_statut\_problemes.xlsx"

&nbsp;   export\_df.to\_excel(output\_file, index=False)

&nbsp;   print(f"✅ Export terminé : {output\_file}")

else:

&nbsp;   print("✅ Aucun problème détecté, tous les matricules ont un seul statut.")





\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*





\# Étape 1 : Compter le nombre de statuts distincts par matricule

statut\_par\_matricule = (

&nbsp;   panel\_solde\_df.groupby("MATRICULE")\["Statut\_def\_annee"]

&nbsp;   .nunique()

&nbsp;   .reset\_index(name="nb\_statuts\_distincts")

)



\# Étape 2 : Filtrer les matricules ayant plusieurs statuts

problemes\_statut = statut\_par\_matricule\[statut\_par\_matricule\["nb\_statuts\_distincts"] > 1]



\# Étape 3 : Nombre de matricules concernés

nb\_matricules = problemes\_statut.shape\[0]

print(f"Nombre de matricules ayant plusieurs statuts : {nb\_matricules}")



\# Étape 4 : Extraire les lignes correspondantes et exporter

if nb\_matricules > 0:

&nbsp;   detail\_problemes = panel\_solde\_df\[

&nbsp;       panel\_solde\_df\["MATRICULE"].isin(problemes\_statut\["MATRICULE"])

&nbsp;   ]

&nbsp;   export\_df = detail\_problemes\[\["MATRICULE","DATE\_COLLECTE", "Statut\_def\_annee"]]

&nbsp;   output\_file = "matricules\_statut\_problemes\_2.xlsx"

&nbsp;   export\_df.to\_excel(output\_file, index=False)

&nbsp;   print(f"✅ Export terminé : {output\_file}")

else:

&nbsp;   print("✅ Aucun problème détecté, tous les matricules ont un seul statut.")



\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*

**SEXE**



\# Regrouper par matricule et compter le nombre de sexes distincts

sexe\_par\_matricule = (

&nbsp;   panel\_solde\_df.groupby("MATRICULE")\["SEXE"]

&nbsp;   .nunique()

&nbsp;   .reset\_index(name="nb\_sexes\_distincts")

)



\# Filtrer ceux qui ont plus d’un sexe (incohérents)

problemes\_sexe = sexe\_par\_matricule\[sexe\_par\_matricule\["nb\_sexes\_distincts"] > 1]



\# Afficher les matricules problématiques

print("Matricules ayant plusieurs sexes enregistrés :")

print(problemes\_sexe)





\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*

**AGE** 



\# Étape 1 : Supprimer les doublons sur la colonne AGE

filtered\_df = panel\_solde\_df.drop\_duplicates(subset=\["AGE"])



\# Étape 2 : Définir le chemin et le nom du fichier Excel de sortie

output\_file = "Age.xlsx"



\# Étape 3 : Exporter uniquement les colonnes pertinentes

filtered\_df\[\["AGE", "AGE\_VALIDE", "AGE\_IMPUTE"]].to\_excel(output\_file, index=False)



\# Étape 4 : Confirmation

print(f"Fichier exporté : {output\_file}")





\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*



\# Étape 1 : Supprimer les doublons exacts sur (MATRICULE, PERIODE\_COLLECTE, AGE)

age\_unique = panel\_solde\_df.drop\_duplicates(subset=\["MATRICULE", "DATE\_COLLECTE", "AGE"])



\# Étape 2 : Vérifier s'il existe plusieurs âges différents pour un même matricule et une même période

age\_incoherent = (

&nbsp;   age\_unique.groupby(\["MATRICULE", "DATE\_COLLECTE"])\["AGE"]

&nbsp;   .nunique()

&nbsp;   .reset\_index(name="nb\_age\_distincts")

)



\# Étape 3 : Filtrer les cas incohérents

problemes\_age = age\_incoherent\[age\_incoherent\["nb\_age\_distincts"] > 1]



\# Étape 4 : Afficher un message selon le résultat

if problemes\_age.empty:

&nbsp;   print("✅ Aucun problème détecté — les âges sont cohérents par matricule et période.")

else:

&nbsp;   print("⚠️ Matricules ayant plusieurs âges enregistrés pour une même période :")

&nbsp;   print(problemes\_age)





\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*

**AGE PREMIERE PRISE DE SERVICE**



\# Étape 1 : Compter le nombre d'âges distincts par matricule

age\_service\_par\_matricule = (

&nbsp;   panel\_solde\_df.groupby("MATRICULE")\["AGE\_AU\_SERVICE\_IMPUTE"]

&nbsp;   .nunique()

&nbsp;   .reset\_index(name="nb\_age\_service\_distincts")

)



\# Étape 2 : Filtrer les matricules avec plusieurs âges (incohérences)

problemes\_age\_service = age\_service\_par\_matricule\[age\_service\_par\_matricule\["nb\_age\_service\_distincts"] > 1]



\# Étape 3 : Afficher le résultat

nb\_matricules = problemes\_age\_service.shape\[0]

if nb\_matricules == 0:

&nbsp;   print("✅ Tous les matricules ont un seul AGE\_AU\_SERVICE\_IMPUTE — aucune incohérence détectée.")

else:

&nbsp;   print(f"⚠️ {nb\_matricules} matricules ont plusieurs AGE\_AU\_SERVICE\_IMPUTE :")

&nbsp;   print(problemes\_age\_service)



\# Étape 4 (optionnel) : Exporter les détails si des problèmes existent

if nb\_matricules > 0:

&nbsp;   detail\_problemes = panel\_solde\_df\[

&nbsp;       panel\_solde\_df\["MATRICULE"].isin(problemes\_age\_service\["MATRICULE"])

&nbsp;   ]\[\["MATRICULE", "AGE\_AU\_SERVICE\_IMPUTE"]].drop\_duplicates()

&nbsp;   

&nbsp;   output\_file = "problemes\_age\_service.xlsx"

&nbsp;   detail\_problemes.to\_excel(output\_file, index=False)

&nbsp;   print(f"✅ Export terminé : {output\_file}")



\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*

\# Sélectionner les colonnes et supprimer les doublons

export\_df = panel\_solde\_df\[\[

&nbsp;   "MATRICULE",

&nbsp;   "DATE\_NAISSANCE\_CORRIGEE",

&nbsp;   "AGE\_AU\_SERVICE", 

&nbsp;   "AGE\_AU\_SERVICE\_VALIDE",

&nbsp;   "AGE\_AU\_SERVICE\_IMPUTE",

&nbsp;   "AGE\_IMPUTE"

]].drop\_duplicates()



\# Exporter par chunks de 1 000 000 lignes

chunk\_size = 1000000

for i, start in enumerate(range(0, len(export\_df), chunk\_size)):

&nbsp;   file\_name = f"ages\_service\_part{i+1}.xlsx"

&nbsp;   export\_df.iloc\[start:start+chunk\_size].to\_excel(file\_name, index=False)

&nbsp;   print(f"✅ Export terminé : {file\_name}")





**ANCIENNETE DANS L'ORGANISME, et DANS L'EMPLOI**



