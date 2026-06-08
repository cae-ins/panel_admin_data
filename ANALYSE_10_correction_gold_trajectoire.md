# Document d'analyse — `10_correction_gold_trajectoire.py`

**Correction Gold : Trajectoires professionnelles**

---

## 1. À quoi sert ce script ? (en une phrase)

Ce script prend le **panel complet** des agents (une ligne = un agent × un mois)
et **corrige les erreurs de saisie** sur les informations qui ne devraient
**jamais changer** pour une même personne (sexe, date de naissance, date de
prise de service), afin de produire un panel propre et fiable pour suivre les
**trajectoires professionnelles** dans le temps.

> **Idée centrale à retenir :** une information « invariante » (par exemple la
> date de naissance) doit être **identique sur toutes les lignes d'un même
> matricule**. Si ce n'est pas le cas, c'est une erreur de saisie. La méthode
> consiste à **faire voter** toutes les lignes de l'agent et à retenir la
> valeur **majoritaire**.

---

## 2. Le vocabulaire métier

| Terme | Signification |
|---|---|
| **Matricule** | Identifiant unique d'un agent. C'est la clé de regroupement. |
| **Période / `mois_annee`** | Le mois observé (ex. `012023`). Un agent apparaît sur plusieurs périodes. |
| **Panel** | Le tableau longitudinal : un agent suivi sur plusieurs mois → plusieurs lignes par agent. |
| **Colonne invariante** | Une donnée qui ne doit pas changer dans le temps : `sexe`, `date_naissance`, `prise_service`. |
| **Gold** | Le niveau « propre, prêt à l'analyse » dans l'architecture de données (bronze → silver → gold). |
| **Valeur modale** | La valeur la plus fréquente dans un groupe (le « vote majoritaire »). |

---

## 3. Vue d'ensemble du pipeline (7 étapes)

```
   ┌─────────────────────────────────────────────────────────────┐
   │  SOURCE : s3://gold/panel_admin/panel_complet.parquet        │
   └─────────────────────────────────────────────────────────────┘
                              │
   [1] Chargement ............│ lecture du panel + vérification des colonnes
   [2] Parsing des dates .....│ texte → vraies dates (multi-formats)
   [3] Diagnostic ............│ combien d'agents ont des valeurs incohérentes ?
   [4] Anomalies calendaires .│ détection des dates impossibles (âge<16, futur…)
   [5] Correction ............│ détection des ex æquo + vote majoritaire
   [6] Indicateurs ...........│ âge, ancienneté, mobilité, contrôle des résidus
   [7] Écriture ..............│ panel propre + 4 rapports, en local + S3
                              │
   ┌─────────────────────────────────────────────────────────────┐
   │  SORTIE : panel_trajectoire_clean.parquet + rapports         │
   └─────────────────────────────────────────────────────────────┘
```

Le script est **100 % autonome** (« isolé ») : il ne dépend d'aucun autre
module du pipeline, et utilise **uniquement Polars** (pas de Pandas) pour la
performance.

---

## 4. Détail des étapes

### Étape 1 — Chargement du panel complet
*(lignes 218-251)*

- Télécharge le fichier Parquet depuis S3/MinIO en mémoire (`lire_parquet_s3`).
- **Garde-fou (lignes 235-241)** : si une colonne indispensable manque
  (`sexe`, `date_naissance`, `prise_service`, `matricule`, `mois_annee`), le
  script s'arrête immédiatement avec une erreur explicite. → On ne corrige
  jamais à l'aveugle.
- Affiche les volumes : nombre d'observations, de matricules uniques, et
  l'étendue temporelle (première → dernière période).

### Étape 2 — Parsing des dates
*(lignes 254-275)*

Les dates arrivent en **texte**, dans des formats hétérogènes. Il faut les
transformer en vraies dates pour pouvoir faire des comparaisons.

- `parse_date_multi` (lignes 158-181) essaie **plusieurs formats** dans
  l'ordre (`YYYY-MM-DD`, `DD/MM/YYYY`, `DD-MM-YYYY`, etc.) grâce à
  `pl.coalesce` : on prend le **premier format qui réussit**, sinon `null`.
- `parse_mois_annee` (lignes 184-197) transforme `mois_annee` (`MMYYYY` ou
  `YYYYMM`) en **premier jour du mois** correspondant.
- Les colonnes parsées sont **temporaires**, préfixées par `_` :
  `_dn_dt` (date naissance), `_ps_dt` (prise service), `_ma_dt` (mois observé).
  → Convention importante : tout ce qui commence par `_` sera **supprimé à la
  fin** (ligne 749).
- Un **taux de parsing** est affiché par colonne (% de valeurs converties avec
  succès), pour repérer un format mal géré.

### Étape 3 — Diagnostic de cohérence intra-individu
*(lignes 278-327)*

C'est l'étape de **mesure du problème, avant toute correction**.

Pour chaque colonne invariante, on regroupe par matricule et on compte le
**nombre de valeurs distinctes** (`n_unique`). Trois cas :

| `n_valeurs_distinctes` | Interprétation |
|---|---|
| `0` | Aucune valeur (que des `null`) → rien à corriger |
| `1` | Cohérent → l'agent a toujours la même valeur ✓ |
| `> 1` | **Incohérent** → erreur de saisie à corriger ✗ |

Le script affiche aussi la **répartition fine** des cas incohérents (combien
d'agents ont 2 valeurs distinctes, 3, etc.). Ces chiffres sont stockés dans
`diagnostics[col]` pour le rapport de synthèse final.

### Étape 4 — Détection des anomalies calendaires
*(lignes 330-416)*

On vérifie la **plausibilité logique** des dates en calculant des âges :

- `_age_obs` = âge de l'agent **au moment de l'observation** (`mois_annee − date_naissance`)
- `_age_prise_service` = âge **à la prise de service** (`prise_service − date_naissance`)

Les contrôles appliqués (avec leurs bornes, définies lignes 104-111) :

| Contrôle | Règle logique |
|---|---|
| Âge négatif | né **après** la date d'observation → impossible |
| Âge < 16 ou > 80 à la période | hors plage active plausible |
| Prise de service dans le futur | `prise_service > mois_annee` → impossible |
| Âge à la prise de service < 16 ou > 65 | recrutement implausible |
| Naissance ≥ prise de service | né après avoir commencé à travailler → impossible |
| Retraite < prise de service | (si la colonne existe) → impossible |

> **Important :** cette étape **ne supprime ni ne modifie rien**. Elle
> **signale** : toutes les lignes présentant au moins une anomalie sont
> collectées dans `df_anomalies` (lignes 387-413) pour être **exportées dans un
> rapport** d'audit. La correction, elle, se fait à l'étape suivante par le
> vote.

### Étape 5 — Correction par vote majoritaire ⭐ (le cœur de la méthode)

**Sous-étape 5.0 — Détection des cas d'égalité (ex æquo), AVANT correction.**
Avant de corriger, le script mesure pour chaque colonne invariante combien de
matricules ont un **vote ambigu** : au moins deux valeurs distinctes atteignent
la **même fréquence maximale**. Ces cas sont ceux où la règle de départage
n'est pas fiable (cf. §7). La détection (fonction `detecter_egalites_vote`)
utilise **exactement les mêmes filtres de plausibilité** que le vote, afin de
compter sur le même ensemble de valeurs. Résultat : deux rapports dédiés
(`rapport_egalites_vote.csv` / `.txt`) qui quantifient le phénomène **sans rien
modifier** — pour décider ensuite d'une politique de départage explicite.

La même logique de correction est appliquée aux trois colonnes. Prenons le
**sexe** comme exemple :

```
1.  On ne garde que les lignes où le sexe est renseigné (≠ null)
2.  On compte les occurrences de chaque valeur par matricule
        group_by(["matricule", "sexe"]) → n
3.  On trie par fréquence décroissante
        sort(["matricule", "n"], descending=[False, True])
4.  On garde la 1ʳᵉ ligne de chaque matricule = la valeur la plus fréquente
        unique(subset=["matricule"], keep="first") → "sexe_corrige"
5.  On rattache cette valeur corrigée à toutes les lignes de l'agent (join)
6.  On remplace : si une valeur corrigée existe, on l'applique partout
```

**Gestion des égalités — point de vigilance :** l'en-tête du script annonce
qu'en cas d'égalité parfaite, on retient « la première valeur chronologique ».
**Mais l'implémentation actuelle ne le garantit pas** : le tri se fait
uniquement sur la fréquence (`n`), sans second critère ; quand deux valeurs ont
le même `n`, leur ordre après `group_by` n'est ni stable ni chronologique chez
Polars. Le départage est donc en réalité **arbitraire / non déterministe**.
C'est précisément pourquoi la sous-étape 5.0 a été ajoutée : **mesurer** ces cas
avant de décider d'une vraie règle (cf. §7).

**Spécificité dates de naissance (lignes 500-577) :** avant le vote, on
**exclut les dates aberrantes** (hors `[1930-01-01, 2010-01-01]`, lignes
506-512). Ainsi une date manifestement fausse **ne peut pas gagner le vote**,
même si elle est fréquente. Après correction, on met aussi à jour la **version
texte** de la colonne pour rester cohérent (lignes 572-577).

**Spécificité prise de service (lignes 583-657) :** même principe, avec un seul
filtre de borne basse (`≥ 1960-01-01`, ligne 592).

À chaque colonne, le script :
- compte les **lignes corrigées** et les **matricules affectés** ;
- construit un **détail des corrections** (ancienne valeur → nouvelle valeur,
  nombre de lignes) ajouté à `rapport_corrections` pour l'audit ;
- libère la mémoire (`del` + `gc.collect()`) car le panel peut être volumineux.

### Étape 6 — Indicateurs de trajectoire
*(lignes 663-733)*

Une fois les données fiabilisées, on calcule les **indicateurs analytiques** :

- **Par ligne** : `age_a_la_periode`, `anciennete_debut` (âge à l'embauche),
  `anciennete_a_la_periode` (mois_annee − prise_service).
- **Par agent** (table `trajectoire`, lignes 688-714) :
  - nombre de périodes observées, première/dernière période et date,
  - durée d'observation en années,
  - **mobilité** : nombre d'organismes, emplois, grades, fonctions, services
    distincts → permet de repérer qui a *changé* de poste.

**Contrôle qualité post-correction (lignes 716-724) :** on revérifie que le
nombre de valeurs distinctes par agent est bien retombé à 1. Les
`n_encore_incoh_*` mesurent les **résidus** — idéalement zéro. Un résidu non
nul signale un cas que le vote n'a pas pu trancher (ex. : toutes les valeurs
filtrées comme aberrantes).

### Étape 7 — Écriture des résultats

Six fichiers produits, **en local ET sur S3** :

| Fichier | Contenu |
|---|---|
| `panel_trajectoire_clean.parquet` | Le panel corrigé complet (colonnes `_` retirées, indicateurs ajoutés) |
| `rapport_corrections.csv` | Détail de chaque correction : colonne, matricule, ancienne→nouvelle valeur |
| `rapport_anomalies_calendaires.csv` | Toutes les lignes avec dates impossibles (audit) |
| `rapport_egalites_vote.csv` | Cas d'égalité au vote, par matricule × colonne (n_max, nb valeurs ex æquo, valeurs concernées) |
| `rapport_egalites_vote.txt` | Synthèse lisible des cas d'égalité : comptes et % par colonne, répartition par nombre de valeurs ex æquo |
| `rapport_synthese.txt` | Synthèse lisible : diagnostic, corrections, anomalies, **cas d'égalité**, résidus, mobilité |

Avant l'écriture, **toutes les colonnes temporaires `_…` sont supprimées**
(ligne 749), puis les deux indicateurs utiles sont réinjectés (lignes 753-756).

---

## 5. Les fonctions utilitaires clés

| Fonction | Rôle |
|---|---|
| `lire_parquet_s3` / `ecrire_parquet_s3` | Entrées/sorties Parquet depuis/vers MinIO en mémoire |
| `parse_date_multi` | Texte → date, en essayant plusieurs formats (best-effort) |
| `parse_mois_annee` | `MMYYYY`/`YYYYMM` → 1ᵉʳ jour du mois |
| `valeur_modale` | Valeur la plus fréquente d'un groupe (helper du vote majoritaire) |

---

## 6. Principes de conception à mettre en avant pour expliquer

1. **Diagnostiquer avant de corriger.** Les étapes 3 et 4 mesurent le problème ;
   la correction ne vient qu'à l'étape 5. On sait toujours *combien* on corrige
   et *pourquoi*.
2. **Le vote majoritaire comme arbitre objectif.** Plutôt que de choisir
   arbitrairement, on laisse la **fréquence** décider : la valeur correcte est
   presque toujours celle saisie le plus souvent.
3. **Filtrer les aberrations avant de voter.** Une date impossible ne doit pas
   pouvoir « gagner » : on l'exclut du décompte (bornes de plausibilité).
4. **Traçabilité totale.** Chaque correction est journalisée
   (`rapport_corrections.csv`), chaque anomalie est exportée. Rien n'est corrigé
   en silence → auditabilité.
5. **Vérification de bouclage.** L'étape 6 recontrôle que les incohérences ont
   bien disparu (`résidus post-correction`). On ne suppose pas que la correction
   a marché, on le **vérifie**.
6. **Robustesse opérationnelle.** Garde-fou sur les colonnes, gestion mémoire
   explicite, double sortie local + S3, formats de dates multiples tolérés.

---

## 7. Limites et points d'attention (pour anticiper les questions)

- **Hypothèse de stabilité** : la méthode suppose que ces attributs sont
  réellement invariants. Un vrai changement de sexe à l'état civil, par exemple,
  serait « corrigé » à tort vers la valeur majoritaire.
- **⚠️ Cas d'égalité (ex æquo) — départage non déterministe** : quand deux
  valeurs atteignent la même fréquence maximale (typiquement un agent observé
  sur 2 périodes avec 2 valeurs, ou 3 « M » / 3 « H »), le vote ne peut pas
  trancher. L'en-tête promet « la première valeur chronologique », **mais le
  code n'implémente pas cette règle** : le tri ne porte que sur la fréquence,
  donc le résultat est arbitraire et peut varier d'une exécution à l'autre.
  → C'est pourquoi la sous-étape **5.0** a été ajoutée : elle **dénombre** ces
  cas par colonne et les liste dans `rapport_egalites_vote.csv` / `.txt`, **sans
  les corriger**. Une fois les volumes connus, on choisira une politique
  explicite (départage par la valeur la plus ancienne — déterministe — ; par la
  plus récente ; ou laisser non corrigé et signaler pour revue manuelle).
- **Dates exclues = pas de vote possible** : si toutes les dates d'un agent sont
  hors bornes, aucune valeur corrigée n'est produite (résidu).
- **`parse_mois_annee`** distingue `MMYYYY` et `YYYYMM` par essais successifs ;
  des formats ambigus (ex. `012023` lisible comme janvier-2023 ou 0120-23)
  reposent sur l'ordre des tentatives.

---

## 8. Résumé en 30 secondes (pour une présentation orale)

> « Ce script fiabilise le panel des agents. Pour chaque personne, des
> informations qui ne devraient jamais changer — sexe, date de naissance, date
> de prise de service — peuvent varier d'un mois à l'autre à cause d'erreurs de
> saisie. On commence par **mesurer** ces incohérences et par **détecter les
> dates impossibles**. Puis on **corrige par vote majoritaire** : on retient,
> pour chaque agent, la valeur la plus fréquente, en ayant pris soin d'écarter
> au préalable les valeurs aberrantes. On **revérifie** ensuite que les
> incohérences ont disparu, et on calcule des indicateurs de trajectoire (âge,
> ancienneté, mobilité). Enfin, on exporte le panel propre **et** des rapports
> d'audit détaillant chaque correction. Tout est tracé, rien n'est corrigé en
> silence. »
