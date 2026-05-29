# Production Readiness — Panel Admin Data Pipeline

## Synthèse

Le pipeline dispose désormais d'une base d'industrialisation solide : architecture Medallion, orchestrateur avec reprise, configuration centralisée, client S3 commun, logging structuré, Dockerfile, tests unitaires ciblés et chargement PostgreSQL via table staging. Il reste toutefois des travaux avant de considérer l'ensemble comme une plateforme de production complètement opérée.

## État Actuel

### Points déjà en place

- **Architecture de données** : couches Staging, Bronze, Silver et Gold clairement séparées.
- **Orchestration** : `orchestrateur.py` supporte `--dry-run`, `--depuis`, `--etapes`, `--resume` et `--force`.
- **Configuration** : variables d'environnement via `.env` et paramètres pipeline dans `src/pipeline/config/pipeline_settings.yaml`.
- **Sécurité S3** : credentials MinIO lus depuis l'environnement, vérification SSL active par défaut, retries boto3 configurés.
- **Observabilité** : logging commun avec formats key-value ou JSON, logs fichier optionnels.
- **Fiabilité de run** : les étapes 01, 02 et 03 échouent maintenant explicitement si des fichiers/périodes internes sont en erreur.
- **Tests** : la suite `pytest` passe localement, avec tests dédiés à la normalisation, l'imputation et la pseudonymisation.
- **Déploiement** : Dockerfile présent et aligné sur le `requirements.txt` racine.
- **Chargement PostgreSQL** : chargement dans une table staging, contrôle du nombre de lignes, puis remplacement transactionnel de la table finale.

## Risques Résiduels

### 1. Couverture de tests encore limitée

Les tests couvrent surtout les fonctions pures de normalisation, imputation et pseudonymisation. Les étapes complètes S3 -> Parquet -> Gold ne sont pas encore testées en intégration.

**Actions recommandées**
- Ajouter un jeu de fixtures Excel minimal et anonymisé.
- Tester les étapes 02, 03 et 06 sur un bucket S3 local ou mocké.
- Ajouter un test de non-régression sur les agrégats clés : nombre de périodes, effectifs, sommes de salaires.

### 2. Validation de qualité des données perfectible

Les scripts produisent des diagnostics, mais il manque encore un contrat formel de qualité avec seuils bloquants.

**Actions recommandées**
- Définir les seuils bloquants : taux de matching ANSTAT minimal, périodes attendues, nulls maximum, doublons interdits.
- Centraliser ces seuils dans `pipeline_settings.yaml`.
- Faire échouer le pipeline quand un seuil bloquant est dépassé.

### 3. PostgreSQL : remplacement atomique mais pas encore migration contrôlée

Le chargement final est plus sûr qu'un `DROP` initial, mais la table finale est encore reconstruite à chaque run. Les indexes, grants et vues dépendantes doivent être gérés explicitement si l'usage PostgreSQL devient critique.

**Actions recommandées**
- Définir un schéma SQL cible versionné.
- Recréer indexes et grants après swap, ou les gérer via migrations.
- Tester le comportement avec les vues Superset dépendantes.

### 4. CI/CD absent

Les tests passent localement, mais il n'y a pas encore de pipeline automatisé qui bloque les changements régressifs.

**Actions recommandées**
- Ajouter une CI avec `python -m compileall` et `python -m pytest`.
- Ajouter un build Docker de contrôle.
- Publier les rapports de tests et logs d'orchestration comme artefacts.

### 5. Gouvernance des secrets

Le code lit correctement les secrets depuis l'environnement, mais le stockage effectif dépend encore de l'environnement de déploiement.

**Actions recommandées**
- Utiliser des secrets Kubernetes, Vault ou un gestionnaire équivalent.
- Interdire les secrets dans les notebooks et archives exportées.
- Ajouter un scan de secrets dans la CI.

## Priorités

### Court terme

1. Ajouter un test d'intégration minimal sur un échantillon anonymisé.
2. Formaliser les seuils de qualité bloquants dans `pipeline_settings.yaml`.
3. Ajouter une CI simple : compilation, tests, build Docker.
4. Vérifier le swap PostgreSQL avec les dashboards Superset réels.

### Moyen terme

1. Versionner le schéma PostgreSQL et les indexes.
2. Ajouter des métriques de run : durée par étape, volumes lus/écrits, taux d'erreur, taux de matching.
3. Produire un rapport de qualité consolidé par run.
4. Nettoyer les archives/notebooks hors chemin de production ou les isoler clairement.

## Critères de Production

- Aucun run partiel ne peut être marqué succès.
- Les paramètres méthodologiques du run sont versionnés et archivés.
- La suite de tests et le build Docker passent en CI.
- Les seuils de qualité bloquants sont automatisés.
- Le chargement PostgreSQL est testé avec les consommateurs aval.
- Les secrets sont fournis par l'infrastructure, jamais par le code source.

*Évaluation mise à jour : 2026-05-29*
