## 📄 Documentation du Script de Pseudonymisation CNPS -> ANSTAT

Ce document décrit le script Python utilisé par la CNPS pour générer des identifiants uniques et robustes (**pseudonymes**) à partir des Numéros CNPS, en vue d'une transmission sécurisée et anonymisée vers l'ANSTAT.

### Objectif

L'objectif principal est de fournir un identifiant non réversible qui permet le **suivi longitudinal** des individus par l'ANSTAT sans jamais exposer le Numéro CNPS (donnée personnelle).

---

## ⚙️ Détails Techniques

### 1. Modules Utilisés

| Module | Description | Rôle dans le Script |
| :--- | :--- | :--- |
| `secrets` | Fournit des fonctions de génération de nombres aléatoires pour la cryptographie. | Utilisé uniquement pour créer le **Sel Secret** initial. |
| `hashlib` | Implémente des algorithmes de hachage sécurisé (comme SHA-256). | Utilisé pour calculer le pseudonyme à partir du Numéro CNPS et du Sel. |

### 2. Variables Clés

| Nom de la Variable | Rôle et Importance |
| :--- | :--- |
| `SEL_SECRET_GLOBAL` | 🔑 **Le Sel Secret**. C'est une chaîne générée aléatoirement, stockée **UNIQUEMENT** par la CNPS. Il est crucial pour la sécurité : il rend le hachage imprévisible et empêche la réversion. **Il ne doit JAMAIS être partagé et ne doit JAMAIS changer.** |
| `cnps_exemple_1` | Le Numéro CNPS en clair (la donnée source à anonymiser). |
| `id_anstat_1` | Le résultat du hachage salé, c'est-à-dire l'**identifiant unique** qui sera transmis à l'ANSTAT. |

---

## 💻 Fonctions et Logique du Code

### 1. `generer_sel_unique()`

| Signature | `def generer_sel_unique()` |
| :--- | :--- |
| Rôle | Crée une chaîne hexadécimale longue (64 caractères, 32 octets) et cryptographiquement forte à utiliser comme sel. |
| **Usage** | Cette fonction ne doit être exécutée **qu'une seule fois** lors de l'initialisation du système. Le résultat doit être stocké en sécurité. |
| Implémentation | Utilise `secrets.token_hex(32)` pour garantir une haute qualité d'aléatoire. |

### 2. `generer_id_anstat(numero_cnps, sel)`

| Signature | `def generer_id_anstat(numero_cnps: str, sel: str) -> str` |
| :--- | :--- |
| Rôle | La fonction principale de pseudonymisation. Elle applique l'algorithme de **hachage salé** pour créer l'identifiant de l'ANSTAT. |
| **Logique** | 1. **Concaténation** : Assemble le `numero_cnps` et le `sel`. <br> 2. **Encodage** : Convertit la chaîne en octets (`.encode('utf-8')`). <br> 3. **Hachage** : Applique l'algorithme **SHA-256** pour produire une empreinte numérique de longueur fixe (64 caractères hexadécimaux). |
| Robustesse | Garantie que le même Numéro CNPS générera **toujours** le même identifiant pour l'ANSTAT, assurant la cohérence des données au fil du temps. |

---

## ⚠️ Instructions de Sécurité et de Production

1.  **Stockage du Sel :** Le `SEL_SECRET_GLOBAL` doit être retiré du code source après sa génération. En production, il doit être chargé depuis une source sécurisée (variables d'environnement, gestionnaire de secrets).

2.  **CNPS : Table de Correspondance :** La CNPS doit maintenir une table de correspondance interne et sécurisée pour ses propres besoins de gestion et de mise à jour :
    $$\{\text{ID}_{\text{ANSTAT}} : \text{Numéro CNPS}\}$$
    L'ANSTAT n'aura pas accès à cette table.

3.  **Algorithme :** L'utilisation de **SHA-256** est standard et considérée comme sûre.

4.  **Transmission :** Seul l'`ID_ANSTAT` est transmis avec les données statistiques. Le Numéro CNPS ne quitte jamais le système de la CNPS.

---

## 🚀 Utilitaire : traitement d'un tableur (Excel / CSV) et sortie d'une table de correspondance

Un petit utilitaire CLI est fourni dans `tools/pseudonymisation_excel.py` pour :
- lire une colonne contenant une série de Numéros CNPS dans un fichier Excel (.xls/.xlsx) ou CSV
- calculer les pseudonymes (dédupliqués)
- écrire une table de correspondance en sortie (CSV avec deux colonnes `original` et `pseudonyme`)

Exemple d'utilisation (depuis la racine du projet) :

```bash
# 1) configurez la clé secrète de façon sûre (ou utilisez --allow-generate pour test local)
export ANSTAT_SECRET_KEY="ma_cle_secrete_persistante"

# 2) exécutez le convertisseur sur un fichier Excel
python tools/pseudonymisation_excel.py donnees.xlsx --column CNPS --out mapping.csv

# 3) mapping.csv contient maintenant la table de correspondance
```

Remarques :
- Le script cherche la clé de pseudonymisation dans la variable d'environnement `ANSTAT_SECRET_KEY`.
- Pour des tests locaux, `--allow-generate` permettra de générer une clé temporaire (ne pas utiliser en production).
- Le script supporte CSV sans dépendances externes. La lecture d'Excel requiert `pandas`.