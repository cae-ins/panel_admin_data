import os
import secrets
import hashlib
import hmac
from typing import Optional

# --- 1. GESTION DU SEL SECRET ---

def generer_sel_unique() -> str:
    """Génère un sel cryptographiquement sûr de 32 octets (64 caractères hexadécimaux).
    Cette fonction peut servir lors de l'installation initiale pour créer une clé secrète
    qui devra ensuite être stockée de manière sécurisée (vault, gestionnaire de secrets, env var chiffrée...).

    NOTE: En production, NE PAS générer la clé automatiquement à l'import — chargez-la depuis
    un gestionnaire de secrets persistant. Cette fonction est disponible pour générer
    une clé lors de l'initialisation.
    """
    return secrets.token_hex(32)

# --- IMPORTANT ---
# Do not generate or expose the secret key at import time in production.
# Instead, set the environment variable ANSTAT_SECRET_KEY (or use a secrets manager)
# with the persistent secret key used to compute HMAC pseudonyms.

def charger_cle_secrete_depuis_env(varname: str = "ANSTAT_SECRET_KEY", allow_generate: bool = False) -> str:
        """Charge la clé secrète depuis une variable d'environnement.

        - Si la variable existe, elle est renvoyée.
        - Si elle n'existe pas et allow_generate=True, la fonction génère une clé temporaire
            (utile pour tests ou usages locaux).
        - Si elle n'existe pas et allow_generate=False, lève une erreur pour forcer
            l'utilisation d'une clé persistante et sûre.
        """
        val = os.getenv(varname)
        if val:
                return val
        if allow_generate:
                return generer_sel_unique()
        raise EnvironmentError(f"La variable d'environnement '{varname}' n'est pas définie."
                                                     " Configurez la clé secrète de pseudonymisation de façon sécurisée.")

# --- 2. FONCTION DE HACHAGE SALÉ ---

def generer_id_anstat(numero_cnps: str, cle_secrete: str) -> str:
    """Crée un pseudonyme de façon sûre en utilisant HMAC-SHA256.

    Cette version utilise HMAC plutôt que la simple concaténation + SHA256.
    HMAC évite certaines erreurs de concaténation et permet d'utiliser une
    clé secrète comme véritable clé MAC.

    Arguments:
        numero_cnps (str): Le numéro CNPS à anonymiser.
        cle_secrete (str): La clé secrète persistante (format hexadécimal ou chaîne). 

    Retourne:
        str: Le pseudonyme HMAC-SHA256 en hexadécimal (64 caractères).
    """
    if not isinstance(numero_cnps, (str, bytes)):
        raise TypeError("numero_cnps doit être une chaîne ou bytes")

    # canonicalisation minimale : retirer espaces et normaliser
    numero = numero_cnps.strip()

    key_bytes = cle_secrete.encode("utf-8") if isinstance(cle_secrete, str) else cle_secrete
    msg = numero.encode("utf-8") if isinstance(numero, str) else numero

    mac = hmac.new(key_bytes, msg, digestmod=hashlib.sha256)
    return mac.hexdigest()


def generer_id_anstat_pbkdf2(numero_cnps: str, cle_secrete: str, iterations: int = 100_000) -> str:
    """Alternative résistante aux attaques par force brute : PBKDF2-HMAC-SHA256.

    Note: PBKDF2 rendra la dérivation du pseudonyme plus coûteuse (utile si le
    numéro est dans un petit espace et sujet aux attaques par dictionnaire).
    L'output sera en hexadécimal.
    """
    if not isinstance(numero_cnps, (str, bytes)):
        raise TypeError("numero_cnps doit être une chaîne ou bytes")

    numero = numero_cnps.strip()
    key_bytes = cle_secrete.encode("utf-8") if isinstance(cle_secrete, str) else cle_secrete
    derived = hashlib.pbkdf2_hmac('sha256', numero.encode('utf-8'), key_bytes, iterations)
    return derived.hex()

# --- 3. EXEMPLE D'UTILISATION ---

if __name__ == '__main__':
    # Example usage (for local tests only). In production, the secret key MUST be
    # loaded from a secure secret store and never printed.
    key = charger_cle_secrete_depuis_env('ANSTAT_SECRET_KEY', allow_generate=True)

    cnps_exemple_1 = "194011724471"
    cnps_exemple_2 = "194011724472"

    id_anstat_1 = generer_id_anstat(cnps_exemple_1, key)
    id_anstat_2 = generer_id_anstat(cnps_exemple_2, key)

    print("\n--- RÉSULTATS (Exemple) ---")
    print(f"Numéro CNPS 1 : {cnps_exemple_1}")
    print(f"ID ANSTAT 1 (Pseudonyme) : {id_anstat_1}")

    print(f"\nNuméro CNPS 2 : {cnps_exemple_2}")
    print(f"ID ANSTAT 2 (Pseudonyme) : {id_anstat_2}")
    print("\nNote: Même une petite différence dans le CNPS produit un ID complètement différent (robustesse).")