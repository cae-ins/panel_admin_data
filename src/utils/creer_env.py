"""Crée le fichier .env du pipeline à partir de .env.example, de façon interactive.

À lancer une fois par machine (serveur JupyterLab, poste local) :
  python src/utils/creer_env.py

- Les secrets (mots de passe, clés) sont saisis sans écho via getpass : ils
  n'apparaissent ni à l'écran ni dans l'historique du shell.
- Aucune commande shell n'est lancée : rien n'est interprété par un shell.
- Les valeurs sont refusées si elles contiennent un retour à la ligne ou un
  caractère de contrôle (sinon on pourrait injecter d'autres variables dans .env).
- Les valeurs sont écrites entre apostrophes, et chacune est relue par
  python-dotenv avant écriture : si elle ne revient pas à l'identique (« ${ »,
  « \\ » final), elle est refusée.
- Le fichier est créé avec les droits 600, puis l'écran du terminal est effacé.
"""
import getpass
import io
import logging
import os
import re
import sys
import tempfile
from pathlib import Path

from dotenv import dotenv_values

RACINE = Path(__file__).resolve().parents[2]
MODELE = RACINE / ".env.example"
CIBLE = RACINE / ".env"

# Variables saisies sans écho
SECRETS = {"MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "PG_PASSWORD", "ANSTAT_SECRET_KEY"}
# Valeurs d'exemple à ne jamais proposer comme défaut
VALEURS_EXEMPLE = {"change-me", ""}

LIGNE_VARIABLE = re.compile(r"^([A-Z][A-Z0-9_]*)=(.*)$")
CARACTERES_INTERDITS = re.compile(r"[\x00-\x1f\x7f]")

# Les contrôles de relecture provoquent des avertissements python-dotenv attendus
logging.getLogger("dotenv.main").setLevel(logging.ERROR)


def effacer_terminal() -> None:
    # Séquences ANSI : efface l'écran, l'historique de défilement, remet le curseur en haut
    sys.stdout.write("\033[2J\033[3J\033[H")
    sys.stdout.flush()


def demander_valeur(nom: str, defaut: str) -> str:
    secret = nom in SECRETS
    defaut = "" if defaut in VALEURS_EXEMPLE else defaut
    invite = f"{nom}" + (f" [{defaut}]" if defaut and not secret else "") + " : "

    while True:
        valeur = getpass.getpass(invite) if secret else input(invite)
        valeur = valeur.strip() or defaut
        if not valeur:
            print("  Valeur obligatoire.")
            continue
        if CARACTERES_INTERDITS.search(valeur):
            print("  Caractère interdit (retour à la ligne ou caractère de contrôle).")
            continue
        if not relu_a_l_identique(valeur):
            print("  Valeur non représentable dans un .env (« ${ » ou « \\ » final) : choisissez-en une autre.")
            continue
        if secret and getpass.getpass(f"{nom} (confirmation) : ").strip() != valeur:
            print("  Les deux saisies diffèrent.")
            continue
        return valeur


def formater(valeur: str) -> str:
    return "'" + valeur.replace("\\", "\\\\").replace("'", "\\'") + "'"


def relu_a_l_identique(valeur: str) -> bool:
    # Les scripts appellent load_dotenv() avec interpolation : « ${ » serait remplacé.
    # Le parseur de python-dotenv décide seul du reste : on vérifie l'aller-retour.
    if "${" in valeur:
        return False
    # Ligne suivante témoin : un « \ » final peut faire déborder la lecture sur elle.
    lu = dotenv_values(
        stream=io.StringIO(f"X={formater(valeur)}\nY='y'\n"), interpolate=False
    )
    return lu.get("X") == valeur and lu.get("Y") == "y"


def ecrire_atomique(contenu: str) -> None:
    descripteur, temporaire = tempfile.mkstemp(dir=RACINE, prefix=".env.", suffix=".tmp")
    try:
        with os.fdopen(descripteur, "w", encoding="utf-8", newline="\n") as fichier:
            fichier.write(contenu)
        os.chmod(temporaire, 0o600)
        os.replace(temporaire, CIBLE)
    except BaseException:
        Path(temporaire).unlink(missing_ok=True)
        raise


def main() -> int:
    if not MODELE.exists():
        print(f"Modèle introuvable : {MODELE}")
        return 1

    if CIBLE.exists():
        reponse = input(f"{CIBLE} existe déjà. L'écraser ? [o/N] ").strip().lower()
        if reponse not in {"o", "oui"}:
            print("Aucune modification.")
            return 0

    print("Entrée = valeur par défaut entre crochets. Les secrets ne s'affichent pas.\n")
    lignes, attendu = [], {}
    try:
        for ligne in MODELE.read_text(encoding="utf-8").splitlines():
            correspondance = LIGNE_VARIABLE.match(ligne.strip())
            if not correspondance:
                lignes.append(ligne)  # commentaires et lignes vides conservés
                continue
            nom, defaut = correspondance.groups()
            attendu[nom] = demander_valeur(nom, defaut.strip())
            lignes.append(f"{nom}={formater(attendu[nom])}")
        contenu = "\n".join(lignes) + "\n"
        # Dernier contrôle : le fichier complet doit se relire à l'identique
        if dict(dotenv_values(stream=io.StringIO(contenu), interpolate=False)) != attendu:
            effacer_terminal()
            print("Relecture du .env incohérente : aucun fichier écrit.")
            return 1
        ecrire_atomique(contenu)
    except (KeyboardInterrupt, EOFError):
        effacer_terminal()
        print("Interrompu : aucun fichier écrit.")
        return 1

    effacer_terminal()
    print(f"Fichier créé : {CIBLE} (droits 600)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
