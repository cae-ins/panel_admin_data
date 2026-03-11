# ============================================================
# Tests — Fonctions de normalisation (03_bronze_to_silver)
# ============================================================
import re
import unicodedata
import pytest


# ── Copies des fonctions pures (sans imports S3) ─────────────

def normaliser_pour_matching(texte) -> str | None:
    if texte is None:
        return None
    texte = str(texte).strip()
    if not texte:
        return None
    texte = re.sub(r"[-]", " ", texte)
    texte = re.sub(r"[.,;:()\[\]/]", " ", texte)
    texte = re.sub("[''\u2019]", " ", texte)
    texte = texte.upper()
    texte = unicodedata.normalize("NFKD", texte)
    texte = "".join(c for c in texte if not unicodedata.combining(c))
    texte = re.sub(r"\s+", " ", texte).strip()
    return texte if texte else None


def normaliser_situation(situation) -> str:
    if not situation or str(situation).strip() == "":
        return "autre"
    sit = str(situation).strip().upper()
    sit = unicodedata.normalize("NFKD", sit)
    sit = "".join(c for c in sit if not unicodedata.combining(c))
    sit = re.sub(r"\s+", " ", sit).strip()
    situations_valides = {
        "en_activite":      ["EN ACTIVITE", "ACTIVITE"],
        "regul_indemnites": ["REGUL. INDEMNITES", "REGUL INDEMNITES",
                             "REGULARISATION INDEMNITES"],
        "demi_solde":       ["DEMI-SOLDE", "DEMI SOLDE", "DEMISOLDE", "1/2 SOLDE"],
    }
    for categorie, valeurs in situations_valides.items():
        if sit in valeurs:
            return categorie
    return "autre"


def normaliser_nom_colonne(nom: str) -> str:
    if not nom or isinstance(nom, float):
        return "colonne_vide"
    nom = str(nom)
    nom = re.sub(r"^[A-Z_]+[0-9]+\.", "", nom)
    nom = nom.upper()
    nom = re.sub(r"[-/_ ]", "_", nom)
    nom = re.sub(r"\.", "_", nom)
    nom = unicodedata.normalize("NFKD", nom)
    nom = "".join(c for c in nom if not unicodedata.combining(c))
    nom = re.sub(r"[^A-Z0-9_]", "", nom)
    nom = re.sub(r"_+", "_", nom)
    return nom.strip("_")


# ── Tests normaliser_pour_matching ───────────────────────────

class TestNormaliserPourMatching:

    def test_none_retourne_none(self):
        assert normaliser_pour_matching(None) is None

    def test_chaine_vide_retourne_none(self):
        assert normaliser_pour_matching("") is None
        assert normaliser_pour_matching("   ") is None

    def test_accents_supprimes(self):
        assert normaliser_pour_matching("médecin") == "MEDECIN"
        assert normaliser_pour_matching("Éducation") == "EDUCATION"

    def test_majuscules(self):
        assert normaliser_pour_matching("infirmier") == "INFIRMIER"

    def test_tirets_remplacés_par_espace(self):
        result = normaliser_pour_matching("demi-solde")
        assert "DEMI" in result and "SOLDE" in result

    def test_espaces_multiples_reduits(self):
        assert normaliser_pour_matching("ministère   de   la  santé") == "MINISTERE DE LA SANTE"

    def test_ponctuation_supprimee(self):
        result = normaliser_pour_matching("Santé, Éducation et Formation")
        assert "," not in result


# ── Tests normaliser_situation ────────────────────────────────

class TestNormaliserSituation:

    def test_en_activite(self):
        assert normaliser_situation("EN ACTIVITE") == "en_activite"
        assert normaliser_situation("Activite") == "en_activite"
        assert normaliser_situation("activité") == "en_activite"

    def test_demi_solde(self):
        assert normaliser_situation("DEMI-SOLDE") == "demi_solde"
        assert normaliser_situation("Demi Solde") == "demi_solde"
        assert normaliser_situation("1/2 SOLDE") == "demi_solde"

    def test_regul_indemnites(self):
        assert normaliser_situation("REGUL INDEMNITES") == "regul_indemnites"
        assert normaliser_situation("REGUL. INDEMNITES") == "regul_indemnites"

    def test_autre_par_defaut(self):
        assert normaliser_situation("RETRAITE") == "autre"
        assert normaliser_situation("CONGE") == "autre"
        assert normaliser_situation(None) == "autre"
        assert normaliser_situation("") == "autre"

    def test_insensible_casse(self):
        assert normaliser_situation("en activite") == "en_activite"
        assert normaliser_situation("En Activite") == "en_activite"


# ── Tests normaliser_nom_colonne ──────────────────────────────

class TestNormaliserNomColonne:

    def test_tirets_et_espaces(self):
        assert normaliser_nom_colonne("montant-brut") == "MONTANT_BRUT"
        assert normaliser_nom_colonne("montant brut") == "MONTANT_BRUT"

    def test_accents_supprimes(self):
        assert normaliser_nom_colonne("Prénom") == "PRENOM"

    def test_none_retourne_colonne_vide(self):
        assert normaliser_nom_colonne(None) == "colonne_vide"

    def test_float_nan_retourne_colonne_vide(self):
        assert normaliser_nom_colonne(float("nan")) == "colonne_vide"

    def test_underscores_multiples_reduits(self):
        assert "__" not in normaliser_nom_colonne("montant__brut")
