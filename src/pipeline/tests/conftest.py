# ============================================================
# PANEL ADMIN — Configuration pytest partagée
# ============================================================
import pytest
import polars as pl


# ── Fixtures de DataFrames Silver minimal ────────────────────

@pytest.fixture
def df_silver_grades():
    """Silver avec grades valides, invalides et NF."""
    return pl.DataFrame({
        "matricule":           ["M001", "M002", "M003", "M004", "M005"],
        "emploi":              ["médecin", "médecin", "infirmier", "infirmier", "agent"],
        "statut_fonctionnaire": [
            "Catégorie A3",
            "Catégorie B7",       # invalide
            None,                 # NF
            "Catégorie B2",
            "Catégorie C1",
        ],
        "mois_annee":          ["012024", "012024", "012024", "022024", "022024"],
        "montant_brut":        [500_000.0, 300_000.0, None, 250_000.0, 200_000.0],
        "montant_net":         [420_000.0, None,      None, 210_000.0, 170_000.0],
        "CODE_EMPLOI":         ["CE001", "CE001", "CE002", "CE002", "CE003"],
        "CODE_ORGANISME":      ["ORG1",  "ORG1",  "ORG1",  "ORG1",  "ORG2"],
        "cle_unique":          ["M001|ORG1", "M002|ORG1", "M003|ORG1",
                                "M004|ORG1", "M005|ORG2"],
    })


@pytest.fixture
def df_locf():
    """Panel individuel avec trous mensuels pour tester le LOCF."""
    return pl.DataFrame({
        "cle_unique":     ["A|ORG1", "A|ORG1", "A|ORG1", "B|ORG1", "B|ORG1"],
        "mois_annee":     ["012023", "022023", "032023", "012023", "022023"],
        "montant_brut":   [100_000.0, None, None, 200_000.0, 210_000.0],
        "montant_net":    [85_000.0,  None, None, 170_000.0, None],
    })
