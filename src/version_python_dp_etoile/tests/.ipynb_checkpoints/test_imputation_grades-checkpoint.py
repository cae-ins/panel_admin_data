# ============================================================
# Tests — Imputation des grades (03b_imputer_grades_silver)
# ============================================================
import re
import pytest
import polars as pl


# ── Constantes ───────────────────────────────────────────────

GRADES_VALIDES = (
    [f"A{i}" for i in range(1, 8)] +
    [f"B{i}" for i in range(1, 7)] +
    [f"C{i}" for i in range(1, 6)] +
    [f"D{i}" for i in range(1, 4)]
)
GRADES_INVALIDES = ["B7", "C7", "D7", "A", "B", "C", "D"]


# ── Copie de la fonction pure ─────────────────────────────────

def extraire_grade(statut: str | None) -> str:
    if not statut:
        return "NF"
    m = re.search(r"Cat[eé]gorie\s+([ABCD][0-9]*)", str(statut), re.IGNORECASE)
    if m:
        return m.group(1).upper().strip()
    return "NF"


def appliquer_cascade_grades(df: pl.DataFrame,
                              lk_p1: pl.DataFrame | None,
                              lk_p2: pl.DataFrame | None,
                              lk_p3: pl.DataFrame | None) -> pl.DataFrame:
    """Reproduction de la logique cascade de 03b (sans S3)."""
    df = df.with_columns(
        pl.col("GRADE").is_in(GRADES_INVALIDES).alias("_grade_invalide")
    )
    if lk_p1 is not None and "emploi" in df.columns:
        df = df.with_columns([
            pl.col("mois_annee").str.slice(2, 4).cast(pl.Int32, strict=False).alias("_ANNEE"),
            pl.col("mois_annee").str.slice(0, 2).cast(pl.Int32, strict=False).alias("_MOIS_NUM"),
        ])
        if lk_p1 is not None:
            df = df.join(lk_p1, on=["emploi", "_ANNEE", "_MOIS_NUM"], how="left")
        if lk_p2 is not None:
            df = df.join(lk_p2, on=["emploi", "_ANNEE"], how="left")
        if lk_p3 is not None:
            df = df.join(lk_p3, on=["emploi"], how="left")

        df = df.with_columns(
            pl.when(pl.col("_grade_invalide"))
            .then(
                pl.when(pl.col("GRADE_P1").is_not_null()).then(pl.col("GRADE_P1"))
                .when(pl.col("GRADE_P2").is_not_null()).then(pl.col("GRADE_P2"))
                .when(pl.col("GRADE_P3").is_not_null()).then(pl.col("GRADE_P3"))
                .otherwise(pl.col("GRADE"))
            )
            .otherwise(pl.col("GRADE"))
            .alias("GRADE")
        ).drop([c for c in ["GRADE_P1", "GRADE_P2", "GRADE_P3", "_ANNEE", "_MOIS_NUM"]
                if c in df.columns])

    df = df.with_columns(
        pl.when(~pl.col("_grade_invalide") & pl.col("GRADE").is_in(GRADES_VALIDES))
        .then(pl.lit("OBSERVE"))
        .when(pl.col("_grade_invalide") & pl.col("GRADE").is_in(GRADES_VALIDES))
        .then(pl.lit("IMPUTE"))
        .otherwise(pl.lit("NF_NON_IMPUTABLE"))
        .alias("GRADE_SOURCE")
    ).drop("_grade_invalide")

    return df


# ── Tests extraire_grade ──────────────────────────────────────

class TestExtraireGrade:

    def test_categorie_avec_accent(self):
        assert extraire_grade("Catégorie A3") == "A3"
        assert extraire_grade("Catégorie B2") == "B2"
        assert extraire_grade("Catégorie C4") == "C4"

    def test_categorie_sans_accent(self):
        assert extraire_grade("Categorie A5") == "A5"

    def test_insensible_casse(self):
        assert extraire_grade("CATÉGORIE A1") == "A1"
        assert extraire_grade("catégorie d3") == "D3"

    def test_none_retourne_nf(self):
        assert extraire_grade(None) == "NF"

    def test_vide_retourne_nf(self):
        assert extraire_grade("") == "NF"

    def test_contractuel_retourne_nf(self):
        assert extraire_grade("Contractuel") == "NF"
        assert extraire_grade("Agent permanent") == "NF"

    def test_grade_sans_numero_retourne_lettre(self):
        # "Catégorie A" → invalide mais extrait "A"
        assert extraire_grade("Catégorie A") == "A"

    def test_tous_grades_valides_extraits(self):
        for g in ["A1", "A7", "B1", "B6", "C1", "C5", "D1", "D3"]:
            assert extraire_grade(f"Catégorie {g}") == g


# ── Tests cascade d'imputation ────────────────────────────────

class TestCascadeGrades:

    def _make_lookups(self):
        lk_p1 = pl.DataFrame({
            "emploi": ["médecin"],
            "_ANNEE": [2024],
            "_MOIS_NUM": [1],
            "GRADE_P1": ["A3"],
        })
        lk_p2 = pl.DataFrame({
            "emploi": ["infirmier"],
            "_ANNEE": [2024],
            "GRADE_P2": ["B2"],
        })
        lk_p3 = pl.DataFrame({
            "emploi": ["technicien"],
            "GRADE_P3": ["C1"],
        })
        return lk_p1, lk_p2, lk_p3

    def test_grade_valide_observe(self):
        df = pl.DataFrame({
            "emploi":    ["médecin"],
            "GRADE":     ["A3"],
            "mois_annee": ["012024"],
        })
        lk_p1, lk_p2, lk_p3 = self._make_lookups()
        result = appliquer_cascade_grades(df, lk_p1, lk_p2, lk_p3)
        assert result["GRADE_SOURCE"][0] == "OBSERVE"
        assert result["GRADE"][0] == "A3"

    def test_grade_invalide_impute_p1(self):
        df = pl.DataFrame({
            "emploi":    ["médecin"],
            "GRADE":     ["B7"],        # invalide
            "mois_annee": ["012024"],
        })
        lk_p1, lk_p2, lk_p3 = self._make_lookups()
        result = appliquer_cascade_grades(df, lk_p1, lk_p2, lk_p3)
        assert result["GRADE"][0] == "A3"
        assert result["GRADE_SOURCE"][0] == "IMPUTE"

    def test_grade_invalide_impute_p2(self):
        """P1 absent pour cet emploi → repli sur P2."""
        df = pl.DataFrame({
            "emploi":    ["infirmier"],
            "GRADE":     ["C7"],        # invalide
            "mois_annee": ["012024"],
        })
        lk_p1, lk_p2, lk_p3 = self._make_lookups()
        result = appliquer_cascade_grades(df, lk_p1, lk_p2, lk_p3)
        assert result["GRADE"][0] == "B2"
        assert result["GRADE_SOURCE"][0] == "IMPUTE"

    def test_grade_invalide_impute_p3(self):
        """P1 et P2 absents → repli sur P3."""
        df = pl.DataFrame({
            "emploi":    ["technicien"],
            "GRADE":     ["D7"],        # invalide
            "mois_annee": ["022024"],
        })
        lk_p1, lk_p2, lk_p3 = self._make_lookups()
        result = appliquer_cascade_grades(df, lk_p1, lk_p2, lk_p3)
        assert result["GRADE"][0] == "C1"
        assert result["GRADE_SOURCE"][0] == "IMPUTE"

    def test_nf_non_imputable(self):
        """Emploi inconnu → NF_NON_IMPUTABLE."""
        df = pl.DataFrame({
            "emploi":    ["inconnu"],
            "GRADE":     ["NF"],
            "mois_annee": ["012024"],
        })
        lk_p1, lk_p2, lk_p3 = self._make_lookups()
        result = appliquer_cascade_grades(df, lk_p1, lk_p2, lk_p3)
        assert result["GRADE_SOURCE"][0] == "NF_NON_IMPUTABLE"

    def test_grades_valides_listes(self):
        assert "A3" in GRADES_VALIDES
        assert "B7" not in GRADES_VALIDES
        assert "NF" not in GRADES_VALIDES
        assert len(GRADES_VALIDES) == 7 + 6 + 5 + 3  # A:7 + B:6 + C:5 + D:3
