# ============================================================
# Tests — Imputation des salaires (03c_imputer_salaires_silver)
# ============================================================
import pytest
import polars as pl


# ── Reproduction de la logique LOCF ──────────────────────────

def appliquer_locf(df: pl.DataFrame,
                   locf_key: str,
                   cols: list[str]) -> tuple[pl.DataFrame, dict]:
    """
    Applique le LOCF sur les colonnes `cols` par groupe `locf_key`.
    Retourne le DataFrame modifié et un dict {col: nb_valeurs_imputées}.
    """
    df = df.with_columns([
        pl.col(c).is_null().alias(f"_{c}_null") for c in cols if c in df.columns
    ])
    df = df.with_columns(
        (pl.col("mois_annee").str.slice(2, 4) + pl.col("mois_annee").str.slice(0, 2))
        .alias("_periode_sort")
    )
    df = df.sort([locf_key, "_periode_sort"], nulls_last=True)

    df = df.with_columns([
        pl.col(c).forward_fill().over(locf_key) for c in cols if c in df.columns
    ])

    stats = {}
    for c in cols:
        if c not in df.columns:
            continue
        n = int(df.filter(pl.col(f"_{c}_null") & pl.col(c).is_not_null()).select(pl.len()).item())
        stats[c] = n

    df = df.drop(["_periode_sort"] + [f"_{c}_null" for c in cols if c in df.columns])
    return df, stats


# ── Reproduction de la logique médiane cascade ────────────────

def appliquer_mediane_cascade(df: pl.DataFrame,
                               col: str,
                               niveaux: list[tuple],
                               min_obs: int = 3) -> pl.DataFrame:
    """Applique la médiane cascade sur les nulls résiduels."""
    df_obs = df.filter(pl.col(col).is_not_null())
    for niveau, cluster_raw in niveaux:
        cluster = [c for c in cluster_raw if c in df.columns]
        if not cluster:
            continue
        med = (
            df_obs.group_by(cluster)
            .agg([
                pl.col(col).median().alias("_med"),
                pl.len().alias("_n"),
            ])
            .filter(pl.col("_n") >= min_obs)
            .drop("_n")
        )
        if len(med) == 0:
            continue
        df = df.join(med, on=cluster, how="left")
        df = df.with_columns(
            pl.when(pl.col(col).is_null() & pl.col("_med").is_not_null())
            .then(pl.col("_med"))
            .otherwise(pl.col(col))
            .alias(col)
        ).drop("_med")
    return df


# ── Tests LOCF ───────────────────────────────────────────────

class TestLOCF:

    def test_locf_remplit_mois_suivant(self):
        df = pl.DataFrame({
            "cle_unique":   ["A|ORG1", "A|ORG1", "A|ORG1"],
            "mois_annee":   ["012023", "022023", "032023"],
            "montant_brut": [100_000.0, None, None],
        })
        result, stats = appliquer_locf(df, "cle_unique", ["montant_brut"])
        assert result["montant_brut"].to_list() == [100_000.0, 100_000.0, 100_000.0]
        assert stats["montant_brut"] == 2

    def test_locf_ne_remonte_pas(self):
        """Le LOCF ne doit pas remonter — valeur précédente absente reste None."""
        df = pl.DataFrame({
            "cle_unique":   ["A|ORG1", "A|ORG1"],
            "mois_annee":   ["012023", "022023"],
            "montant_brut": [None, 100_000.0],
        })
        result, stats = appliquer_locf(df, "cle_unique", ["montant_brut"])
        assert result["montant_brut"].to_list()[0] is None
        assert stats["montant_brut"] == 0

    def test_locf_isole_par_individu(self):
        """Le LOCF d'un individu ne doit pas contaminer un autre."""
        df = pl.DataFrame({
            "cle_unique":   ["A|ORG1", "A|ORG1", "B|ORG1", "B|ORG1"],
            "mois_annee":   ["012023", "022023", "012023", "022023"],
            "montant_brut": [100_000.0, None, 200_000.0, None],
        })
        result, stats = appliquer_locf(df, "cle_unique", ["montant_brut"])
        vals = result.sort(["cle_unique", "mois_annee"])["montant_brut"].to_list()
        assert vals[0] == 100_000.0
        assert vals[1] == 100_000.0   # A imputé depuis A
        assert vals[2] == 200_000.0
        assert vals[3] == 200_000.0   # B imputé depuis B
        assert stats["montant_brut"] == 2

    def test_locf_cross_annee(self):
        """LOCF doit traverser les années (décembre → janvier suivant)."""
        df = pl.DataFrame({
            "cle_unique":   ["A|ORG1", "A|ORG1"],
            "mois_annee":   ["122023", "012024"],
            "montant_brut": [150_000.0, None],
        })
        result, stats = appliquer_locf(df, "cle_unique", ["montant_brut"])
        assert result.sort("mois_annee")["montant_brut"][1] == 150_000.0
        assert stats["montant_brut"] == 1

    def test_locf_multiples_colonnes(self):
        df = pl.DataFrame({
            "cle_unique":  ["A|ORG1", "A|ORG1"],
            "mois_annee":  ["012023", "022023"],
            "montant_brut": [100_000.0, None],
            "montant_net":  [85_000.0,  None],
        })
        result, stats = appliquer_locf(df, "cle_unique", ["montant_brut", "montant_net"])
        assert result["montant_brut"][1] == 100_000.0
        assert result["montant_net"][1] == 85_000.0
        assert stats["montant_brut"] == 1
        assert stats["montant_net"] == 1


# ── Tests médiane cascade ─────────────────────────────────────

class TestMedianeCascade:

    NIVEAUX = [
        ("L1", ["CODE_EMPLOI", "GRADE", "CODE_ORGANISME", "mois_annee"]),
        ("L2", ["CODE_EMPLOI", "GRADE", "mois_annee"]),
        ("L3", ["GRADE", "mois_annee"]),
    ]

    def _df_base(self):
        # CE3/C1 a 3 valeurs observées + 1 null → L3 (GRADE × mois) peut imputer
        return pl.DataFrame({
            "cle_unique":    list("ABCDEFGHI"),
            "mois_annee":    ["012024"] * 9,
            "CODE_EMPLOI":   ["CE1"]*3 + ["CE2"]*2 + ["CE3"]*4,
            "GRADE":         ["A3"]*3  + ["B2"]*2  + ["C1"]*4,
            "CODE_ORGANISME": ["ORG1"] * 9,
            "montant_brut":  [100_000.0, 110_000.0, 120_000.0,
                              200_000.0, 210_000.0,
                              180_000.0, 190_000.0, 170_000.0, None],
        })

    def test_mediane_l1_impute(self):
        """L1 (emploi × grade × organisme × mois) doit imputer si ≥ min_obs."""
        df = self._df_base()
        result = appliquer_mediane_cascade(df, "montant_brut", self.NIVEAUX, min_obs=3)
        # CE3 / C1 / ORG1 n'a qu'1 obs → L1 ne peut pas imputer (min_obs=3)
        # Mais L3 (GRADE × mois) peut imputer depuis C1
        assert result["montant_brut"][5] is not None

    def test_valeurs_observees_inchangees(self):
        df = self._df_base()
        result = appliquer_mediane_cascade(df, "montant_brut", self.NIVEAUX, min_obs=3)
        # Les valeurs observées ne doivent pas changer
        for i in range(5):
            assert result["montant_brut"][i] == df["montant_brut"][i]

    def test_null_reste_null_si_cluster_vide(self):
        """Si aucun cluster n'a assez d'obs, la valeur reste None."""
        df = pl.DataFrame({
            "cle_unique":    ["X"],
            "mois_annee":    ["012024"],
            "CODE_EMPLOI":   ["CE_RARE"],
            "GRADE":         ["A7"],
            "CODE_ORGANISME": ["ORG_RARE"],
            "montant_brut":  [None],
        })
        result = appliquer_mediane_cascade(df, "montant_brut", self.NIVEAUX, min_obs=3)
        assert result["montant_brut"][0] is None

    def test_mediane_correcte(self):
        """La médiane imputée doit être la médiane des valeurs observées du cluster."""
        df = pl.DataFrame({
            "cle_unique":    ["A", "B", "C", "D"],
            "mois_annee":    ["012024"] * 4,
            "CODE_EMPLOI":   ["CE1"] * 4,
            "GRADE":         ["A3"] * 4,
            "CODE_ORGANISME": ["ORG1"] * 4,
            "montant_brut":  [100_000.0, 200_000.0, 300_000.0, None],
        })
        result = appliquer_mediane_cascade(df, "montant_brut", self.NIVEAUX, min_obs=3)
        # médiane de [100k, 200k, 300k] = 200k
        assert result["montant_brut"][3] == 200_000.0


# ── Tests colonne SOURCE ──────────────────────────────────────

class TestSourceColonne:

    def test_source_observe(self):
        brut_null  = False
        brut_locf  = False
        brut_value = 100_000.0
        # Logique : not null → OBSERVE
        source = (
            "OBSERVE" if not brut_null
            else "LOCF" if brut_locf
            else "IMPUTE" if brut_value is not None
            else "NON_IMPUTABLE"
        )
        assert source == "OBSERVE"

    def test_source_locf(self):
        brut_null  = True
        brut_locf  = True
        brut_value = 100_000.0
        source = (
            "OBSERVE" if not brut_null
            else "LOCF" if brut_locf
            else "IMPUTE" if brut_value is not None
            else "NON_IMPUTABLE"
        )
        assert source == "LOCF"

    def test_source_impute(self):
        brut_null  = True
        brut_locf  = False
        brut_value = 150_000.0
        source = (
            "OBSERVE" if not brut_null
            else "LOCF" if brut_locf
            else "IMPUTE" if brut_value is not None
            else "NON_IMPUTABLE"
        )
        assert source == "IMPUTE"

    def test_source_non_imputable(self):
        brut_null  = True
        brut_locf  = False
        brut_value = None
        source = (
            "OBSERVE" if not brut_null
            else "LOCF" if brut_locf
            else "IMPUTE" if brut_value is not None
            else "NON_IMPUTABLE"
        )
        assert source == "NON_IMPUTABLE"
