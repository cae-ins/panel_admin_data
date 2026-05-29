from pathlib import Path

import yaml

CONFIG_PATH = Path(__file__).parent.parent / "config" / "pipeline_settings.yaml"

DEFAULT_METHODOLOGY: dict = {
    "mode_salaire_brut": True,
    "periode_recente": True,
    "inclure_zeros": True,
    "seuil_wins": 0.01,
    "min_obs_wins": 100,
    "grades_valides": [
        "A3", "A4", "A5", "A6", "A7",
        "B1", "B3",
        "C1", "C2", "C3",
        "D1", "D2",
    ],
}


def load_methodology_settings() -> dict:
    if not CONFIG_PATH.exists():
        return DEFAULT_METHODOLOGY.copy()
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}
    settings = DEFAULT_METHODOLOGY.copy()
    settings.update(config.get("methodology") or {})

    grades = settings.get("grades_valides")
    if not isinstance(grades, list) or len(grades) == 0:
        raise ValueError(
            f"methodology.grades_valides doit être une liste non vide dans {CONFIG_PATH} "
            f"(valeur actuelle : {grades!r})"
        )
    return settings
