"""Read the three Excel files in the workspace and print schema / variable dictionaries.

Outputs a human-readable markdown file and prints the same to stdout.

This script reads all sheets and produces a markdown file `extracted_variable_dictionaries.md` in the workspace.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any

import pandas as pd


WORKSPACE = Path(r"c:\Users\f.migone\Desktop\projects\panel_travailleurs")

EXCEL_FILES = [
    WORKSPACE / "DONNEES SUR LES RETRAITES (1).xlsx",
    WORKSPACE / "REQUETES_ANSTAT_MODULE_ SUR_LES_TRAVAILEURS_VERSION_FINALE.xlsx",
    WORKSPACE / "REQUETES ANSTAT_MODULE EMPLOYEURS (1).xlsx",
]


def summarize_dataframe(df: pd.DataFrame, max_samples: int = 5) -> Dict[str, Any]:
    out = {}
    for col in df.columns:
        ser = df[col]
        non_null = ser.dropna()
        dtype = str(ser.dtype)
        # Try to detect datetime columns
        is_datetime = False
        try:
            if pd.api.types.is_datetime64_any_dtype(ser) or pd.api.types.is_datetime64tz_dtype(ser):
                is_datetime = True
            else:
                # try to convert a sample
                _ = pd.to_datetime(non_null.iloc[:5], errors="coerce")
                if _.notna().sum() > 0:
                    is_datetime = True
        except Exception:
            is_datetime = False

        unique_vals = non_null.unique()
        unique_count = len(unique_vals)

        # sample values: up to max_samples of unique non-null values
        sample_values = list(map(lambda x: _format_value(x), list(unique_vals[:max_samples])))

        out[col] = {
            "dtype": dtype,
            "is_datetime_like": is_datetime,
            "non_null_count": int(non_null.shape[0]),
            "null_count": int(df.shape[0] - non_null.shape[0]),
            "unique_count": unique_count,
            "sample_values": sample_values,
        }

    return out


def _format_value(x: Any) -> Any:
    # Format values for readable output
    if pd.isna(x):
        return None
    if isinstance(x, (pd.Timestamp,)):
        return str(x)
    if isinstance(x, float):
        if x.is_integer():
            return int(x)
    return x


def process_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"error": f"file not found: {path}"}

    try:
        # read all sheets
        sheets = pd.read_excel(path, sheet_name=None)
    except Exception as e:
        return {"error": f"failed to read {path}: {e}"}

    result = {"path": str(path), "sheets": {}}

    for sheet_name, df in sheets.items():
        try:
            result["sheets"][sheet_name] = summarize_dataframe(df)
        except Exception as e:
            result["sheets"][sheet_name] = {"error": f"failed to summarize sheet: {e}"}

    return result


def pretty_markdown(results: Dict[str, Any]) -> str:
    parts = []
    for r in results:
        if "error" in r:
            parts.append(f"## {r.get('path')} — ERROR\n{r['error']}\n\n")
            continue

        parts.append(f"# File: {r['path']}\n")
        for sheet, schema in r["sheets"].items():
            parts.append(f"\n## Sheet: {sheet}\n")
            parts.append("| variable | dtype | non-null | nulls | unique | sample values |\n")
            parts.append("|---|---:|---:|---:|---:|---|\n")
            for col, meta in schema.items():
                dv = meta.get("dtype", "")
                nn = meta.get("non_null_count", "")
                nnl = meta.get("null_count", "")
                uc = meta.get("unique_count", "")
                sv = meta.get("sample_values", [])
                svs = ", ".join([str(x) for x in sv])
                parts.append(f"| `{col}` | {dv} | {nn} | {nnl} | {uc} | {svs} |\n")

        parts.append("\n---\n")

    return "\n".join(parts)


def main():
    results = []
    for p in EXCEL_FILES:
        out = process_file(p)
        results.append(out)

    md = pretty_markdown(results)

    out_path = WORKSPACE / "extracted_variable_dictionaries.md"
    out_path.write_text(md, encoding="utf-8")

    print(md)


if __name__ == "__main__":
    main()
