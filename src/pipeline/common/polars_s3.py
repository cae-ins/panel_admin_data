"""Helpers Polars/S3 partagés entre les étapes du pipeline."""
import io
import logging
import re
import unicodedata
from functools import lru_cache

import polars as pl

from common.s3_client import create_s3_client

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _s3():
    return create_s3_client()


def lire_parquet_s3(bucket: str, key: str) -> pl.DataFrame:
    buf = io.BytesIO()
    _s3().download_fileobj(bucket, key, buf)
    buf.seek(0)
    return pl.read_parquet(buf)


def ecrire_parquet_s3(df: pl.DataFrame, bucket: str, key: str) -> None:
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)
    taille = len(buf.getvalue()) / 1024**2
    _s3().put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    logger.info(f"  ✓ s3://{bucket}/{key}  ({len(df):,} lignes × {len(df.columns)} cols  {taille:.1f} MB)")


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
