import io
import os
import polars as pl
import warnings
from dotenv import load_dotenv
import boto3
from botocore.client import Config

load_dotenv(".env")

s3 = boto3.client(
    "s3",
    endpoint_url          = os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id     = os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key = os.getenv("MINIO_SECRET_KEY"),
    config                = Config(signature_version="s3v4"),
    region_name           = "us-east-1",
    verify                = False,
)

# Charger base_avec_statut.parquet (TOUS les agents, conformes et hors barème)
buf = io.BytesIO()
s3.download_fileobj("gold", "panel_admin/base_avec_statut.parquet", buf)
buf.seek(0)
base = pl.read_parquet(buf)

base

hors = base.filter(
    pl.col("Statut").is_in([
        "Inférieur au minimum du barème",
        "Supérieur au maximum du barème",
    ])
)
col_sal = "montant_brut_bareme"

hors

mois_incoherents = (
    hors.with_columns(
        pl.when(
            pl.col("Statut").is_in([
                "Inférieur au minimum du barème",
                "Supérieur au maximum du barème"
            ])
        )
        .then(1)
        .otherwise(0)
        .alias("flag_hors_bareme")
    )
    .group_by("cle_unique") 
    .agg(
        pl.col("flag_hors_bareme").sum().alias("nombre_de_mois_incoherents")
    )
)
mois_incoherents

mois_incoherents.filter(
    (pl.col('nombre_de_mois_incoherents') <= 5) & 
    (pl.col('nombre_de_mois_incoherents') >0)
)

table_agents = (
    mois_incoherents.group_by('nombre_de_mois_incoherents').agg(
        pl.col('cle_unique').n_unique().alias('nb_agents')
    ).sort('nombre_de_mois_incoherents')
)

table_lignes = table_agents.with_columns(
    (pl.col("nb_agents") * pl.col("nombre_de_mois_incoherents"))
    .alias("nb_lignes_incoherentes")
)

print(table_lignes)

table_lignes.head(5).select(
    (pl.col("nb_lignes_incoherentes").sum())
)