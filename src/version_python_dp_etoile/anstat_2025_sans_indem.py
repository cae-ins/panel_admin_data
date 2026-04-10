import os
import tempfile
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import polars as pl

load_dotenv(".env")

# --- CONFIGURATION ---

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://192.168.1.230:30137")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "datalab-team")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio-datalabteam123")

BUCKET_STAGING = os.getenv("BUCKET_STAGING", "staging")

CLE_ANSTAT = "/panel_admin/references/anstat_2025.txt.xlsx"

COLONNES_BRUT = [
    "Salaire de Base",
    "Résidence",
    "Logement",
    "Transport",
    "Allocations\n (2 enfants)",
]

COLONNES_GROUPAGE = ["SECTEUR", "GRADE"]


# ============================================================
# FONCTIONS
# ============================================================


def creer_client_s3():
    """Crée et renvoie un client S3 configuré pour MinIO."""
    return boto3.client(
        "s3",
        endpoint_url          = MINIO_ENDPOINT,
        aws_access_key_id     = MINIO_ACCESS_KEY,
        aws_secret_access_key = MINIO_SECRET_KEY,
        config                = Config(signature_version="s3v4"),
        region_name           = "us-east-1",
        verify                = False,
    )


def charger_anstat_depuis_s3(client_s3, bucket, cle_s3):
    """Télécharge un fichier Excel (anstat_2025.txt.xlsx) depuis S3 et le renvoie en DataFrame Polars.

    Paramètres
    ----------
    client_s3 : boto3.client
        Client S3 déjà configuré (voir creer_client_s3).
    bucket : str
        Nom du bucket S3.
    cle_s3 : str
        Chemin (clé) du fichier dans le bucket.

    Retour
    ------
    pl.DataFrame
        Le contenu du fichier Excel sous forme de DataFrame.
    """
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp_path = tmp.name
        client_s3.download_file(bucket, cle_s3, tmp_path)
        # infer_schema_length=0 → tout est lu en String d'abord,
        # ce qui évite l'erreur "Could not determine dtype for column 10"
        df = pl.read_excel(tmp_path, infer_schema_length=0)

    os.unlink(tmp_path)  # supprimer le fichier temporaire

    # Convertir les colonnes numériques (lues comme String) en Float64
    colonnes_numeriques = [
        c for c in df.columns
        if c not in ("SECTEUR", "CATEGORIE", "GRADE", "CORPS", "EMPLOI",
                      "AUTRES INDEMNITES")
    ]
    df = df.with_columns([
        pl.col(c).cast(pl.Float64, strict=False) for c in colonnes_numeriques
        if c in df.columns
    ])

    print(f"✓ ANSTAT chargé : {len(df):,} lignes × {len(df.columns)} colonnes")
    return df


def calculer_montant_brut_sans_indem(df, colonnes_composantes, colonne_apres="AUTRES INDEMNITES"):
    """Ajoute la colonne 'Montant brut sans indemnités' et la positionne.

    Paramètres
    ----------
    df : pl.DataFrame
        DataFrame ANSTAT source.
    colonnes_composantes : list[str]
        Liste des colonnes à additionner pour obtenir le montant brut.
    colonne_apres : str
        Nom de la colonne après laquelle insérer le résultat.

    Retour
    ------
    pl.DataFrame
        DataFrame avec la nouvelle colonne insérée à la bonne position.
    """
    nom_col = "Montant brut sans indemnités"

    df = df.with_columns(
        pl.sum_horizontal([pl.col(c) for c in colonnes_composantes]).alias(nom_col)
    )

    # Réorganiser : placer la nouvelle colonne juste après colonne_apres
    cols = df.columns
    cols.remove(nom_col)
    position = cols.index(colonne_apres) + 1
    cols.insert(position, nom_col)

    return df.select(cols)


def calculer_stats_par_groupes(df, colonnes_groupage=["SECTEUR", "GRADE"], colonne_valeur="Montant brut sans indemnités"):
    """Calcule min, max, médiane, moyenne et décompte.

    Paramètres
    ----------
    df : pl.DataFrame
        DataFrame contenant les données.
    colonnes_groupage : list[str]
        Colonnes sur lesquelles grouper (ex : SECTEUR et GRADE).
    colonne_valeur : str
        Colonne numérique à analyser.

    Retour
    ------
    dict[str, pl.DataFrame]
        Dictionnaire {nom_colonne: DataFrame de statistiques}.
    """

    resultats = (
        df.group_by(colonnes_groupage)
        .agg([
            pl.col(colonne_valeur).min().alias("Minimum"),
            pl.col(colonne_valeur).max().alias("Maximum"),
            pl.col(colonne_valeur).median().alias("Médiane"),
            pl.col(colonne_valeur).mean().alias("Moyenne"),
            pl.len().alias("Nombre"),
            ])
            .sort(colonnes_groupage)
        )

    return resultats


# ============================================================
# POINT D'ENTRÉE
# ============================================================


def main():
    """Orchestre le pipeline : chargement → calcul → statistiques."""

    # 1. Connexion S3
    client_s3 = creer_client_s3()

    # 2. Chargement des données
    df_anstat = charger_anstat_depuis_s3(client_s3, BUCKET_STAGING, CLE_ANSTAT)

    # 3. Calcul du montant brut sans indemnités
    df_anstat = calculer_montant_brut_sans_indem(df_anstat, COLONNES_BRUT)

    # 4. Statistiques par cluster (SECTEUR × GRADE)
    df_stats = calculer_stats_par_groupes(df_anstat, COLONNES_GROUPAGE)

    print(f"\n--- Statistiques par cluster ({' × '.join(COLONNES_GROUPAGE)}) ---")
    print(f"{len(df_stats)} clusters trouvés\n")
    print(df_stats)


if __name__ == "__main__":
    main()