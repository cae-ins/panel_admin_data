"""
Panel Admin DAG — Airflow
=========================
Déclenché 3x/semaine (lun, mer, ven) à partir du 15 du mois.
Vérifie la présence d'un fichier .xlsx NOUVEAU dans MinIO avant de lancer le pipeline.

Architecture des buckets :
    staging → bronze → silver → gold

Idempotence :
    Le dernier fichier traité est mémorisé dans la Variable Airflow
    "PANEL_ADMIN_DERNIER_FICHIER_TRAITE". Un fichier déjà traité
    ne déclenche pas de nouveau run.
"""

from datetime import datetime, timedelta
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator


# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

BUCKET_STAGING  = Variable.get("BUCKET_STAGING",  default_var="staging")
BUCKET_BRONZE   = Variable.get("BUCKET_BRONZE",   default_var="bronze")
BUCKET_SILVER   = Variable.get("BUCKET_SILVER",   default_var="silver")
BUCKET_GOLD     = Variable.get("BUCKET_GOLD",     default_var="gold")

PREFIX_MENSUELS = "panel_admin/fichiers_mensuels"
PREFIX_REFS     = "panel_admin/references"

SCRIPTS = [
    "01_pre_analyse.py",
    "02_staging_to_bronze.py",
    "03_bronze_to_silver.py",
    "03b_imputer_grades_silver.py",
    "03c_imputer_salaires_silver.py",
    "04_validation_silver.py",
    "05_silver_to_gold.py",
    "06_compiler_panel.py",
    "07_calcul_indicateur.py",
    "08_creation_fichier_excel_avec_sommaire.py",
    "09_charger_panel_pg.py",
]

CODE_DIR = "${AIRFLOW_HOME}/dags/repo/src/version_python_dp_etoile"

ENV_MINIO = {
    "MINIO_ENDPOINT":   "{{ var.value.MINIO_ENDPOINT }}",
    "MINIO_ACCESS_KEY": "{{ var.value.MINIO_ACCESS_KEY }}",
    "MINIO_SECRET_KEY": "{{ var.value.MINIO_SECRET_KEY }}",
    "BUCKET_STAGING":   BUCKET_STAGING,
    "BUCKET_BRONZE":    BUCKET_BRONZE,
    "BUCKET_SILVER":    BUCKET_SILVER,
    "BUCKET_GOLD":      BUCKET_GOLD,
    "PREFIX_MENSUELS":  PREFIX_MENSUELS,
    "PREFIX_REFS":      PREFIX_REFS,
}


# ---------------------------------------------------------------------------
# Helper — client S3/MinIO
# ---------------------------------------------------------------------------

def _get_s3_client():
    """Construit un client boto3 à partir des variables Airflow."""
    verify_ssl = Variable.get("MINIO_VERIFY_SSL", default_var="false") == "true"
    return boto3.client(
        "s3",
        endpoint_url          = Variable.get("MINIO_ENDPOINT"),
        aws_access_key_id     = Variable.get("MINIO_ACCESS_KEY"),
        aws_secret_access_key = Variable.get("MINIO_SECRET_KEY"),
        config                = Config(signature_version="s3v4"),
        region_name           = "us-east-1",
        verify                = verify_ssl,
    )


# ---------------------------------------------------------------------------
# Callables Python
# ---------------------------------------------------------------------------

def est_apres_le_15(**context):
    """Court-circuite le pipeline si on est avant le 15 du mois."""
    jour = context["logical_date"].day
    if jour >= 15:
        print(f"✓ Jour {jour} — on est après le 15, pipeline lancé")
        return True
    print(f"✗ Jour {jour} — on est avant le 15, pipeline ignoré")
    return False


def verifier_fichiers_staging(**context):
    """
    - Pagination complète (pas de limite à 1000 objets)
    - Récupère le fichier le plus récent
    - Vérifie qu'il n'a pas déjà été traité (idempotence)
    - Pousse le fichier retenu en XCom
    """
    s3 = _get_s3_client()

    # --- Pagination ---
    fichiers           = []
    continuation_token = None

    while True:
        kwargs = {
            "Bucket": BUCKET_STAGING,
            "Prefix": f"{PREFIX_MENSUELS}/",
        }
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        resp = s3.list_objects_v2(**kwargs)
        fichiers.extend([
            {"Key": o["Key"], "LastModified": o["LastModified"]}
            for o in resp.get("Contents", [])
            if o["Key"].endswith(".xlsx")
        ])

        if not resp.get("IsTruncated"):
            break

        continuation_token = resp.get("NextContinuationToken")

    if not fichiers:
        raise FileNotFoundError(
            f"Aucun fichier .xlsx trouvé dans "
            f"s3://{BUCKET_STAGING}/{PREFIX_MENSUELS}/ — pipeline arrêté."
        )

    # --- Fichier le plus récent ---
    fichiers_tries = sorted(fichiers, key=lambda x: x["LastModified"], reverse=True)
    dernier_key    = fichiers_tries[0]["Key"]

    print(f"✓ {len(fichiers)} fichier(s) trouvé(s) :")
    for f in fichiers_tries:
        print(f"  - {f['Key']} ({f['LastModified']})")

    # --- Idempotence : comparaison avec le dernier fichier traité ---
    dernier_traite = Variable.get(
        "PANEL_ADMIN_DERNIER_FICHIER_TRAITE",
        default_var=""
    )

    if dernier_key == dernier_traite:
        raise ValueError(
            f"Le fichier '{dernier_key}' a déjà été traité.\n"
            f"Aucun nouveau fichier détecté — pipeline arrêté."
        )

    print(f"\n→ Nouveau fichier retenu : {dernier_key}")
    print(f"  (précédent traité : '{dernier_traite or 'aucun'}')")

    context["ti"].xcom_push(key="latest_file", value=dernier_key)


def verifier_buckets(**context):
    """Vérifie que les 4 buckets existent, les crée si nécessaire."""
    s3 = _get_s3_client()
    for bucket in [BUCKET_STAGING, BUCKET_BRONZE, BUCKET_SILVER, BUCKET_GOLD]:
        try:
            s3.head_bucket(Bucket=bucket)
            print(f"  ✓ Bucket OK : {bucket}")
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("404", "NoSuchBucket"):
                s3.create_bucket(Bucket=bucket)
                print(f"  ✓ Bucket créé : {bucket}")
            else:
                raise


def marquer_fichier_traite(**context):
    """
    Mémorise le fichier traité dans une Variable Airflow.
    Appelée uniquement après un pipeline complet et réussi.
    """
    dernier_key = context["ti"].xcom_pull(
        task_ids="verifier_fichier",
        key="latest_file",
    )
    Variable.set("PANEL_ADMIN_DERNIER_FICHIER_TRAITE", dernier_key)
    print(f"✓ Fichier marqué comme traité : {dernier_key}")


def on_failure_callback(context):
    """Appelé automatiquement si une tâche échoue."""
    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    log_url = context["task_instance"].log_url
    print(
        f"❌ Échec dans le DAG '{dag_id}'\n"
        f"   Tâche  : {task_id}\n"
        f"   Logs   : {log_url}\n"
        # Ajouter ici : envoi email, Slack, Teams, etc.
    )


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner":               "datalab",
    "depends_on_past":     False,
    "retries":             1,
    "retry_delay":         timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="panel_admin_pipeline",
    schedule="0 7 * * 1,3,5",      # lun, mer, ven à 7h
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["panel_admin", "pipeline"],
) as dag:

    # ── 1. Court-circuit date ─────────────────────────────────────────────────
    verifier_date = ShortCircuitOperator(
        task_id="verifier_date",
        python_callable=est_apres_le_15,
    )

    # ── 2. Vérification fichiers + idempotence ────────────────────────────────
    verifier_fichier = PythonOperator(
        task_id="verifier_fichier",
        python_callable=verifier_fichiers_staging,
    )

    # ── 3. Vérification buckets ───────────────────────────────────────────────
    verifier_buckets_task = PythonOperator(
        task_id="verifier_buckets",
        python_callable=verifier_buckets,
    )

    # ── 4. Pipeline ───────────────────────────────────────────────────────────
    prev = verifier_buckets_task
    for script in SCRIPTS:
        step = BashOperator(
            task_id=f"run_{script.removesuffix('.py')}",
            bash_command=f"cd {CODE_DIR} && python {script}",
            env={
                **ENV_MINIO,
                "LATEST_FILE": "{{ ti.xcom_pull(task_ids='verifier_fichier', key='latest_file') }}",
            },
        )
        prev >> step
        prev = step

    # ── 5. Refresh Superset ───────────────────────────────────────────────────
    refresh_superset = BashOperator(
        task_id="refresh_superset",
        bash_command=f"cd {CODE_DIR} && bash 10_refresh_superset.sh",
    )

    # ── 6. Marquage fichier traité ────────────────────────────────────────────
    marquer_traite = PythonOperator(
        task_id="marquer_fichier_traite",
        python_callable=marquer_fichier_traite,
    )

    # ── Chaîne principale ─────────────────────────────────────────────────────
    verifier_date >> verifier_fichier >> verifier_buckets_task
    prev >> refresh_superset >> marquer_traite