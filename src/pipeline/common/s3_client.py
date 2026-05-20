import os
from functools import lru_cache
from pathlib import Path

import boto3
from botocore.client import Config
from dotenv import find_dotenv, load_dotenv


class PipelineConfigError(RuntimeError):
    pass


@lru_cache(maxsize=1)
def load_pipeline_env() -> None:
    env_path = find_dotenv(usecwd=True)
    if not env_path:
        env_path = next(
            (
                str(parent / ".env")
                for parent in Path(__file__).resolve().parents
                if (parent / ".env").exists()
            ),
            "",
        )
    if env_path:
        load_dotenv(env_path)


def get_required_env(name: str) -> str:
    load_pipeline_env()
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise PipelineConfigError(
            f"Variable d'environnement obligatoire manquante : {name}"
        )
    return value


def get_bool_env(name: str, default: bool = True) -> bool:
    load_pipeline_env()
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def create_s3_client():
    load_pipeline_env()
    max_attempts = int(os.getenv("S3_MAX_ATTEMPTS", "5"))
    return boto3.client(
        "s3",
        endpoint_url=get_required_env("MINIO_ENDPOINT"),
        aws_access_key_id=get_required_env("MINIO_ACCESS_KEY"),
        aws_secret_access_key=get_required_env("MINIO_SECRET_KEY"),
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": max_attempts, "mode": "adaptive"},
            connect_timeout=int(os.getenv("S3_CONNECT_TIMEOUT", "30")),
            read_timeout=int(os.getenv("S3_READ_TIMEOUT", "120")),
        ),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        verify=get_bool_env("MINIO_VERIFY_SSL", True),
    )


load_pipeline_env()
