import os
import json
import logging
from io import BytesIO, StringIO

logger = logging.getLogger(__name__)

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except Exception:
    boto3 = None
    BotoCoreError = Exception
    ClientError = Exception


def _get_s3_client():
    if boto3 is None:
        raise RuntimeError("boto3 não está instalado")

    # Deixe o boto3 usar a cadeia de credenciais padrão se não houver variáveis explícitas
    session = boto3.session.Session()
    return session.client('s3')


def upload_file(local_path: str, bucket: str, key: str) -> bool:
    """Faz upload seguro de `local_path` para `s3://bucket/key`. Retorna True se ok."""
    if boto3 is None:
        logger.warning("boto3 não disponível — pulando upload para S3")
        return False

    try:
        client = _get_s3_client()
        client.upload_file(local_path, bucket, key)
        logger.info("Uploaded %s to s3://%s/%s", local_path, bucket, key)
        return True
    except (BotoCoreError, ClientError) as e:
        logger.exception("Falha no upload para S3: %s", e)
        return False


def get_json(bucket: str, key: str):
    if boto3 is None:
        raise RuntimeError("boto3 não está instalado")
    try:
        client = _get_s3_client()
        obj = client.get_object(Bucket=bucket, Key=key)
        content = obj['Body'].read()
        return json.loads(content.decode('utf-8'))
    except Exception as e:
        logger.warning("Falha ao ler json do S3 s3://%s/%s: %s", bucket, key, e)
        raise


def get_csv_df(bucket: str, key: str):
    if boto3 is None:
        raise RuntimeError("boto3 não está instalado")
    try:
        client = _get_s3_client()
        obj = client.get_object(Bucket=bucket, Key=key)
        content = obj['Body'].read()
        s = content.decode('utf-8-sig')
        return StringIO(s)
    except Exception as e:
        logger.warning("Falha ao ler csv do S3 s3://%s/%s: %s", bucket, key, e)
        raise
*** End Patch