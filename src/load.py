import boto3
import logging
import os
from botocore.exceptions import NoCredentialsError, ClientError

# Logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def upload_to_s3(file_path: str, s3_folder: str = "processed"):
    """
    Faz o upload de um arquivo local para o Bucket S3 configurado.
    """
    # Recupera variáveis de ambiente (carregadas pelo config.py ou Docker)
    bucket_name = os.getenv("S3_BUCKET_NAME")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION")

    if not bucket_name:
        raise ValueError("Bucket Name não encontrado nas variáveis de ambiente.")

    # Nome do arquivo no S3 (mantém o nome original)
    filename = os.path.basename(file_path)
    s3_key = f"{s3_folder}/{filename}"

    try:
        logging.info(f"Iniciando upload para s3://{bucket_name}/{s3_key}")
        
        # Cliente S3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )

        s3_client.upload_file(file_path, bucket_name, s3_key)
        
        logging.info(f"Upload concluído com sucesso: {s3_key}")
        return True

    except FileNotFoundError:
        logging.error(f"Arquivo não encontrado localmente: {file_path}")
        return False
    except NoCredentialsError:
        logging.error("Credenciais AWS não encontradas.")
        return False
    except ClientError as e:
        logging.error(f"Erro no cliente S3: {e}")
        return False