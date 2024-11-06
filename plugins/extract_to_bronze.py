import requests
import logging
import time
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# Configurações de logging
logging.basicConfig(level=logging.INFO)

# Configurações
s3_bucket = 'nome_seu_bucket'  # Nome do seu bucket S3
max_attempts = 10  # Máximo de tentativas
base_url = "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/mensal/Brasil/focos_mensal_br_"
headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
}

def get_yesterday_date():
    return (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')

def download_csv(url):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Levanta um erro para códigos de status 4xx/5xx
        logging.info(f"Arquivo baixado com sucesso: {url}")
        return response.content
    except requests.RequestException as e:
        logging.error(f"Erro ao baixar o arquivo: {e}")
        return None

def upload_to_s3(content, s3_key):
    s3_client = boto3.client('s3')
    try:
        s3_client.put_object(Body=content, Bucket=s3_bucket, Key=s3_key)
        logging.info(f'Arquivo enviado para S3: s3://{s3_bucket}/{s3_key}')
        # Logando a localização do arquivo no S3
        logging.info(f'Dados salvos em: s3://{s3_bucket}/{s3_key}')
    except (NoCredentialsError, ClientError) as e:
        logging.error(f"Erro ao enviar para S3: {e}")

def main():
    AAAAMMDD = get_yesterday_date()
    AAAAMM = AAAAMMDD[:6]
    year = AAAAMMDD[:4]
    month = AAAAMMDD[4:6]
    day = AAAAMMDD[6:]
    url = f"{base_url}{AAAAMM}.csv"
    s3_key = f"bronze/foco_queimada/diario/extract_year={year}/extract_month={month}/extract_day={day}/table.csv"  # Caminho no S3 onde o arquivo será salvo

    for attempt in range(1, max_attempts + 1):
        content = download_csv(url)
        if content:
            upload_to_s3(content, s3_key)
            logging.info(f'Dados salvos em {s3_bucket}/{s3_key}')
            break
        else:
            logging.info(f'Tentativa {attempt} falhou. Aguardando antes de tentar novamente...')
            time.sleep(2)
    else:
        logging.error(f"Falha após {max_attempts} tentativas. Não foi possível baixar o arquivo.")

if __name__ == "__main__":
    main()
