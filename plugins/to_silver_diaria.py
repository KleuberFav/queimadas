from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import logging

try:
    spark = (
        SparkSession.builder.appName("to_silver_diario")
        .config("spark.driver.memory", "4g")  # Reduzindo a memória do driver
        .config("spark.executor.memory", "4g")  # Reduzindo a memória do executor
        .config("spark.yarn.executor.memoryOverhead", "512m")  # Ajuste conforme necessário
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.executor.cores", "4")
        .config("spark.default.parallelism", "24")  # Ajuste conforme o cluster
        .config("spark.sql.shuffle.partitions", "50")  # Ajuste conforme o cluster
        .config("spark.task.cpus", "1")
        .getOrCreate()
    )
except Exception as e:
    print(f"Erro ao inicializar a sessão Spark: {e}")
    raise

# Configurações de logging
logging.basicConfig(level=logging.INFO)

# Configurações do bucket S3
s3_bucket = '001-lake-ambiental'  # Nome do seu bucket S3

# Função para pegar a data de ontem
def get_yesterday_date():
    return (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')

# Função principal
def main():
    # Data de ontem no formato AAAAMMDD
    AAAAMMDD = get_yesterday_date()
    year = AAAAMMDD[:4]
    month = AAAAMMDD[4:6]
    day = AAAAMMDD[6:]

    # Caminho do diretório no S3 (camada bronze e silver)
    s3_bronze_path = f"s3a://{s3_bucket}/bronze/foco_queimada/diario/extract_year={year}/extract_month={month}/extract_day={day}/table.csv"
    s3_silver_path = f"s3a://{s3_bucket}/silver/foco_queimada/diario/extract_year={year}/extract_month={month}/extract_day={day}"

    try:
        # Ler os arquivos Parquet diretamente do S3 (camada bronze)
        df = spark.read.csv(s3_bronze_path,header=True, inferSchema=False)
        df.createOrReplaceTempView('df')  # Criar a visualização temporária para as queries

        # Query de limpeza com a data AAAAMMDD
        query_clear = f"""
        SELECT
            id,
            lat,
            lon,
            data_hora_gmt,
            satelite,
            municipio,
            estado,
            pais,
            municipio_id,
            estado_id,
            pais_id,
            CASE 
                WHEN numero_dias_sem_chuva IS NULL OR numero_dias_sem_chuva = -999 THEN -999 
                ELSE numero_dias_sem_chuva 
            END AS numero_dias_sem_chuva,
            CASE 
                WHEN precipitacao IS NULL OR precipitacao = -999 THEN -999 
                ELSE precipitacao 
            END AS precipitacao,
            CASE 
                WHEN risco_fogo IS NULL OR risco_fogo = -999 THEN -999 
                ELSE risco_fogo 
            END AS risco_fogo,
            bioma,
            CASE 
                WHEN frp IS NULL OR frp < 0 THEN -999 
                ELSE frp 
            END AS frp
        FROM
            df
        WHERE
            date_format(data_hora_gmt, 'yyyyMMdd') = '{AAAAMMDD}'
        """

        # Executar a query de limpeza
        df_clear = spark.sql(query_clear).cache()
        df_clear.createOrReplaceTempView('df_clear')

        # Query de tipagem dos dados
        sql_tipagem = f"""
        SELECT
            CAST(id AS STRING) AS id_foco,
            CAST(data_hora_gmt AS TIMESTAMP) AS data_hora_gmt,
            CAST('{AAAAMMDD}' AS STRING) as DATREF,
            CAST(CURRENT_TIMESTAMP() AS TIMESTAMP) AS DATVER,
            CAST(EXTRACT(YEAR FROM DATA_HORA_GMT) AS SMALLINT) AS ANO,
            CAST(EXTRACT(MONTH FROM DATA_HORA_GMT) AS SMALLINT) AS MES,
            CAST(EXTRACT(DAY FROM DATA_HORA_GMT) AS SMALLINT) AS DIA,
            CAST(EXTRACT(HOUR FROM DATA_HORA_GMT) AS SMALLINT) AS HORA,
            CAST(EXTRACT(MINUTE FROM DATA_HORA_GMT) AS SMALLINT) AS MINUTO,
            CAST(lat AS DOUBLE) AS lat,
            CAST(lon AS DOUBLE) AS lon,
            CAST(satelite AS STRING) AS satelite,
            CAST(municipio AS STRING) AS municipio,
            CAST(estado AS STRING) AS estado,
            CAST(pais AS STRING) AS pais,
            CAST(municipio_id AS INT) AS municipio_id,
            CAST(estado_id AS INT) AS estado_id,
            CAST(pais_id AS INT) AS pais_id,
            CAST(numero_dias_sem_chuva AS SMALLINT) AS numero_dias_sem_chuva,
            CAST(precipitacao AS DOUBLE) AS precipitacao,
            CAST(risco_fogo AS DOUBLE) AS risco_fogo,
            CAST(bioma AS STRING) AS bioma,
            CAST(frp AS DOUBLE) AS frp
        FROM
            df_clear
        """

        # Executar a query de tipagem
        df_tipagem = spark.sql(sql_tipagem)

        # Reparticionar o DataFrame para evitar muitos arquivos pequenos no S3
        df_tipagem = df_tipagem.repartition(1)

        # Salvar df_tipagem no S3 (camada silver)
        df_tipagem.write.mode("overwrite").parquet(s3_silver_path)
        logging.info("Processamento e salvamento concluídos com sucesso.")

    except Exception as e:
        logging.error(f"Erro ao processar o diretório no S3: {e}")

if __name__ == "__main__":
    main()
