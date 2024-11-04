from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging

try:
    spark = (
        SparkSession.builder.appName("to_gold_bioma_diario")
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

    # Caminho do diretório no S3 (camada silver e gold)
    s3_silver_path = f"s3a://{s3_bucket}/silver/foco_queimada/diario/extract_year={year}/extract_month={month}/extract_day={day}"
    s3_gold_bioma = f"s3a://{s3_bucket}/gold/foco_bioma/diario/extract_year={year}/extract_month={month}/extract_day={day}"


    try:
        # Ler os arquivos Parquet diretamente do S3 (camada bronze)
        df = spark.read.parquet(s3_silver_path)
        df.createOrReplaceTempView('df')

        # Apenas Satelite de Referência do INPE
        df_referencia = df.filter('SATELITE = "AQUA_M-T"')
        df_referencia.createOrReplaceTempView('df_referencia')

        # Query para gerar Tabela Analitica por Estado
        foco_bioma_agregado = """
        SELECT
            DATREF,
            CURRENT_TIMESTAMP() AS DATVER,
            BIOMA,
            COUNT(ID_FOCO) AS QUANTIDADE_FOCO,
            CASE WHEN COUNT(CASE WHEN FRP <> -999.0 THEN 1 END) = 0 THEN NULL 
                ELSE ROUND(SUM(CASE WHEN FRP <> -999.0 THEN FRP ELSE 0 END), 2) 
            END AS SUM_FRP,
            CASE WHEN COUNT(CASE WHEN FRP <> -999.0 THEN 1 END) = 0 THEN NULL 
                ELSE ROUND(AVG(CASE WHEN FRP <> -999.0 THEN FRP ELSE 0 END), 2) 
            END AS AVG_FRP,
            CASE WHEN COUNT(CASE WHEN FRP <> -999.0 THEN 1 END) = 0 THEN NULL 
                ELSE ROUND(MIN(CASE WHEN FRP <> -999.0 THEN FRP ELSE 0 END), 2) 
            END AS MIN_FRP,
            CASE WHEN COUNT(CASE WHEN FRP <> -999.0 THEN 1 END) = 0 THEN NULL 
                ELSE ROUND(MAX(CASE WHEN FRP <> -999.0 THEN FRP ELSE 0 END), 2) 
            END AS MAX_FRP,
            CASE WHEN COUNT(CASE WHEN numero_dias_sem_chuva <> -999.0 THEN 1 END) = 0 THEN NULL 
                ELSE ROUND(SUM(CASE WHEN numero_dias_sem_chuva <> -999.0 THEN numero_dias_sem_chuva ELSE 0 END), 2) 
            END AS SUM_DIAS_SEM_CHUVA,
            CASE WHEN COUNT(CASE WHEN numero_dias_sem_chuva <> -999.0 THEN 1 END) = 0 THEN NULL 
                ELSE ROUND(AVG(CASE WHEN numero_dias_sem_chuva <> -999.0 THEN numero_dias_sem_chuva ELSE 0 END), 2) 
            END AS AVG_DIAS_SEM_CHUVA,
            CASE WHEN COUNT(CASE WHEN numero_dias_sem_chuva <> -999.0 THEN 1 END) = 0 THEN NULL 
                ELSE ROUND(MIN(CASE WHEN numero_dias_sem_chuva <> -999.0 THEN numero_dias_sem_chuva ELSE 0 END), 2) 
            END AS MIN_DIAS_SEM_CHUVA,
            CASE WHEN COUNT(CASE WHEN numero_dias_sem_chuva <> -999.0 THEN 1 END) = 0 THEN NULL 
                ELSE ROUND(MAX(CASE WHEN numero_dias_sem_chuva <> -999.0 THEN numero_dias_sem_chuva ELSE 0 END), 2) 
            END AS MAX_DIAS_SEM_CHUVA,
            CASE WHEN COUNT(CASE WHEN precipitacao <> -999.0 THEN 1 END) = 0 THEN NULL 
                ELSE ROUND(SUM(CASE WHEN precipitacao <> -999.0 THEN precipitacao ELSE 0 END), 2) 
            END AS SUM_PRECIPITACAO,
            CASE WHEN COUNT(CASE WHEN precipitacao <> -999.0 THEN 1 END) = 0 THEN NULL 
                ELSE ROUND(AVG(CASE WHEN precipitacao <> -999.0 THEN precipitacao ELSE 0 END), 2) 
            END AS AVG_PRECIPITACAO,
            CASE WHEN COUNT(CASE WHEN precipitacao <> -999.0 THEN 1 END) = 0 THEN NULL 
                ELSE ROUND(MIN(CASE WHEN precipitacao <> -999.0 THEN precipitacao ELSE 0 END), 2) 
            END AS MIN_PRECIPITACAO,
            CASE WHEN COUNT(CASE WHEN precipitacao <> -999.0 THEN 1 END) = 0 THEN NULL 
                ELSE ROUND(MAX(CASE WHEN precipitacao <> -999.0 THEN precipitacao ELSE 0 END), 2) 
            END AS MAX_PRECIPITACAO,
            CASE WHEN COUNT(CASE WHEN risco_fogo <> -999.0 THEN 1 END) = 0 THEN NULL 
                ELSE ROUND(SUM(CASE WHEN risco_fogo <> -999.0 THEN risco_fogo ELSE 0 END), 2) 
            END AS SUM_RISCO_FOGO,
            CASE WHEN COUNT(CASE WHEN risco_fogo <> -999.0 THEN 1 END) = 0 THEN NULL 
                ELSE ROUND(AVG(CASE WHEN risco_fogo <> -999.0 THEN risco_fogo ELSE 0 END), 2) 
            END AS AVG_RISCO_FOGO,
            CASE WHEN COUNT(CASE WHEN risco_fogo <> -999.0 THEN 1 END) = 0 THEN NULL 
                ELSE ROUND(MIN(CASE WHEN risco_fogo <> -999.0 THEN risco_fogo ELSE 0 END), 2) 
            END AS MIN_RISCO_FOGO,
            CASE WHEN COUNT(CASE WHEN risco_fogo <> -999.0 THEN 1 END) = 0 THEN NULL 
                ELSE ROUND(MAX(CASE WHEN risco_fogo <> -999.0 THEN risco_fogo ELSE 0 END), 2) 
            END AS MAX_RISCO_FOGO
        FROM
            df_referencia
        GROUP BY DATREF, DATVER, BIOMA
        ORDER BY QUANTIDADE_FOCO DESC
        """

        # Executar a query
        gold_foco_bioma = spark.sql(foco_bioma_agregado)

        # Salvar gold_foco_municipio no S3 (camada gold)
        gold_foco_bioma.write.mode("overwrite").parquet(s3_gold_bioma)
        logging.info("Processamento e salvamento concluídos com sucesso.")

    except Exception as e:
        logging.error(f"Erro ao processar o diretório no S3: {e}")

if __name__ == "__main__":
    main()