from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from configparser import ConfigParser
import boto3
import time
import requests

# Variável region_name
region_name = 'us-east-1'

# Função para checar status do website
def _check_website_status(url):
    try:
        response = requests.get(url)
        return response.status_code == 200
    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar o site: {e}")
        return False

# Função para verificar se o website está disponível
def _check_site():
    url = 'https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/mensal/Brasil/'  # Substitua pelo URL desejado
    if not _check_website_status(url):
        return 'site_off'
    return 'site_on'

# Função para buscar as credenciais de acesso
def _get_emr_client():
    config = ConfigParser()
    config_file_dir = '/opt/airflow/config/aws.cfg'
    config.read_file(open(config_file_dir))

    aws_access_key_id = config.get('EMR', 'AWS_ACCESS_KEY_ID')
    aws_secret_access_key = config.get('EMR', 'AWS_SECRET_ACCESS_KEY')

    try:
        return boto3.client(
            'emr', 
            region_name=region_name, 
            aws_access_key_id=aws_access_key_id,  
            aws_secret_access_key=aws_secret_access_key  
        )
    except Exception as e: 
        raise Exception(f'Erro ao conectar à AWS! ERRO: {e}')


# Função para criar o cluster EMR
def _create_emr_cluster(ti):    
    emr_client = _get_emr_client()

    # Parâmetros do cluster EMR
    cluster_name = "etl_queimadas"
    emr_version = 'emr-6.14.0'
    subnet_id = 'subnet-097a3da8438be2e76'
    service_role = 'EMR_DefaultRole2'
    instance_profile = 'EMR_EC2_DefaultRole2'
    instance_type = 'c5.xlarge'
    instance_count = 2

    # Script Bootstrap para instalar bibliotecas
    bootstrap_script = '''#!/bin/bash
    sudo python3 -m pip install requests==2.26.0 boto3 ConfigParser
    '''

    # Salvar o script de bootstrap em um bucket S3
    config = ConfigParser()
    config.read_file(open('/opt/airflow/config/aws.cfg'))
    aws_access_key_id = config.get('EMR', 'AWS_ACCESS_KEY_ID')
    aws_secret_access_key = config.get('EMR', 'AWS_SECRET_ACCESS_KEY')
    bucket_log = config.get('S3', 'BUCKET')
    
    s3_bucket = bucket_log
    s3_key = 'bootstrap/install-requests-logging.sh'
    s3_client = boto3.client(
        's3', 
        region_name=region_name, 
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=bootstrap_script)

    # Criação do Cluster EMR com as credenciais do cliente
    response = emr_client.run_job_flow(
        Name=cluster_name,
        LogUri='s3://logs-emr2/',
        ReleaseLabel=emr_version,
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master node",
                    'Market': 'SPOT',
                    'InstanceRole': 'MASTER',
                    'InstanceType': instance_type,
                    'InstanceCount': 1
                },
                {
                    'Name': "Core - 2",
                    'Market': 'SPOT',
                    'InstanceRole': 'CORE',
                    'InstanceType': instance_type,
                    'InstanceCount': instance_count - 1
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': subnet_id
        },
        Applications=[{'Name': 'Hadoop'}, {'Name': 'Spark'}],
        Configurations=[
            {
                "Classification": "spark-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": {
                            "PYSPARK_PYTHON": "/usr/bin/python3"
                        }
                    }
                ]
            }
        ],
        JobFlowRole=instance_profile,
        ServiceRole=service_role,
        VisibleToAllUsers=True,
        EbsRootVolumeSize=32,
        BootstrapActions=[
            {
                'Name': 'Install libraries',
                'ScriptBootstrapAction': {
                    'Path': f's3://{s3_bucket}/{s3_key}',
                    'Args': []
                }
            }
        ],
        StepConcurrencyLevel=1
    )

    # Pegando o Cluster ID
    cluster_id = response['JobFlowId']
    ti.xcom_push(key='cluster_id', value=cluster_id)

# Função para checar o status do Cluster
def _check_emr_cluster_state(ti):
    emr_client = _get_emr_client()
    cluster_id = ti.xcom_pull(key='cluster_id')

    while True:
        response = emr_client.describe_cluster(ClusterId=cluster_id)
        state = response['Cluster']['Status']['State']

        if state in ['WAITING', 'RUNNING']:
            print(f'Cluster está pronto para adicionar passos... Estado: {state}')
            break
        elif state in ['STARTING', 'BOOTSTRAPPING']:
            print(f'Cluster ainda está iniciando... Estado: {state}')
            time.sleep(10)
        else:
            raise Exception(f'Erro! Estado do cluster: {state}')

# Função para adicionar step_job da camada bronze
def _add_step_job_bronze(ti):
    time.sleep(5)
    cluster_id = ti.xcom_pull(key='cluster_id')
    emr_client = _get_emr_client()
    
    # Código Python no S3
    s3_python_script = 's3://codigosemr/extract_to_bronze.py'  # Código .py para executar script spark

    # Adiciona um passo para executar o código Python
    step_response = emr_client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                'Name': 'Extrair e Carregar na camada Bronze',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        s3_python_script
                    ]
                }
            }
        ]
    )

    step_job_id = step_response['StepIds'][0] 
    ti.xcom_push(key='step_job_id', value=step_job_id)

    # Aguardar a execução do passo
    while True:
        time.sleep(10)  # Aguarda 10 segundos entre as verificações
        step_info = emr_client.describe_step(ClusterId=cluster_id, StepId=step_job_id)['Step']
        step_status = step_info['Status']['State']
        step_job_id = step_info['Id']  # Captura o ID do passo
        step_name = step_info['Name']  # Captura o nome do passo
        print(f'Status do passo "{step_name}" (ID: {step_job_id}): {step_status}')
        
        if step_status in ['COMPLETED', 'FAILED', 'CANCELLED']:
            break

# Função para adicionar step_job da camada silver
def _add_step_job_silver(ti):
    time.sleep(5)
    cluster_id = ti.xcom_pull(key='cluster_id')
    emr_client = _get_emr_client()
    
    # Código Python no S3
    s3_python_script = 's3://codigosemr/to_silver_diaria.py'  # Código .py para executar script spark

    # Adiciona um passo para executar o código Python
    step_response = emr_client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                'Name': 'Transformar e Carregar na Camada Silver',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        s3_python_script
                    ]
                }
            }
        ]
    )

    step_job_id = step_response['StepIds'][0]
    ti.xcom_push(key='step_job_id', value=step_job_id)

    # Aguardar a execução do passo
    while True:
        time.sleep(10)  # Aguarda 10 segundos entre as verificações
        step_info = emr_client.describe_step(ClusterId=cluster_id, StepId=step_job_id)['Step']
        step_status = step_info['Status']['State']
        step_job_id = step_info['Id']  # Captura o ID do passo
        step_name = step_info['Name']  # Captura o nome do passo
        print(f'Status do passo "{step_name}" (ID: {step_job_id}): {step_status}')
        
        if step_status in ['COMPLETED', 'FAILED', 'CANCELLED']:
            break

# Função para adicionar step_job da camada gold estado
def _add_step_job_gold_estado(ti):
    time.sleep(5)
    cluster_id = ti.xcom_pull(key='cluster_id')
    emr_client = _get_emr_client()
    
    # Código Python no S3
    s3_python_script = 's3://codigosemr/to_gold_estado_diaria.py'  # Código .py para executar script spark

    # Adiciona um passo para executar o código Python
    step_response = emr_client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                'Name': 'Agregar e Carregar na Camada Gold - Estado',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        s3_python_script
                    ]
                }
            }
        ]
    )

    step_job_id = step_response['StepIds'][0] 
    ti.xcom_push(key='step_job_id', value=step_job_id)

    # Aguardar a execução do passo
    while True:
        time.sleep(10)  # Aguarda 10 segundos entre as verificações
        step_info = emr_client.describe_step(ClusterId=cluster_id, StepId=step_job_id)['Step']
        step_status = step_info['Status']['State']
        step_job_id = step_info['Id']  # Captura o ID do passo
        step_name = step_info['Name']  # Captura o nome do passo
        print(f'Status do passo "{step_name}" (ID: {step_job_id}): {step_status}')
        
        if step_status in ['COMPLETED', 'FAILED', 'CANCELLED']:
            break

# Função para adicionar step_job da camada gold municipio
def _add_step_job_gold_municipio(ti):
    time.sleep(5)
    cluster_id = ti.xcom_pull(key='cluster_id')
    emr_client = _get_emr_client()
    
    # Código Python no S3
    s3_python_script = 's3://codigosemr/to_gold_municipio_diaria.py'  # Código .py para executar script spark

    # Adiciona um passo para executar o código Python
    step_response = emr_client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                'Name': 'Agregar e Carregar na Camada Gold - Municipio',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        s3_python_script
                    ]
                }
            }
        ]
    )

    step_job_id = step_response['StepIds'][0] 
    ti.xcom_push(key='step_job_id', value=step_job_id)

    # Aguardar a execução do passo
    while True:
        time.sleep(10)  # Aguarda 10 segundos entre as verificações
        step_info = emr_client.describe_step(ClusterId=cluster_id, StepId=step_job_id)['Step']
        step_status = step_info['Status']['State']
        step_job_id = step_info['Id']  # Captura o ID do passo
        step_name = step_info['Name']  # Captura o nome do passo
        print(f'Status do passo "{step_name}" (ID: {step_job_id}): {step_status}')
        
        if step_status in ['COMPLETED', 'FAILED', 'CANCELLED']:
            break

# Função para adicionar step_job da camada gold bioma
def _add_step_job_gold_bioma(ti):
    time.sleep(5)
    cluster_id = ti.xcom_pull(key='cluster_id')
    emr_client = _get_emr_client()
    
    # Código Python no S3
    s3_python_script = 's3://codigosemr/to_gold_bioma_diaria.py'  # Código .py para executar script spark

    # Adiciona um passo para executar o código Python
    step_response = emr_client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                'Name': 'Agregar e Carregar na Camada Gold - Bioma',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        s3_python_script
                    ]
                }
            }
        ]
    )

    step_job_id = step_response['StepIds'][0]  # Corrigido aqui
    ti.xcom_push(key='step_job_id', value=step_job_id)

    # Aguardar a execução do passo
    while True:
        time.sleep(10)  # Aguarda 10 segundos entre as verificações
        step_info = emr_client.describe_step(ClusterId=cluster_id, StepId=step_job_id)['Step']
        step_status = step_info['Status']['State']
        step_job_id = step_info['Id']  # Captura o ID do passo
        step_name = step_info['Name']  # Captura o nome do passo
        print(f'Status do passo "{step_name}" (ID: {step_job_id}): {step_status}')
        
        if step_status in ['COMPLETED', 'FAILED', 'CANCELLED']:
            break
  
# Função para Encerrar o Cluster
def _terminate_emr_cluster(ti):
    cluster_id = ti.xcom_pull(key='cluster_id')
    emr_client = _get_emr_client()
    emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
    print(f'Cluster Id {cluster_id} encerrado!!!')
        
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Criação da DAG
with DAG(
    'etl_queimadas',
    default_args=default_args,
    tags=['aws'],
    start_date=datetime(2024, 10, 1),
    schedule_interval='35 13 * * *', # Executar todo dia as 13:35 UTC (10:35 horário de Brasilia)
    catchup=False
) as dag:

    # task para checar URL
    check_url = BranchPythonOperator(
        task_id='check_url',
        python_callable=_check_site
    )

    # Dummys para decidir o que fazer após a checagem da URL
    site_on = DummyOperator(task_id='site_on')
    site_off = DummyOperator(task_id='site_off')

    # task para criar o cluster EMR na AWS
    create_emr_cluster = PythonOperator(
        task_id='create_emr_cluster',
        python_callable=_create_emr_cluster
    )

    # task para verificar status do cluster
    check_emr_cluster_state = PythonOperator(
        task_id='check_emr_cluster_state',
        python_callable=_check_emr_cluster_state
    )

    # task para adicionar stepjob da camada bronze
    add_step_job_bronze = PythonOperator(
        task_id='add_step_job_bronze',
        python_callable=_add_step_job_bronze
    )

    # task para adicionar stepjob da camada silver
    add_step_job_silver = PythonOperator(
        task_id='add_step_job_silver',
        python_callable=_add_step_job_silver
    )

    # task para adicionar stepjob da camada gold estado
    add_step_job_gold_estado = PythonOperator(
        task_id='add_step_job_gold_estado',
        python_callable=_add_step_job_gold_estado
    )

    # task para adicionar stepjob da camada gold municipio
    add_step_job_gold_municipio = PythonOperator(
        task_id='add_step_job_gold_municipio',
        python_callable=_add_step_job_gold_municipio
    )

    # task para adicionar stepjob da camada gold bioma
    add_step_job_gold_bioma = PythonOperator(
        task_id='add_step_job_gold_bioma',
        python_callable=_add_step_job_gold_bioma
    )

    # task para encerrar o cluster
    terminate_emr_cluster = PythonOperator(
        task_id='terminate_emr_cluster',
        python_callable=_terminate_emr_cluster
    )

    # Ordem de execução das tasks
    check_url >> [site_on, site_off]
    site_on >> create_emr_cluster >> check_emr_cluster_state >> add_step_job_bronze >> add_step_job_silver >> [add_step_job_gold_estado, add_step_job_gold_municipio, add_step_job_gold_bioma] >> terminate_emr_cluster
    # task gold estado, gold bioma e gold municipio rodam paralelamente