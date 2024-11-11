# Queimadas
Repositório com códigos para extração, tratamento e carga de dados de Queimadas do INPE em um Lake

## Índice
- [Sobre](#sobre)
- [Dados](#fonte-dos-dados)
- [Metadados](#metadados)
- [Ambiente AWS](#ambiente-aws)
- [Airflow](#gerenciamento-de-processos-com-airflow)
- [Instalação](#instalação-do-ambiente)
- [Autor](#autor)

## Sobre

Este projeto foi desenvolvido para automatizar a extração, transformação e carga (ETL) de dados sobre queimadas obtidos do Instituto Nacional de Pesquisas Espaciais (*INPE*). Utilizando uma arquitetura de lakehouse baseada em camadas (*Bronze*, *Silver* e *Gold*), o sistema organiza e processa os dados de forma eficiente, garantindo a qualidade e a governança necessária para análises e relatórios.

Os dados são extraídos diretamente do site do INPE, que disponibiliza informações sobre focos de queimadas em formato CSV. A escolha do formato mensal é baseada na maior confiabilidade das informações, que incluem detalhes geográficos, meteorológicos e características dos incêndios. Cada registro representa um foco de incêndio, identificado de forma única, e os dados de múltiplos satélites são armazenados, focando no satélite de referência *AQUA_M-T*.

Para gerenciar a infraestrutura, utilizamos **AWS** (Amazon Web Services), que oferece uma plataforma escalável e segura para armazenar os dados no **Amazon S3**. A execução dos processos de **ETL** é realizada através de um cluster do **AWS EMR** (Elastic MapReduce), que permite o processamento distribuído de grandes volumes de dados utilizando **Apache Spark**. A comunicação entre as diversas etapas do fluxo de trabalho é orquestrada pelo **Apache Airflow**, uma ferramenta de gerenciamento de workflows que automatiza a execução de jobs em sequência, garantindo que cada etapa do processo seja concluída antes de prosseguir para a próxima.

A extração dos dados é realizada via *web scraping*, técnica que permite a coleta de informações de sites da web de forma automatizada. Com o uso de scripts Python, os dados são baixados e armazenados na camada Bronze, prontos para as transformações necessárias nas camadas subsequentes. Este processo não só otimiza o fluxo de trabalho, mas também assegura a atualização contínua das informações, permitindo análises em tempo real sobre as queimadas no Brasil.

Dessa forma, o projeto não apenas atende à necessidade de um sistema robusto de coleta e análise de dados ambientais, mas também contribui para a conscientização e monitoramento das queimadas, um tema de extrema relevância no contexto atual de mudanças climáticas e preservação ambiental.

<img src=https://github.com/KleuberFav/queimadas/blob/main/artefatos/fluxo_queimadas.png/>

## Fonte dos Dados

A fonte dos dados está no site do [INPE](https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/mensal/Brasil/). Os dados estão armazenados em arquivos CSV e organizados em pastas, onde cada pasta corresponde a um ano-mes. Há informações mensais, diárias e a cada 10 minutos, mas optei pela mensal, pois tem informações mais confiáveis. As informações mensais são atualizadas todo dia. Possui informações de localização (estado, municipio, bioma, latitude, longitude), dias sem chuva, precipitação, risco fogo e FRP (indica a quantidade de energia liberada por uma área em chamas em um dado instante e pode ser calculado com base em dados de sensores remotos, como satélites). Também há uma coluna informando o Satélite, como mais de um satélite pode obter informação do mesmo foco de incêndio, optei por gravar os dados de todos os satélites até a camada silver, porém consolidei na camada gold apenas o Satélite de Referência usado pelo INPE (AQUA_M-T). Cada registro é um foco de incêndio e cada foco de incêndio tem seu id único.

Validei as informações através do [Terrabrasilis](https://terrabrasilis.dpi.inpe.br/queimadas/bdqueimadas/), um sistema de monitoramento ambiental desenvolvido pelo INPE que disponibiliza dados geoespaciais e análises sobre desmatamento, queimadas, uso e cobertura da terra, entre outros temas ambientais no Brasil. 

<img src=https://github.com/KleuberFav/queimadas/blob/main/artefatos/fonte_dados.png/>

## Metadados

| **tabela_origem**   | **coluna_origem**            | **tipo_coluna_origem** | **tabela_intermediaria**      | **coluna_intermediaria**          | **tipo_coluna_intermediaria**   | **tabelas_finais**     | **coluna_final**            | **tipo_coluna_final**       |
|----------------------|------------------------------|-------------------------|--------------------------|------------------------------|----------------------------|-------------------------|----------------------------------|----------------------------------|
| bronze             | id                           | STRING                  | silver            | id_foco                      | STRING                     | gold_bioma, gold_estado, gold_municipio            | QUANTIDADE_FOCO                 | INT                              |
| bronze             | data_hora_gmt               | TIMESTAMP               | silver            | data_hora_gmt               | TIMESTAMP                  | -                       | -                                | -                                |
| bronze             | lat                          | DOUBLE                  | silver            | lat                          | DOUBLE                     | -                       | -                                | -                                |
| bronze             | lon                          | DOUBLE                  | silver            | lon                          | DOUBLE                     | -                       | -                                | -                                |
| bronze             | satelite                     | STRING                  | silver            | satelite                     | STRING                     | -                       | -                                | -                                |
| bronze             | pais                         | STRING                  | silver            | pais                         | STRING                     | -                       | -                                | -                                |
| bronze             | municipio_id                 | INT                     | silver            | municipio_id                 | INT                        | -                       | -                                | -                                |
| bronze             | estado_id                    | INT                     | silver            | estado_id                    | INT                        | -                       | -                                | -                                |
| bronze             | pais_id                      | INT                     | silver            | pais_id                      | INT                        | -                       | -                                | -                                |
| bronze             | municipio                    | STRING                  | silver            | municipio                    | STRING                     | gold_municipio                       | MUNICIPIO                                | STRING                                |
| bronze             | estado                       | STRING                  | silver            | estado                       | STRING                     | gold_estado                       | ESTADO                                | STRING                                |
| bronze             | bioma                        | STRING                  | silver            | bioma                        | STRING                     | gold_bioma                       | BIOMA                                | STRING                                |
| bronze             | numero_dias_sem_chuva       | SMALLINT                | silver            | numero_dias_sem_chuva       | SMALLINT                  | gold_bioma, gold_estado, gold_municipio            | SUM_DIAS_SEM_CHUVA, AVG_DIAS_SEM_CHUVA, MIN_DIAS_SEM_CHUVA, MAX_DIAS_SEM_CHUVA | DOUBLE                  |
| bronze             | precipitacao                 | DOUBLE                  | silver            | precipitacao                 | DOUBLE                     | gold_bioma, gold_estado, gold_municipio            | SUM_PRECIPITACAO, AVG_PRECIPITACAO, MIN_PRECIPITACAO, MAX_PRECIPITACAO | DOUBLE                  |
| bronze             | risco_fogo                   | DOUBLE                  | silver            | risco_fogo                   | DOUBLE                     | gold_bioma, gold_estado, gold_municipio            | SUM_RISCO_FOGO, AVG_RISCO_FOGO, MIN_RISCO_FOGO, MAX_RISCO_FOGO     | DOUBLE                  |
| bronze             | frp                          | DOUBLE                  | silver            | frp                          | DOUBLE                     | gold_bioma, gold_estado, gold_municipio            | SUM_FRP, AVG_FRP, MIN_FRP, MAX_FRP | DOUBLE                  |




### Ambiente AWS

#### 1. AWS S3
A arquitetura de lakehouse baseada em camadas de Lake Medalhão (ou Medallion Architecture) é uma abordagem que organiza os dados em um data lake em três camadas principais: Bronze, Silver e Gold. Essa estrutura facilita o processamento, a análise e a governança dos dados, especialmente em ambientes de big data. As camadas estão armazenadas em um bucket no **AWS S3**

Os dados são armazenados em diretórios no formato *extract_year=year/extract_month=month/extract_day=day/* e no formato parquet a partir da camada silver. A combinação desses métodos oferece uma solução robusta para armazenamento e análise de dados, otimizando a organização e o desempenho em ambientes de Big Data

<img src=https://github.com/KleuberFav/queimadas/blob/main/artefatos/lake.png/>

- **Bronze**: Esta é a camada de entrada onde os dados são armazenados em seu formato bruto, ou seja, exatamente como foram extraídos das fontes originais 
- **Silver**: Nesta camada, os dados são refinados e limpos: aqui ocorre a transformação, enriquecimento e normalização dos dados da camada Bronze. Os dados são carregados na silver no formato parquet
- **Gold**: Esta é a camada de dados prontos para consumo, onde estão armazenados dados de alta qualidade e que já passaram por validações rigorosas. Contém dados agregados e já transformados em formatos específicos para necessidades de negócio. Os dados são carregados na gold no formato parquet

Um repositório foi criado no **AWS S3** para armazenar códigos em Python (.py) que executam scripts Spark para o processo de ETL (Extração, Transformação e Carga). Cada script é dedicado a uma camada ou tabela específica da arquitetura, garantindo uma organização clara e facilitando a manutenção e a escalabilidade do sistema.

#### 2. AWS EMR
Foi utilizado um cluster do AWS EMR para executar os jobs Spark. As principais configurações e implementações realizadas foram:
- Os jobs executam os scripts em Python (.py) armazenados no repositório.
- Uma **VPC** pública foi configurada para permitir que o cluster acesse o site do INPE e extraia os dados necessários.
- Foram configuradas **AWS IAM roles** para garantir que o cluster tenha permissões adequadas de leitura e escrita no S3.

## Gerenciamento de Processos com Airflow

O Airflow foi utilizado para fazer o gerenciamento de todo o Pipeline.

### DAG

[etl_queimadas](https://github.com/KleuberFav/queimadas/blob/main/dags/dag_cria_emr.py)

<img src=https://github.com/KleuberFav/queimadas/blob/main/artefatos/fluxco_dag.png/>

Esta DAG foi utilizada para gerenciar o fluxo de trabalho no Lake, incluindo:

#### 1. Verificação do Status do Website**:
  - Verifica o status do site, se retornar status code = 200 continua a execução do Pipeline, caso contrário, encerra o fluxo.

#### 2. **Inicialização do Cluster**:
  - O cluster é inicializado apenas se o website estiver acessível, evitando gastos desnecessários.
  - O cluster possui diversas configurações, incluindo a quantidade de nós, a região e outros parâmetros relevantes.
  - Durante a inicialização, é realizado um processo de bootstrap para instalar as bibliotecas Python necessárias para o fluxo de trabalho.

#### 3. **Verificação da Saúde do Cluster**:
  - O status do cluster é monitorado, aguardando enquanto ele estiver nas fases "STARTING" ou "BOOTSTRAPPING". Se o status mudar para "WAITING", a tarefa é encerrada e a próxima etapa é iniciada.

#### 4. **Step Jobs**:
Após a completa inicialização do cluster, o primeiro job do Step é executado.
A partir do segundo step job, o Airflow foi configurado para executar cada step job apenas se o anterior for executado por completo

- **Extração e Ingestão na Camada Bronze**:
  
  - A extração dos dados brutos é feita diretamente do site do INPE e os dados são carregados na camada sem nenhuma transformação. 
  - O Airflow foi configurado para aguardar até o script for executado por completo
  - script usado: [extract_to_bronze](https://github.com/KleuberFav/queimadas/blob/main/plugins/extract_to_bronze.py).

- **Limpeza, Transformação e Carga na Camada Silver**:
  - Os dados são transformados, tratando nulos e ajustando a tipagem usando **Spark**.
  - script usado: [to_silver_diaria](https://github.com/KleuberFav/queimadas/blob/main/plugins/to_silver_diaria.py).

- **Agregações e Carga na Camada Gold**:
  - Os dados são filtrados com base no satélite de referência e agregados por Estado, Município e Bioma utilizando **Spark**.
  - Para cada uma dessas visões (Estado, Município e Bioma), foi desenvolvido um script específico, resultando em três tabelas, uma para cada visão.
  - Esses 3 scripts são executados em paralelo
  - scripts usado: 
    * [to_gold_estado_diaria](https://github.com/KleuberFav/queimadas/blob/main/plugins/to_gold_estado_diaria.py)
    * [to_gold_municipio_diaria](hhttps://github.com/KleuberFav/queimadas/blob/main/plugins/to_gold_municipio_diaria.py)
    * [to_gold_bioma_diaria](https://github.com/KleuberFav/queimadas/blob/main/plugins/to_gold_bioma_diaria.py)

#### 5. **Encerramento do Cluster**

- Após a execução completa de todos os steps jobs, o cluster é encerrado completamente.
- O Fluxo é encerrado.

## Instalação do ambiente

Para configurar e rodar este projeto, certifique-se de que você possui os seguintes pré-requisitos configurados:

### 1. Docker e Docker Compose

O projeto utiliza Docker para containerizar o ambiente. Certifique-se de que o Docker e o Docker Compose estão instalados na sua máquina.

O arquivo `docker-compose.yml` configura os serviços necessários, como Airflow, PostgreSQL e Redis. Ele também define as variáveis de ambiente e volumes necessários para o funcionamento do Airflow.

- **Localização**: O arquivo [`docker-compose.yml`](https://github.com/KleuberFav/queimadas/blob/main/docker-compose.yaml) está na raiz do projeto.
- **Função**: Gerencia a orquestração dos contêineres Docker.

### 2. Plugins e DAGs

- **DAGs**: As DAGs que definem o fluxo de trabalho do Airflow estão localizadas na pasta `dags/`.
- **Scripts S3**: Os scripts Python para rodar os steps jobs estão localizados na pasta `plugins/`. 

### 3. Lembretes
Lembre-se de modificar o nome do bucket e configurar acessos e permissões.
Lembre-se de carregar os scripts da pasta `plugins/` em um diretório no s3 e mude o nome desse diretório na dag, em todas as funções de step_jobs.
Crie uma pasta chamada config e dentro dela um arquivo chamadao aws.cfg. Dentro desse arquivo inclua as seguintes informações:

```
[EMR]
AWS_ACCESS_KEY_ID = sua_chave_acesso
AWS_SECRET_ACCESS_KEY = sua_chave_secreta_acesso

[S3]
BUCKET = nome_seu_bucket
```


### 4. Execução do Projeto

Para rodar o projeto, siga os passos abaixo:

1. **Construa os contêineres**: 
   ```bash
   docker-compose up --build

## Autor
**Kleuber Favacho** - *Engenheiro de Dados, Analista de Dados e Estatístico* 
- [Github](https://github.com/KleuberFav)
- [Linkedin](https://www.linkedin.com/in/kleuber-favacho/)

