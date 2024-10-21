## Sobre a solução

A solução foi feita com o objetivo de resolver o case de engenharia de dados da BEES utilizando Airflow, Python e PySpark.

O DAG é criado pelo script brewery_pipeline.py, nele são definidas as ordens de precedência de execução das tarefas, bem como o seu scheduler e as quantidades de retentativas em caso de falha na execução. Para o case, não foi desenvolvida uma maneira de envio de e-mail em caso de falha, o que poderia ser uma solução interessante para acompanhar o status do pipeline.

Para simular um _data lake_, quando o Docker é iniciado, a pasta **data/** é criada juntamente com **data/1_bronze**, **data/2_silver** e **data/3_gold**. Cada uma destas pastas representa uma camada da arquitetura medallion, caso a solução utilizasse de serviços de cloud, essas pastas poderiam ser pastas de um bucket, como o S3 ou o Cloud Storage.

A primeira _task_ do DAG é o script **extract_brewery_data.py**. Ele é responsável por extrair os dados da API através da biblioteca _requests_ do Python. Esta API tem paginação e, portanto, foi necessário percorrer suas páginas para obter todos os dados disponíveis, cada página é salva com o seu número e em .json no que seria a camada de bronze do _data lake_. Esses dados não são modificados e nem tratados.

O segunda _task_ do DAG é a **transform_brewery_data.py**. Esse script é responsável por aplicar um schema aos dados da camada bronze, filtrar e tratá-los. No tratamento, checamos se há países e tipos de cervejaria nulos e removemos eles, removemos os identificadores (ids) duplicados e, por fim, removemos um espaço a mais que tem em um caso do país _United States_. Ao final da transformação dos dados, eles são salvos em formato **Parquet** e particionados por país.
Note que essa é a primeira task em que o PySpark é utilizado, apesar da quantidade de dados não ser grande o suficiente para o uso do Spark, a escolha para essa ferramenta foi puramente para demonstrar o uso dela.

Por fim, a última _task_ do DAG é o script **load_tb_brewery_by_location.py**. Ele é responsável por gerar a visão agregada dos dados. É um script que poderia ser uma query em SQL em algum Data Warehouse ou Lakehouse. Novamente o uso do PySpark aqui é apenas para demonstrar o seu uso, não é necessário e provavelmente afeta o desempenho da solução.

Em termos de monitoramento, logs simples foram feitos para demonstrarem a viabilidade para acompanhar a execução das _tasks_, esses logs poderiam ser observados em ferramentas como o AWS CloudWatch e o GCP Logs Explorer, por exemplo.
Em termos de qualidade de dados, um _framework_ de qualidade de dados poderia ser utilizado, como o **Great Expectations**. Nele, é possível criar testes de dados diretamente nos dataframes criados no Spark. Há também outras decisões de arquitetura que podem ser tomadas neste sentido, como por exemplo a criação de uma "reciclagem de dados" ou de uma quarentena para a validação deles - para o case, provavelmente esses dois últimos casos não seriam tão úteis.

Na pasta docs/ há uma proposta de arquitetura deste case em um ambiente GCP - o objetivo é demonstrar a familiaridade com cloud e alguns de seus serviços.