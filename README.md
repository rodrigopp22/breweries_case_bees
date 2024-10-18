# Case DE BEES - Breweries
Este repositório contém uma solução para o case de data engineering da BEES. Um pipeline foi desenvolvido para extrair, transformar e carregar dados de cervejarias, tudo orquestrado pelo Apache Airflow.

### Estrutura do repositório

- **dags/**: Contém os DAGs (Directed Acyclic Graphs) do Apache Airflow para orquestrar o pipeline de dados.
  
- **modules/**: 
  - **utils/**: Módulos utilitários usados no pipeline.
    - `EnvironmentVariables.py`: Define e gerencia variáveis de ambiente necessárias para o pipeline.
    - `extract_brewery_data.py`: Script responsável pela extração dos dados das cervejarias de uma API.
    - `load_tb_brewery_by_location.py`: Carrega os dados de cervejarias transformados para um local específico.
    - `transform_brewery_data.py`: Realiza a transformação dos dados extraídos, preparando-os para a fase de carregamento.
    - `brewery_pipeline.py`: Integra e organiza as várias fases do pipeline de dados.

- **data/**: Diretório onde os dados são armazenados durante o pipeline (bronze, silver, gold).

- **docs/**: Contém a documentação relacionada ao projeto.
  
- **logs/**: Diretório usado pelo Airflow para armazenar logs de execução.

- **plugins/**: Contém possíveis plugins do Airflow que possam ser usados no projeto.

- **tests/**: Scripts de teste para garantir a integridade do pipeline e seus módulos.

- **.env**: Arquivo de variáveis de ambiente contendo configurações sensíveis como chaves de API e credenciais.

- **docker-compose.yml**: Arquivo de configuração Docker Compose para facilitar a criação e execução do ambiente com Airflow e seus componentes.

- **Dockerfile**: Contém as instruções para a criação da imagem Docker personalizada para este projeto, incluindo a instalação de dependências.

- **requirements.txt**: Lista as dependências de Python necessárias para o pipeline.

# Como executar o projeto
Para executar a solução, certifique-se que tenha o Docker instalado em sua máquina.
Ao clonar o repositório, você deverá acessá-lo pelo terminal e digitar o seguinte comando:
```
$ docker-compose build
```
Quando ele finalizar o build da imagem do container, ainda no terminal, execute:
```
$ docker-compose up -d
```
Este comando fará com que um container seja inicializado. Ao terminar a configuração inicial, acesse o seu navegador e digite em sua barra de endereços:
```
localhost:8080/
```
Para o case, o usuário e senha são o padrão.
Para iniciar o DAG, clique em "brewery_pipeline" e aperte executar.