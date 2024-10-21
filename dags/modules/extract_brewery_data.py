import datetime
import json
import logging
import os
import requests
from dags.modules.utils.EnvironmentVariables import EnvironmentVariables


def save_json_file(breweries_data: list, folder_path: str, page: int) -> None:
    """
    Salva os dados coletados na API em formato .json concatenado com o número da página
    da API.

    Argumentos:
    breweries_data : list
        Uma lista de dicionários contendo os dados das cervejarias.
    folder_path : str
        O caminho para a pasta onde o arquivo JSON será salvo.
    page : int
        O número da página que será incluído no nome do arquivo JSON.
    """
    try:
        file_path = os.path.join(folder_path, f"breweries_page_{page}.json")
        with open(file_path, "w") as json_file:
            json.dump(breweries_data, json_file)
        logging.info(f"Dados da página {page} salvos em {file_path}")
    except (OSError, IOError) as e:
        logging.error(
            f"Erro ao salvar dados da página {page} em {file_path}. Mensagem de erro: {e}")
        raise


def get_brewery_data(folder_path: str) -> None:
    """
    Extrai dados de cervejarias de uma API e os salva em arquivos .json
    Argumentos:
    folder_path : str
        O caminho para a pasta onde os arquivos .json serão salvos.
    """
    url = "https://api.openbrewerydb.org/breweries"
    page = 1
    per_page = 50
    while True:
        try:
            response = requests.get(
                url,
                params={
                    "page": page,
                    "per_page": per_page,
                    "timeout": 10})
            response.raise_for_status()
            breweries_data = response.json()
            if not breweries_data:
                logging.info(f"Foram processadas {page} páginas.")
                break
            save_json_file(breweries_data, folder_path, page)
            page += 1
        except json.JSONDecodeError as e:
            logging.error(f"Erro ao decodificar JSON da página {page}: {e}")
            break
        except Exception as e:
            logging.error(f"Erro na página {page}: {e}")
            break


def run():
    date_now = datetime.date.today()
    bronze_layer_path = EnvironmentVariables.bronze_path
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info(f"Iniciando a execução do dia {date_now}")
    try:
        get_brewery_data(bronze_layer_path)
        logging.info("Fim da extração de dados.")
    except Exception as e:
        logging.error(f"Erro durante a execução. Mensagem de erro: {e}")


if __name__ == '__main__':
    run()
