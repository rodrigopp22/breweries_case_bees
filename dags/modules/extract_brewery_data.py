import datetime
import json
import logging
import os
import requests

BRONZE_LAYER_PATH = "/opt/airflow/data/1_bronze/"


def save_json_file(breweries_data: list, folder_path: str, page: int) -> None:
    file_path = os.path.join(folder_path, f"breweries_page_{page}.json")
    with open(file_path, "w") as json_file:
        json.dump(breweries_data, json_file)
    logging.info(f"Dados da página {page} salvos em {file_path}")


def get_brewery_data(folder_path: str) -> None:
    url = "https://api.openbrewerydb.org/breweries"
    page = 1
    per_page = 50

    while True:
        response = requests.get(
            url,
            params={
                "page": page,
                "per_page": per_page})
        if response.status_code == 200:
            breweries_data = response.json()
            if not breweries_data:
                break
            save_json_file(breweries_data, folder_path, page)
            page += 1
        else:
            logging.info(
                f"Não foi possível extrair os dados, status code: {response.status_code}")
            break


def run():
    date_now = datetime.date.today()
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info(f"Iniciando a execução do dia {date_now}")
    get_brewery_data(BRONZE_LAYER_PATH)
    logging.info("Fim da extração de dados.")

if __name__ == '__main__':
    run()