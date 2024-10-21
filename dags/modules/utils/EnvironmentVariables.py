import os
from dataclasses import dataclass

@dataclass
class EnvironmentVariables:
    """
    Classe que gerencia as variáveis de ambiente utilizadas no DAG criado.
    
    Atributos:
    bronze_path : str
        Caminho para a camada de bronze, onde estão salvos os dados em .json em sua forma "raw"
    silver_path : str
        Caminho para a camada de prata, onde os dados salvos são resultado da limpeza e transformação dos dados na camada de bronze.
    gold_path : str
        Caminho para a camada de ouro, onde os dados armazenados são normalmente agrupados.
    """
    bronze_path: str = os.getenv('BRONZE')
    silver_path: str = os.getenv('SILVER')
    gold_path: str = os.getenv('GOLD')