import os
from dataclasses import dataclass

@dataclass
class EnvironmentVariables:
    bronze_path: str = os.getenv('BRONZE')
    silver_path: str = os.getenv('SILVER')
    gold_path: str = os.getenv('GOLD')