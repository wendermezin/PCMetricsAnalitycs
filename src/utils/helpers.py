# src/utils/helpers.py

import json
from datetime import datetime

def write_to_log_file(data, filename):
    """
    Escreve dados em uma nova linha de um arquivo de log JSONL.
    """
    try:
        with open(filename, "a") as f:
            f.write(json.dumps(data) + "\n")
        print(f"Dados escritos no arquivo: {filename}")
    except Exception as e:
        print(f"Erro ao escrever no arquivo {filename}: {e}")