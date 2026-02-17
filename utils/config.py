import yaml
from pathlib import Path
import os
from dotenv import load_dotenv

def load_config(path: str = "config/config.yaml") -> dict:
    with open(Path(path), "r") as f:
        return yaml.safe_load(f)

load_dotenv()

def get_mysql_credentials():
    return {
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD")
    }