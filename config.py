import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = os.environ.get("DATA_DIR", "./data")

# Load flag thresholds from YAML config
_CONFIG_PATH = Path(__file__).parent / "flag_config.yaml"
with open(_CONFIG_PATH) as f:
    FLAG_THRESHOLDS = yaml.safe_load(f)
