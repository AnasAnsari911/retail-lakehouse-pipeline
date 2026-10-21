from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  # the project root folder
DATASETS = ROOT / "datasets"
LAKE = ROOT / "lake"
BRONZE_DIR = LAKE / "bronze"
SILVER_DIR = LAKE / "silver"
GOLD_DIR = LAKE / "gold"