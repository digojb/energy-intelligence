import requests
from pathlib import Path


URL = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/balanco_energia_subsistema_ho/BALANCO_ENERGIA_SUBSISTEMA_2026.parquet"

OUTPUT_DIR = Path("data/raw/ons/balanco")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "balanco_energia_2026.parquet"


def download_file():
    print("Iniciando download...")

    response = requests.get(URL, timeout=300)

    response.raise_for_status()

    with open(OUTPUT_FILE, "wb") as file:
        file.write(response.content)

    print(f"Arquivo salvo em: {OUTPUT_FILE}")


if __name__ == "__main__":
    download_file()