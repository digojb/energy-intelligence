import requests
from pathlib import Path


URL = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/curva-carga-ho/CURVA_CARGA_2026.parquet"

OUTPUT_DIR = Path("data/raw/ons/carga")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "curva_carga_2026.parquet"


def download_file():
    print("Iniciando download...")

    response = requests.get(URL)

    response.raise_for_status()

    with open(OUTPUT_FILE, "wb") as file:
        file.write(response.content)

    print(f"Arquivo salvo em: {OUTPUT_FILE}")


if __name__ == "__main__":
    download_file()