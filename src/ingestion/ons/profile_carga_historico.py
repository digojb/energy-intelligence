import pandas as pd


URL = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/curva-carga-ho/CURVA_CARGA_2026.parquet"


df = pd.read_parquet(URL)

print("\n========== PERÍODO ==========")
print("Mínimo:", df["din_instante"].min())
print("Máximo:", df["din_instante"].max())

print("\n========== SUBSISTEMAS ==========")
print(
    df[
        ["id_subsistema", "nom_subsistema"]
    ].drop_duplicates().sort_values("id_subsistema")
)

print("\n========== REGISTROS POR SUBSISTEMA ==========")
print(
    df.groupby(
        ["id_subsistema", "nom_subsistema"]
    ).size()
)

print("\n========== DUPLICIDADES ==========")
print(
    "Linhas duplicadas:",
    df.duplicated().sum()
)

print("\n========== DUPLICIDADE DA CHAVE ==========")

chave = [
    "id_subsistema",
    "din_instante"
]

print(
    "Chaves duplicadas:",
    df.duplicated(subset=chave).sum()
)

print("\n========== CARGA ==========")
print(df["val_cargaenergiahomwmed"].describe())

print("\n========== CARGA NEGATIVA ==========")
print(
    (df["val_cargaenergiahomwmed"] < 0).sum()
)

print("\n========== FREQUÊNCIA TEMPORAL ==========")

intervalos = (
    df
    .sort_values(["id_subsistema", "din_instante"])
    .groupby("id_subsistema")["din_instante"]
    .diff()
    .value_counts()
)

print(intervalos.head(10))