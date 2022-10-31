import os
import pandas as pd


def concat_dfs(folder_path):
    # folder_path = 'parsed_data/auxilio_emergencial'

    file_names = os.listdir(folder_path)

    df = pd.DataFrame([])

    total_rows = 0

    for file in file_names:
        date = file[:-4]
        path = os.path.join(folder_path, file)
        if df.empty:
            df = pd.read_csv(path)
            df["date"] = pd.to_datetime(date, format="%Y%m")
            total_rows += len(df)
        else:
            next_df = pd.read_csv(path)
            next_df["date"] = pd.to_datetime(date, format="%Y%m")
            total_rows += len(next_df)
            df = pd.concat([df, next_df], ignore_index=True)

    df.sort_values("date", ascending=True)

    assert len(df) == total_rows

    return df


def substituir_cod_municipio(df):
    # Substituir cod municipio siafi por ibge
    municipios = pd.read_csv("raw_data/municipios/TABMUN.csv")

    siafi_ibge_lookup = {}
    for row in municipios.iterrows():
        cod_siafi = row[1]["cod_siafi"]
        cod_ibge = row[1]["cod_ibge"]

        siafi_ibge_lookup[cod_siafi] = cod_ibge

    if "municipio_siafi" not in df:
        print("DF sem código siafi")
        return df

    df["municipio_siafi"] = df["municipio_siafi"].replace(siafi_ibge_lookup)

    df.columns = ["municipio_ibge", "contagem", "soma", "date"]

    return df
