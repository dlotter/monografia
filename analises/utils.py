import os
import pandas as pd


def remove_last_digit(x):
    if len(str(x)) == 7:
        return int(str(x)[:-1])
    else:
        return int(str(x))


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


def criar_dataset_completo():
    data_type = "files"  # files ou api

    auxilio_emergencial = concat_dfs(
        f"parsed_data/auxilio_emergencial/{data_type}"
    )

    bolsa_familia = concat_dfs(f"parsed_data/bolsa_familia/{data_type}")

    # suicidios = pd.read_csv("parsed_data/suicidios/suicidios.csv")
    # suicidios["date"] = pd.to_datetime(suicidios["date"])
    # suicidios["municipio_ibge"] = suicidios["municipio_ibge"].astype(float)

    populacao = pd.read_csv("parsed_data/municipios/populacao.csv")

    internacoes = pd.read_csv("parsed_data/internacoes/internacoes.csv")
    internacoes["date"] = pd.to_datetime(internacoes["date"])
    internacoes["municipio_ibge6"] = internacoes["municipio_ibge6"].astype(
        float
    )

    bolsa_familia = substituir_cod_municipio(bolsa_familia)

    auxilio_emergencial = auxilio_emergencial[
        auxilio_emergencial["municipio_ibge"].notna()
    ]
    auxilio_emergencial["municipio_ibge"] = auxilio_emergencial[
        "municipio_ibge"
    ].astype(int)
    # suicidios["municipio_ibge"] = suicidios["municipio_ibge"].astype(int)
    internacoes["municipio_ibge6"] = internacoes["municipio_ibge6"].astype(int)

    df_all = pd.merge(
        bolsa_familia,
        auxilio_emergencial,
        on=["municipio_ibge", "date"],
        how="outer",
    )

    # df_all = pd.merge(
    #     df_all, suicidios, on=["municipio_ibge", "date"], how="outer"
    # )

    df_all["year"] = df_all["date"].dt.year

    df_all["municipio_ibge6"] = df_all["municipio_ibge"].apply(
        lambda x: remove_last_digit(x)
    )

    df_all = pd.merge(
        df_all, populacao, on=["municipio_ibge6", "year"], how="outer"
    )

    df_all = pd.merge(
        df_all, internacoes, on=["municipio_ibge6", "date"], how="outer"
    )

    df_all = df_all[df_all["municipio_ibge"] != 0]

    df_all["internacao_jovens"] = (
        df_all["Menor 1 ano"]
        + df_all["1 a 4 anos"]
        + df_all["5 a 9 anos"]
        + df_all["10 a 14 anos"]
        + df_all["15 a 19 anos"]
        + df_all["20 a 24 anos"]
        + df_all["25 a 29 anos"]
    )

    # ordering columns
    columns = [
        "municipio_ibge",
        "contagem_x",
        # "soma_x",
        "date",
        # "soma_y",
        "contagem_y",
        # "suicidios",
        # "obitos_totais",
        # "taxa_suicidio",
        "year",
        "municipio_ibge6",
        "populacao",
        "nome",
        "total_internacoes_psico",
        "total_internacoes_geral",
        "internacao_jovens",
    ]
    df_all = df_all[columns]

    df_all.columns = [
        "municipio_ibge",
        "beneficiarios_bolsa_familia",
        # "valor_bolsa_familia",
        "date",
        # "valor_auxilio_emergencial",
        "beneficiarios_auxilio_emergencial",
        # "suicidios",
        # "obitos_totais",
        # "taxa_suicidio",
        "ano",
        "municipio_ibge6",
        "populacao",
        "nome_municipio",
        "total_internacoes_psico",
        "total_internacoes_geral",
        "internacao_jovens",
    ]

    date_mask = (df_all["date"] >= "2019-08") & (df_all["date"] <= "2020-12")
    df_all = df_all[date_mask]

    df_all["beneficiarios_bolsa_familia"] = df_all[
        "beneficiarios_bolsa_familia"
    ].fillna(0)
    df_all["beneficiarios_auxilio_emergencial"] = df_all[
        "beneficiarios_auxilio_emergencial"
    ].fillna(0)

    # Dropando municipios sem dados de populacao (37 linhas)
    df_all.dropna(subset="populacao", inplace=True)

    # preenchendo dados faltantes de obitos e suicidios (assume-se que são 0 nessas datas)
    # df_all["obitos_totais"].fillna(0, inplace=True)
    # df_all["suicidios"].fillna(0, inplace=True)
    # df_all["taxa_suicidio"].fillna(0, inplace=True)

    assert df_all.isna().sum().sum() == 0

    # # Não usado
    # df_all["valor_bolsa_familia"] = df_all["valor_bolsa_familia"].fillna(0)
    # df_all["valor_auxilio_emergencial"] = df_all[
    #     "valor_auxilio_emergencial"
    # ].fillna(0)
    # df_all["valor_total"] = (
    #     df_all["valor_bolsa_familia"] + df_all["valor_auxilio_emergencial"]
    # )

    # df_all["valor_medio"] = df_all["valor_total"] / (
    #     df_all["beneficiarios_auxilio_emergencial"]
    #     + df_all["beneficiarios_bolsa_familia"]
    # )

    df_all["cobertura_ae"] = (
        df_all["beneficiarios_auxilio_emergencial"] / df_all["populacao"]
    )
    df_all["cobertura_bf"] = (
        df_all["beneficiarios_bolsa_familia"] / df_all["populacao"]
    )

    df_all["taxa_internacoes"] = (
        df_all["total_internacoes_psico"] / df_all["total_internacoes_geral"]
    )
    df_all["taxa_internacoes_jovens"] = (
        df_all["internacao_jovens"] / df_all["total_internacoes_geral"]
    )

    df_all["internacoes_por_100"] = (
        df_all["total_internacoes_psico"] * 100
    ) / df_all["populacao"]
    df_all["internacoes_jovens_por_100"] = (
        df_all["internacao_jovens"] * 100
    ) / df_all["populacao"]

    return df_all
