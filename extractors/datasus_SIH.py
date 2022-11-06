from pysus.online_data.SIH import download
import pandas as pd
import datetime


estados = [
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
]
anos = [2019, 2020]
cid10_transtornos = {
    "abusos_substancias": [f"F{x}" for x in range(100, 200)]
    + [f"F{x}" for x in range(10, 20)],
    "t_esquizofrenicos": [f"F{x}" for x in range(200, 210)] + ["F20"],
    "t_psicoticos": ["F230", "F231", "F232", "F233", "F238", "F239"]
    + ["F23", "F28", "F29"],
    "t_depressao": [
        "F32",
        "F320",
        "F321",
        "F322",
        "F323",
        "F328",
        "F329",
        "F33",
        "F330",
        "F331",
        "F332",
        "F333",
        "F334",
        "F338",
        "F339",
    ],
    "t_ansiedade": ["F41", "F410", "F411", "F412", "F413", "F418", "F419"],
    "t_estresse": ["F43", "F430", "F431", "F432", "F438", "F439"],
    "t_comportamentais": [
        "F50",
        "F500",
        "F501",
        "F502",
        "F503",
        "F504",
        "F505",
        "F508",
        "F509",
        "F51",
        "F510",
        "F511",
        "F512",
        "F513",
        "F514",
        "F515",
        "F518",
        "F519",
    ],
}

banco = {}
for y in anos:
    # for y in [2019]:
    for month in range(1, 13):
        # for month in [1]:
        for uf in estados:
            banco[uf, y, month] = download(state=uf, year=y, month=month)
            print(
                "Banco de "
                + str(y)
                + "/"
                + str(month)
                + " de "
                + str(uf)
                + " baixado!"
            )

        todos = pd.concat(
            {k: pd.DataFrame.from_dict(v) for k, v in banco.items()}, axis=0
        ).reset_index()
        colunas_relevantes = [
            "level_0",
            "level_1",
            "level_2",
            "DT_INTER",
            "MUNIC_RES",
            "DIAG_PRINC",
        ]
        todos = todos[colunas_relevantes]
        todos.rename(
            columns={
                "level_0": "estado",
                "level_1": "ano",
                "level_2": "mes",
                "MUNIC_RES": "municipio_ibge6",
            },
            inplace=True,
        )  # renomeando colunas

        internacoes = pd.DataFrame(columns=["municipio_ibge6"])
        for transtorno in cid10_transtornos:
            filter_list = cid10_transtornos[transtorno]
            cid10 = todos[todos["DIAG_PRINC"].isin(filter_list)]
            internacoes_por_municipio = (
                cid10.groupby("municipio_ibge6").size().reset_index()
            )
            internacoes_por_municipio.columns = ["municipio_ibge6", transtorno]
            internacoes = pd.merge(
                internacoes,
                internacoes_por_municipio,
                how="outer",
                on="municipio_ibge6",
            )

        internacoes["date"] = datetime.datetime(y, month, 1)
        internacoes.fillna(0, inplace=True)
        internacoes["total"] = (
            internacoes["t_esquizofrenicos"]
            + internacoes["t_psicoticos"]
            + internacoes["t_depressao"]
            + internacoes["t_ansiedade"]
            + internacoes["t_estresse"]
            + internacoes["t_comportamentais"]
            + internacoes["abusos_substancias"]
        )
        internacoes = internacoes[
            [
                "date",
                "municipio_ibge6",
                "t_esquizofrenicos",
                "t_psicoticos",
                "t_depressao",
                "t_ansiedade",
                "t_estresse",
                "t_comportamentais",
                "abusos_substancias",
                "total",
            ]
        ]
        internacoes.to_csv(
            f"parsed_data/sih/{y}{str(month).zfill(2)}.csv", index=False
        )
        banco = {}
