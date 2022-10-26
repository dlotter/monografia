from pysus.online_data.SIM import download
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

banco = {}
for y in anos:
    for uf in estados:
        banco[uf, y] = download(state=uf, year=y)
        print("Banco de " + str(y) + " de " + str(uf) + " baixado!")


todos = pd.concat(
    {k: pd.DataFrame.from_dict(v) for k, v in banco.items()}, axis=0
).reset_index()
todos = todos[
    [
        "level_0",
        "level_1",
        "CIRCOBITO",
        "DTOBITO",
        "DTNASC",
        "SEXO",
        "RACACOR",
        "ESTCIV",
        "ESC",
        "OCUP",
        "CODMUNRES",
        "LOCOCOR",
        "ASSISTMED",
        "CAUSABAS",
        "CAUSABAS_O",
    ]
]
todos.rename(
    columns={"level_0": "estado", "level_1": "ano"}, inplace=True
)  # renomeando colunas

filter_list = ["X{}".format(x) for x in range(600, 850)]
cid10 = todos[
    todos["CAUSABAS"].isin(filter_list) | todos["CAUSABAS_O"].isin(filter_list)
]

dictCIRCOBITO = {
    "1": "Acidente",
    "2": "Suicídio",
    "3": "Homicídio",
    "4": "Outro",
    "0": "NA",
    "6": "NA",
    "7": "NA",
    "8": "NA",
    "9": "NA",
}
dicSEXO = {"1": "Masculino", "2": "Feminino", "0": "NA", "9": "NA"}
dicRACACOR = {
    "1": "Branca",
    "2": "Preta",
    "3": "Amarela",
    "4": "Parda",
    "5": "Indígena",
    "0": "NA",
    "6": "NA",
    "7": "NA",
    "8": "NA",
    "9": "NA",
}
dicESTCIV = {
    "1": "Solteiro",
    "2": "Casado",
    "3": "Viúvo",
    "4": "Separado judicialmente",
    "5": "União consensual",
    "0": "NA",
    "6": "NA",
    "7": "NA",
    "8": "NA",
    "9": "NA",
}
dicESC = {
    "1": "Nenhuma",
    "2": "1 a 3 anos",
    "3": "4 a 7 anos",
    "4": "8 a 11 anos",
    "5": "12 e mais",
    "8": "De 9 a 11 anos",
    "0": "NA",
    "6": "NA",
    "7": "NA",
    "9": "NA",
    "": "NA",
}
dictLOCOCOR = {
    "1": "Hospital",
    "2": "Outro estabelecimento de saúde",
    "3": "Domicílio",
    "4": "Via pública",
    "5": "Outros",
    "9": "NA",
}
dictASSISTMED = {"1": "Sim", "2": "Não", "9": "NA"}
cid10 = cid10.replace(
    {
        "CIRCOBITO": dictCIRCOBITO,
        "SEXO": dicSEXO,
        "RACACOR": dicRACACOR,
        "ESTCIV": dicESTCIV,
        "ESC": dicESC,
        "LOCOCOR": dictLOCOCOR,
        "ASSISTMED": dictASSISTMED,
    }
)
cid10["DTNASC"] = cid10["DTNASC"][cid10["DTNASC"] != ""].apply(
    lambda x: datetime.datetime.strptime(x, "%d%m%Y")
)
cid10["DTOBITO"] = pd.to_datetime(cid10["DTOBITO"], format="%d%m%Y")

cid10["idade"] = cid10["DTOBITO"].dt.year - cid10["DTNASC"].dt.year
cid10["mes"] = cid10["DTOBITO"].map(lambda x: x.strftime("%m"))

CBO2002 = pd.read_csv("extractors/CBO.csv").set_index("CODIGO").to_dict()
cid10["OCUP"] = cid10["OCUP"].replace("", 0).astype(int)
cid10["OCUP"] = cid10["OCUP"].replace(CBO2002["OCUPACAO"])

URL = "http://blog.mds.gov.br/redesuas/wp-content/uploads/2018/06/Lista_Munic%C3%ADpios_com_IBGE_Brasil_Versao_CSV.csv"
municipios = (
    pd.read_csv(
        URL,
        error_bad_lines=False,
        sep=";",
        encoding="latin-1",
        usecols=["IBGE", "IBGE7"],
    )
    .set_index("IBGE")
    .to_dict()
)
cid10["CODMUNRES"] = (
    cid10["CODMUNRES"].astype(int).replace(municipios["IBGE7"])
)

todos["CODMUNRES"] = (
    todos["CODMUNRES"].astype(int).replace(municipios["IBGE7"])
)

todos["DTOBITO"] = pd.to_datetime(todos["DTOBITO"], format="%d%m%Y")

cid10.to_csv(
    "/home/diego/Desktop/monografia/raw_data/suicidios/suicidios.csv",
    index=False,
)
todos.to_csv(
    "/home/diego/Desktop/monografia/raw_data/suicidios/todos.csv", index=False
)
