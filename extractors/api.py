import sys
import requests
import pandas as pd
import logging
import time
import os
from datetime import datetime

logging.basicConfig(format="%(asctime)s %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

api_key_name = sys.argv[1]
api_key = os.environ[api_key_name]


def unique_counties_from_xls(xls_path):
    counties = pd.read_csv(xls_path)
    unique_counties = counties["cod_ibge"].unique()
    return list(unique_counties)


def get_api_data(beneficio: str, municipios, mes_ano, chave_api):
    url = base_url + f"/api-de-dados/{beneficio}-por-municipio"

    headers = {"chave-api-dados": chave_api}

    pagina = 1

    data = []

    destino = f"raw_data/{beneficio.replace('-', '_')}/api/{mes_ano}.csv"

    if os.path.exists(destino):
        destino_df = pd.read_csv(destino)
        municipios_faltantes = (
            set(municipios) - set(destino_df["municipio"]) - {0}
        )
        data = list(destino_df.T.to_dict().values())

    else:
        municipios_faltantes = municipios

    if len(municipios_faltantes) == 0:
        logging.info("Dados de %s já extraídos. Pulando.", mes_ano)

    for municipio in municipios_faltantes:
        start_time = datetime.now()
        params = {"pagina": pagina, "codigoIbge": municipio, "mesAno": mes_ano}

        logging.info("Município: %s. Data: %s", municipio, mes_ano)

        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            logger.error("Erro: status code != 200")
            break

        response_json = response.json()

        logger.info("Response status: %s", response.status_code)
        if not response_json:
            time.sleep(0.6)
            continue
        else:
            response_json[0]["municipio"] = response_json[0]["municipio"][
                "codigoIBGE"
            ]
            data += response_json
            df = pd.DataFrame(data)
            df.to_csv(destino, index=False)
            time.sleep(0.6)
            end_time = datetime.now()
            req_min = 60 / (end_time - start_time).total_seconds()
            logger.info("Req/min: %s", req_min)


unique_counties = unique_counties_from_xls(
    "raw_data/codigo_municipios/TABMUN.csv"
)

base_url = "https://api.portaldatransparencia.gov.br"

# dates_auxilio_1 = [
#     "202004",
#     "202005",
#     "202006",
#     "202007",
#     "202008",
#     "202009",
#     "202010",
#     "202011",
#     "202012",
# ]

# dates_bolsa_1 = [
#     "201908",
#     "201909",
#     "201910",
#     "201911",
#     "201912",
#     "202001",
#     "202002",
#     "202003",
#     "202004",
#     "202005",
#     "202006",
#     "202007",
#     "202008",
#     "202009",
#     "202010",
#     "202011",
#     "202012",
# ]

dates_bolsa_1 = [
    "202005",
    "202007",
    "202009",
    "202011",
]

dates_bolsa_2 = [
    "202006",
    "202008",
    "202010",
    "202012",
]


if api_key_name == "CHAVE_1":
    datas = dates_bolsa_1
    beneficio = "bolsa-familia"
if api_key_name == "CHAVE_2":
    datas = dates_bolsa_2
    beneficio = "bolsa-familia"


for date in datas:
    get_api_data(
        beneficio,
        unique_counties,
        date,
        api_key,
    )
