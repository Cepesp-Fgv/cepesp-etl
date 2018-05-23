import urllib.request as urllib

import pandas as pd

from etl.cepesp.columns import VOTOS, CANDIDATOS, LEGENDAS

BASE_URL = "http://cepesp.io/api/consulta/"


def build_filters(filters: dict) -> str:
    q = ""

    if 'UF' in filters.keys():
        uf = filters.pop('UF')
        q += "&uf_filter=%s".format(uf)

    i = 0
    for k, v in filters.items():
        q += "&columns[{}][name]={}&columns[{}][search][value]={}".format(i, k, i, v)
        i += 1

    return q


def build_columns(columns: list) -> str:
    q = ""
    for column in columns:
        q += "&selected_columns[]={}".format(column)

    return q


def query(request: str, filters: dict, columns: list) -> pd.DataFrame:
    if filters is not None:
        request += build_filters(filters)

    if columns is not None:
        request += build_columns(columns)

    response = urllib.urlopen(BASE_URL + request)
    return pd.read_csv(response, sep=",", dtype=str)


def votos(ano: int, cargo: int, agregacao_regional: int, estado: str = None, filtros: dict = None,
          colunas: list = None):
    request = "votos?cargo={}&ano={}&agregacao_regional={}&format=csv".format(cargo, ano, agregacao_regional)
    if filtros is None:
        filtros = dict()

    if estado is not None:
        filtros['UF'] = estado

    return query(request, filtros, colunas)


def candidatos(ano: int, cargo: int, filtros: dict = None, colunas: list = None):
    request = "candidatos?cargo={}&ano={}&format=csv".format(cargo, ano)
    return query(request, filtros, colunas)


def legendas(ano: int, cargo: int, filtros: dict = None, colunas: list = None):
    request = "legendas?cargo={}&ano={}&format=csv".format(cargo, ano)
    return query(request, filtros, colunas)


def resolve_conflicts(df: pd.DataFrame, prefer='_x', drop='_y') -> pd.DataFrame:
    columns = df.columns.values.tolist()
    conflicts = [c for c in columns if c.endswith(prefer)]
    drops = [c for c in columns if c.endswith(drop)]
    renames = dict()
    for c in conflicts:
        renames[c] = c.replace(prefer, '')

    return df.rename(columns=renames).drop(drops, axis=1)


def votos_x_candidatos(ano: int, cargo: int, agregacao_regional: int, estado=None):
    vot = votos(ano, cargo, agregacao_regional, estado, colunas=VOTOS[agregacao_regional]) \
        .set_index(["NUMERO_CANDIDATO", "SIGLA_UE", "NUM_TURNO", "ANO_ELEICAO"])

    cand = candidatos(ano, cargo, colunas=CANDIDATOS) \
        .set_index(["NUMERO_CANDIDATO", "SIGLA_UE", "NUM_TURNO", "ANO_ELEICAO"])

    merged = vot.merge(cand, how="left", left_index=True, right_index=True).reset_index()

    return resolve_conflicts(merged)


def votos_x_legendas(ano: int, cargo: int, agregacao_regional: int, estado: str = None):
    vot = votos(cargo, ano, agregacao_regional, estado, colunas=VOTOS[agregacao_regional]) \
        .set_index(["NUMERO_CANDIDATO", "SIGLA_UE", "NUM_TURNO", "ANO_ELEICAO"])

    leg = legendas(cargo, ano, colunas=LEGENDAS) \
        .rename(columns={"NUMERO_PARTIDO": "NUMERO_CANDIDATO"}) \
        .set_index(["NUMERO_CANDIDATO", "SIGLA_UE", "NUM_TURNO", "ANO_ELEICAO"])

    merged = vot.merge(leg, how="left", left_index=True, right_index=True).reset_index()

    return resolve_conflicts(merged)


def candidato_x_legendas(ano: int, cargo: int):
    leg = legendas(ano, cargo, colunas=LEGENDAS) \
        .set_index(["NUMERO_PARTIDO", "SIGLA_UE", "ANO_ELEICAO"])

    cand = candidatos(ano, cargo, colunas=CANDIDATOS) \
        .set_index(["NUMERO_PARTIDO", "SIGLA_UE", "ANO_ELEICAO"])

    merged = cand.merge(leg, how="left", left_index=True, right_index=True).reset_index()

    return resolve_conflicts(merged)


class CARGO:
    PRESIDENTE = 1
    SENADOR = 5
    GOVERNADOR = 3
    VEREADOR = 13
    PREFEITO = 11
    DEPUTADO_FEDERAL = 6
    DEPUTADO_ESTADUAL = 7
    DEPUTADO_DISTRITAL = 8


class AGR_REGIONAL:
    BRASIL = 0
    UF = 2
    MUNICIPIO = 6
    MUNZONA = 7
    ZONA = 8
    MACRO = 1
    MESO = 4
    MICRO = 5


class AGR_POLITICA:
    CANDIDATO = 1
    PARTIDO = 2
    COLIGACAO = 3
