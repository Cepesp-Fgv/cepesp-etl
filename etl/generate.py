import pandas as pd

from etl.star_schema_builder import StarSchema, Dim, build_dimensions, build_fact

SCHEMA = StarSchema(
    'candidatos_fact',
    [
        Dim('cargo_dim',
            ['CODIGO_CARGO', 'DESCRICAO_CARGO'],
            'DIM_CARGO_ID'),

        Dim('partido_dim',
            ['NUMERO_PARTIDO', 'SIGLA_PARTIDO', 'NOME_PARTIDO'],
            'DIM_PARTIDO_ID'),

        Dim('nome_urna_dim',
            ['NOME_URNA_CANDIDATO'],
            'DIM_NOME_URNA_ID'),

        Dim('sit_tot_turno_dim',
            ['COD_SIT_TOT_TURNO', 'DESC_SIT_TOT_TURNO'],
            'DIM_SIT_TOT_TURNO_ID', 1),

        Dim('info_candidato_dim',
            ['NOME_CANDIDATO', 'CPF_CANDIDATO', 'NUM_TITULO_ELEITORAL_CANDIDATO'],
            'DIM_INFO_CANDIDATO_ID'),

    ]
)


def read_df(year) -> pd.DataFrame:
    return pd.read_csv("source/candidato_%d.csv.gz" % year, sep=';', dtype=str, encoding='utf-8')


def create_dims(years):
    for y in years:
        print("Building dimensions for year %d" % y)
        df = read_df(y)
        build_dimensions(df, SCHEMA)


def create_fact(years):
    for y in years:
        print("Building fact for year %d" % y)
        i = 1
        for chunk in pd.read_csv("source/candidato_%d.csv.gz" % y, sep=';', dtype=str, encoding='utf-8',
                                 chunksize=10 ** 6):
            print("Chunk: %d" % i)
            build_fact(chunk, SCHEMA)
            i += 1


create_dims(range(1998, 2018, 2))
create_fact(range(1998, 2018, 4))
create_fact(range(2000, 2018, 4))
