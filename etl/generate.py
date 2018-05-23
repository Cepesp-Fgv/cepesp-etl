from etl.cepesp.api import *
from etl.star_schema_builder import StarSchema, Dim, build_fact, build_dimensions, create_dim, create_dim_output

SCHEMA = StarSchema(
    'fact_candidatos',
    [
        Dim('dim_info_ano',
            CANDIDATOS,
            'ID_DIM_INFO_ANO',
            ['ANO_ELEICAO', 'CPF_CANDIDATO', 'NUM_TURNO']),

        Dim('dim_candidato',
            ['NOME_CANDIDATO', 'NOME_URNA_CANDIDATO', 'NUM_TITULO_ELEITORAL_CANDIDATO'],
            'ID_DIM_CANDIDATO',
            'NUM_TITULO_ELEITORAL_CANDIDATO'),

        Dim('dim_votacao',
            ['ANO_ELEICAO', 'CPF_CANDIDATO', 'NUM_TURNO', 'TOTAL_VOTACAO'],
            'ID_DIM_VOTACAO',
            ['ANO_ELEICAO', 'CPF_CANDIDATO', 'NUM_TURNO']),
    ]
)


def get_source(ano: int, cargo: int):
    df = votos_x_candidatos(ano, cargo, AGR_REGIONAL.BRASIL)
    df.QTDE_VOTOS = pd.to_numeric(df['QTDE_VOTOS'], errors='coerce')

    columns = df.columns.values.tolist()
    columns.remove('DATA_GERACAO')
    columns.remove('HORA_GERACAO')
    columns.remove('QTDE_VOTOS')
    df = df.groupby(by=columns, as_index=False)['QTDE_VOTOS'].sum()

    return df.rename(columns={'QTDE_VOTOS': 'TOTAL_VOTACAO'})


def fix_candidates():
    dim = SCHEMA.dims[1]
    df = pd.read_csv('output/dim_candidato.csv.gz')
    df = df.groupby(by='NUM_TITULO_ELEITORAL_CANDIDATO', as_index=False)
    df = df[['NOME_CANDIDATO', 'NOME_URNA_CANDIDATO']].max()
    df['NUM_TITULO_ELEITORAL_CANDIDATO'] = df['NUM_TITULO_ELEITORAL_CANDIDATO'].apply(lambda x: x.zfill(12))
    create_dim(df, dim, overwrite=True)


def fix_votos():
    dim = SCHEMA.dims[2]
    df = pd.read_csv('output/dim_votacao.csv.gz')
    df = df[['ID', 'TOTAL_VOTACAO']]
    create_dim_output(df, dim, overwrite=True)


def generate():
    for j in [CARGO.PRESIDENTE, CARGO.GOVERNADOR]:
        for a in [2014, 2010, 2006, 2002, 1998]:
            df = get_source(a, j)
            build_dimensions(df, SCHEMA)

    fix_candidates()

    for j in [CARGO.PRESIDENTE, CARGO.GOVERNADOR]:
        for a in [2014, 2010, 2006, 2002, 1998]:
            df = get_source(a, j)
            build_fact(df, SCHEMA)

    fix_votos()


generate()
