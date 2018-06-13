from etl.cepesp.api import *
from etl.insert import insert
from etl.star_schema_builder import StarSchema, Dim, build_fact, build_dimensions, create_dim

info_ano_dim = Dim('dim_info_ano', CANDIDATOS, 'ID_DIM_INFO_ANO',
                   ['ANO_ELEICAO', 'CPF_CANDIDATO', 'NUM_TURNO', 'SIGLA_UE'])

candidato_dim = Dim('dim_candidato',
                    ['NOME_CANDIDATO', 'NOME_URNA_CANDIDATO', 'NUM_TITULO_ELEITORAL_CANDIDATO'],
                    'ID_DIM_CANDIDATO', ['NUM_TITULO_ELEITORAL_CANDIDATO'])

votacao_dim = Dim('dim_votacao', ['ANO_ELEICAO', 'CPF_CANDIDATO', 'NUM_TURNO', 'TOTAL_VOTACAO', 'SIGLA_UE'],
                  'ID_DIM_VOTACAO', ['ANO_ELEICAO', 'CPF_CANDIDATO', 'NUM_TURNO', 'SIGLA_UE'])

SCHEMA = StarSchema('fact_candidatos', [info_ano_dim, candidato_dim, votacao_dim])


def get_source(ano: int, cargo: int):
    df = votos_x_candidatos(ano, cargo, AGR_REGIONAL.BRASIL)
    df['NUM_TITULO_ELEITORAL_CANDIDATO'] = df['NUM_TITULO_ELEITORAL_CANDIDATO'].apply(lambda x: str(x).zfill(12))
    df.QTDE_VOTOS = pd.to_numeric(df['QTDE_VOTOS'], errors='coerce')

    columns = df.columns.values.tolist()
    columns.remove('DATA_GERACAO')
    columns.remove('HORA_GERACAO')
    columns.remove('QTDE_VOTOS')
    df = df.groupby(by=columns, as_index=False)['QTDE_VOTOS'].sum()

    return df.rename(columns={'QTDE_VOTOS': 'TOTAL_VOTACAO'})


def fix_candidates():
    df = pd.read_csv('output/dim_candidato.csv.gz', dtype=str)
    df = df.groupby(by='NUM_TITULO_ELEITORAL_CANDIDATO', as_index=False)
    df = df[['NOME_CANDIDATO', 'NOME_URNA_CANDIDATO']].max()
    create_dim(df, candidato_dim, overwrite=True)


def generate():
    jobs = [CARGO.PRESIDENTE,
            CARGO.GOVERNADOR,
            CARGO.SENADOR,
            CARGO.DEPUTADO_FEDERAL,
            CARGO.DEPUTADO_DISTRITAL,
            CARGO.DEPUTADO_ESTADUAL]

    for j in jobs:
        print("Job:", j)
        for a in get_elections(j):
            print("Year:", a)
            df = get_source(a, j)
            build_dimensions(df, SCHEMA)

    fix_candidates()

    for j in jobs:
        print("Job:", j)
        for a in get_elections(j):
            print("Year:", a)
            df = get_source(a, j)
            build_fact(df, SCHEMA)


def import_db():
    # db = MySQLdb.connect(user="root", passwd="123456", db="cepesp")
    # cur = db.cursor()
    for dim in SCHEMA.dims:
        print("Inserting dim %s..." % dim.name)
        insert(None, dim.name, ['ID'] + dim.columns)

        # db.close()


generate()
# import_db()
