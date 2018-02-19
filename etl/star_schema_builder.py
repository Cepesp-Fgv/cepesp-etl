import os

from pandas import DataFrame, read_csv


class Table:
    def __init__(self, name: str, columns: list = list()):
        self.name = name
        self.columns = columns


class Dim(Table):
    def __init__(self, name: str, columns: list, id_column: str, match_idx: int = 0):
        super().__init__(name, columns)
        self.id_column = id_column
        self.match_column = columns[match_idx]


class StarSchema:
    def __init__(self, fact: str, dims: list):
        self.fact = fact
        self.dims = dims


def set_ids(df: DataFrame) -> DataFrame:
    df.is_copy = False
    columns = [c for c in df.columns if c != 'ID']
    df['ID'] = range(1, len(df) + 1)

    return df[['ID'] + columns]


def create_dim_output(df: DataFrame, dim: Dim) -> DataFrame:
    filename = 'output/%s.csv.gz' % dim.name
    exists = os.path.exists(filename)
    if exists:
        original = read_csv(filename, dtype=str, encoding='utf-8')
        original.set_index(dim.match_column, inplace=True)

        df.set_index(dim.match_column, inplace=True)
        df.append(original, ignore_index=False)
        df.reset_index(inplace=True)

    df = set_ids(df)

    df.to_csv(filename, compression='gzip', header=True, index=False, encoding='utf-8')

    return df


def create_fact_output(df: DataFrame, name: str):
    filename = 'output/%s.csv.gz' % name
    exists = os.path.exists(filename)
    if exists:
        mode = 'w'
        header = False
    else:
        mode = 'a'
        header = True

    df.to_csv(filename, compression='gzip', header=header, index=False, mode=mode)


def apply_dim(fact: DataFrame, dim: DataFrame, id_key: str, match_column: str) -> DataFrame:
    print("Applying dim %s to fact" % id_key)
    for index, row in dim.iterrows():
        fact.loc[fact[match_column] == row[match_column], id_key] = row.ID

    fact[id_key] = fact[id_key].astype(int)

    return fact


def create_dim(source: DataFrame, dim: Dim) -> DataFrame:
    print("Creating dim %s" % dim.name)
    df = source.groupby(dim.columns, as_index=False).first()
    df = df.sort_values(dim.match_column, ascending=True)

    return create_dim_output(df[dim.columns], dim)


def create_fact(fact: DataFrame, name: str, dims_columns: list):
    print("Creating fact")
    fact_columns = [c for c in fact.columns if c not in dims_columns]

    create_fact_output(fact[fact_columns], name)


def build_schema(source, schema: StarSchema):
    created_dims = []
    for dim in schema.dims:
        df = create_dim(source, dim)
        created_dims.append(df)

    dims_columns = []
    i = 0
    for dim in schema.dims:
        apply_dim(source, created_dims[i], dim.id_column, dim.match_column)
        dims_columns += dim.columns
        i += 1

    create_fact(source, schema.fact, dims_columns)


def build_dimensions(source: DataFrame, schema: StarSchema):
    created_dims = []
    for dim in schema.dims:
        df = create_dim(source, dim)
        created_dims.append(df)

    return created_dims


def build_fact(source: DataFrame, schema: StarSchema):
    dims_columns = []
    created_dims = []
    for dim in schema.dims:
        df = read_csv("output/%s.csv.gz" % dim.name, dtype=str, encoding='utf-8')
        created_dims.append(df)
        dims_columns = dim.columns
        apply_dim(source, df, dim.id_column, dim.match_column)

    create_fact(source, schema.fact, dims_columns)
