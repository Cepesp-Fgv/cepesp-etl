import csv
import os
from pandas import DataFrame, read_csv, to_numeric


class Table:
    def __init__(self, name: str, columns: list = list()):
        self.name = name
        self.columns = columns


class Dim(Table):
    def __init__(self, name: str, columns: list, id_column: str, key_columns=None):
        super().__init__(name, columns)
        self.id_column = id_column
        if key_columns is not None:
            if isinstance(key_columns, list):
                self.match_columns = key_columns
            else:
                self.match_columns = [key_columns]
        else:
            self.match_columns = columns[0]


class StarSchema:
    def __init__(self, fact: str, dims: list):
        self.fact = fact
        self.dims = dims
        self.fact_columns = []

        for d in self.dims:
            self.fact_columns.append(d.id_column)


def set_ids(df: DataFrame) -> DataFrame:
    df.is_copy = False
    columns = [c for c in df.columns if c != 'ID']
    df['ID'] = range(1, len(df) + 1)

    return df[['ID'] + columns]


def unique_dim(df: DataFrame, dim: Dim):
    df = df.groupby(dim.columns, as_index=False).first()
    df = df.sort_values(dim.match_columns, ascending=True)
    return set_ids(df[dim.columns])


def create_dim_output(df: DataFrame, dim: Dim, overwrite=False) -> DataFrame:
    filename = 'output/%s.csv.gz' % dim.name
    exists = os.path.exists(filename)
    if exists and not overwrite:
        original = read_csv(filename, dtype=str, encoding='utf-8')
        df = df.append(original, ignore_index=True)
        df = unique_dim(df, dim)

    df.to_csv(filename, compression='gzip', header=True, index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8')

    return df


def create_fact_output(df: DataFrame, name: str):
    filename = 'output/%s.csv.gz' % name
    exists = os.path.exists(filename)
    if exists:
        mode = 'a'
        header = False
    else:
        mode = 'w'
        header = True

    df.to_csv(filename, compression='gzip', encoding='utf-8', header=header, index=False, quoting=csv.QUOTE_NONNUMERIC, mode=mode)


def resolve_conflicts(df: DataFrame, prefer='_x', drop='_y') -> DataFrame:
    columns = df.columns.values.tolist()
    conflicts = [c for c in columns if c.endswith(prefer)]
    drops = [c for c in columns if c.endswith(drop)]
    renames = dict()
    for c in conflicts:
        renames[c] = c.replace(prefer, '')

    return df.rename(columns=renames).drop(drops, axis=1)


def apply_dim(fact: DataFrame, dim_df: DataFrame, dim: Dim) -> DataFrame:
    print("Applying dim %s to fact" % dim.name)
    fact = fact.merge(dim_df, on=dim.match_columns)
    fact = fact.rename(columns={'ID': dim.id_column})

    return resolve_conflicts(fact)


def create_dim(df: DataFrame, dim: Dim, overwrite=False) -> DataFrame:
    print("Creating dim %s" % dim.name)
    df = unique_dim(df, dim)

    return create_dim_output(df, dim, overwrite)


def build_dimensions(source: DataFrame, schema: StarSchema):
    for dim in schema.dims:
        create_dim(source, dim)


def build_fact(source: DataFrame, schema: StarSchema):
    for dim in schema.dims:
        dim_path = "output/%s.csv.gz" % dim.name
        df = read_csv(dim_path, dtype=str, encoding='utf-8')
        source = apply_dim(source, df, dim)

    print("Creating fact")
    create_fact_output(source[schema.fact_columns].apply(to_numeric, errors='ignore'), schema.fact)
