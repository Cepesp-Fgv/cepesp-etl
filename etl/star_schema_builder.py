import csv
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


def unique_dim(df: DataFrame, dim: Dim):
    df = df.groupby(dim.columns, as_index=False).first()
    df = df.sort_values(dim.match_column, ascending=True)
    return set_ids(df[dim.columns])


def create_dim_output(df: DataFrame, dim: Dim) -> DataFrame:
    filename = 'output/%s.csv.gz' % dim.name
    exists = os.path.exists(filename)
    if exists:
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

    df.to_csv(filename, compression='gzip', header=header, index=False, quoting=csv.QUOTE_NONNUMERIC, mode=mode)


def apply_dim(fact: DataFrame, dim_df: DataFrame, dim: Dim) -> DataFrame:
    print("Applying dim %s to fact" % dim.name)
    fact.set_index(dim.match_column, inplace=True)
    dim_df.set_index(dim.match_column, inplace=True)
    fact = fact.join(dim_df, lsuffix='_l', rsuffix='_r')
    fact = fact.reset_index(drop=True)
    fact = fact.rename(columns={'ID': dim.id_column})
    columns = [c for c in fact.columns if c != dim.id_column and not (str(c).endswith('_r') or str(c).endswith('_l'))]

    return fact[columns + [dim.id_column]]


def create_dim(df: DataFrame, dim: Dim) -> DataFrame:
    print("Creating dim %s" % dim.name)
    df = unique_dim(df, dim)

    return create_dim_output(df, dim)


def build_dimensions(source: DataFrame, schema: StarSchema):
    for dim in schema.dims:
        create_dim(source, dim)


def build_fact(source: DataFrame, schema: StarSchema):
    for dim in schema.dims:
        df = read_csv("output/%s.csv.gz" % dim.name, dtype=str, encoding='utf-8')
        source = apply_dim(source, df, dim)

    print("Creating fact")
    create_fact_output(source, schema.fact)
