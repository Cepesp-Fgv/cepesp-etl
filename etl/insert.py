import gzip
import os
import shutil


def gunzip(file: str):
    file = os.path.abspath(file).replace("\\", "/")

    with gzip.open(file, 'rb') as f_in:
        file = file.replace(".gz", "")
        with open(file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    return file


def insert(con, table: str, columns: list, sep=',', enclosed='"', terminated='\\r\\n'):
    file = gunzip('output/%s.csv.gz' % table)
    columns = ", ".join(columns)

    truncate = "DELETE FROM {table};".format(table=table)
    query = "LOAD DATA LOCAL INFILE '{file}' " \
            "IGNORE INTO TABLE {table} FIELDS TERMINATED BY '{sep}' " \
            "OPTIONALLY ENCLOSED BY '{enclosed}' " \
            "LINES TERMINATED BY '{terminated}' " \
            "IGNORE 1 LINES ({columns});"\
        .format(file=file, table=table, sep=sep, enclosed=enclosed, terminated=terminated, columns=columns)

    print(query)

    #con.execute(truncate)
    #con.execute(query)
