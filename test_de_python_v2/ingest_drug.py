from pyspark.sql import SparkSession
from pathlib import Path
from test_de_python_v2 import labels


def ingest(spark: SparkSession, csvfile: Path, parquetdir: Path):
    """
    in this function should be the code to retrieve csv file from its location
    by ftp, amazon s3, rest api, ...

    for now we take it from the datadir directory and write to the parquet directory

    :param spark:
    :return:
    """

    if not csvfile.exists():
        raise Exception(f'input file does not exist {csvfile}')
    output_path: Path = parquetdir / labels.parquet_drug

    spark.read.option('header', True)\
        .csv(str(csvfile))\
        .write.parquet(path=str(output_path), mode='append')
