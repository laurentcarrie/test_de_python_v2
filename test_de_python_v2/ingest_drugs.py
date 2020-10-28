from pyspark.sql import SparkSession
from pathlib import Path


def ingest(spark: SparkSession, datadir: Path):
    """
    in this function should be the code to retrieve csv file from its location
    by ftp, amazon s3, rest api, ...

    for now we take it from the datadir directory and write to the parquet directory

    :param spark:
    :return:
    """

    input_file: Path = datadir / 'drugs.csv'
    if not input_file.exists():
        raise Exception(f'input file does not exist {input_file}')
    output_path: Path = datadir / 'parquet'

    spark.read.option('header', True)\
        .csv(str(input_file))\
        .write.parquet(str(output_path))