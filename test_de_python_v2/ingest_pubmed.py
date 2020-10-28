from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType

from pathlib import Path
import datetime


def _correct_date(x):
    """
    we saw in the provided sql files that date could be provided in two different formats.
    this function handles the two formats, and also the conversion from string to date
    :param x:
    :return:
    """
    possible_formats = ['%d/%m/%Y', '%Y-%m-%d']
    date = None
    for format in possible_formats:
        try:
            date = datetime.datetime.strptime(x.date, format)
        except Exception:
            pass
    id = int(x.id)
    return id, x.title, date, x.journal


def ingest(spark: SparkSession, datadir: Path):
    """
    in this function should be the code to retrieve csv file from its location
    by ftp, amazon s3, rest api, ...

    for now we take it from the datadir directory and write to the parquet directory

    :param spark:
    :return:
    """

    input_file: Path = datadir / 'pubmed.csv'
    if not input_file.exists():
        raise Exception(f'input file does not exist {input_file}')
    output_path: Path = datadir / 'parquet-pubmed'

    schema = StructType([StructField('id', IntegerType(), True),
                         StructField('title', StringType(), True),
                         StructField('date', DateType(), True),
                         StructField('journal', StringType(), True),
                         ])

    spark.read.option('header', True)\
        .csv(str(input_file))\
        .rdd.map(_correct_date)\
        .toDF(schema)\
        .write.parquet(str(output_path))
