from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType

from pathlib import Path
import datetime
import logging

from test_de_python_v2 import labels


def _correct_date(item):
    """
    we saw in the provided sql files that date could be provided in different formats.
    this function handles the two formats, and also the conversion from string to date
    :param item:
    :return:
    """
    possible_formats = ['%d/%m/%Y', '%Y-%m-%d']
    date = None
    for format in possible_formats:
        try:
            date = datetime.datetime.strptime(item.date, format)
            break
        except ValueError:
            pass
    else:
        logging.warning(f"could not convert date '{item.date}', replace it by None")

    try:
        id = int(item.id)
    except ValueError:
        raise Exception(f'found a non integer id {item.id}')

    return id, item.title, date, item.journal


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
    output_path: Path = parquetdir / labels.parquet_pubmed

    schema = StructType([StructField('id', IntegerType(), True),
                         StructField('title', StringType(), True),
                         StructField('date', DateType(), True),
                         StructField('journal', StringType(), True),
                         ])

    spark.read.option('header', True)\
        .csv(str(csvfile))\
        .rdd.map(_correct_date)\
        .toDF(schema)\
        .write.parquet(path=str(output_path), mode='append')
