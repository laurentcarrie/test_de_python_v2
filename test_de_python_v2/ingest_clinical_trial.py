from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType

from pathlib import Path
import datetime
import logging

from test_de_python_v2 import labels


def _correct_date(item):
    """
    we saw in the provided csv files that date could be provided in different formats.
    this function handles the two formats, and also the conversion from string to date
    :param item:
    :return:
    """
    possible_formats = ['%d/%m/%Y', '%Y-%m-%d', '%d %B %Y']
    date = None
    for format in possible_formats:
        try:
            date = datetime.datetime.strptime(item.date, format)
            break
        except Exception:
            pass
    else:
        logging.warning(f"could not convert date '{item.date}', replace it by None")

    return item.id, item.scientific_title, date, item.journal


def ingest(spark: SparkSession, datadir: Path):
    """
    in this function should be the code to retrieve csv file from its location
    by ftp, amazon s3, rest api, ...

    for now we take it from the datadir directory and write to the parquet directory

    :param spark:
    :return:
    """

    input_file: Path = datadir / 'clinical_trials.csv'
    if not input_file.exists():
        raise Exception(f'input file does not exist {input_file}')
    output_path: Path = datadir / labels.parquet_clinical_trial

    schema = StructType([StructField('id', StringType(), True),
                         StructField('scientific_title', StringType(), True),
                         StructField('date', DateType(), True),
                         StructField('journal', StringType(), True),
                         ])

    spark.read.option('header', True)\
        .csv(str(input_file))\
        .rdd.map(_correct_date)\
        .toDF(schema)\
        .write.parquet(str(output_path))
