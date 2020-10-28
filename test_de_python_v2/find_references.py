from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import pyarrow.csv as pv
import pyarrow.parquet as pq
from pathlib import Path
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType
import datetime

import logging


def find_references(spark: SparkSession, datadir: Path):

    datadrugs = spark.read.parquet(str(datadir / 'parquet-drug'))
    datadrugs.createOrReplaceTempView('drug')

    datapubmed = spark.read.parquet(str(datadir / 'parquet-pubmed'))
    datapubmed.createOrReplaceTempView('pubmed')

    cross = spark.sql(
        'select drug.atccode as drug_atccode, drug.drug as drug_name,pubmed.id as pubmed_id,pubmed.title as pubmed_title,pubmed.date from drug cross join pubmed')

    def filter_cross(row):
        return row.drug_name.upper() in row.pubmed_title.upper()

    cross = cross.rdd.filter(filter_cross).toLocalIterator()

    result = {}

    for row in cross:
        if result.get(row.drug_atccode) is None:
            result[row.drug_atccode] = {'name': row.drug_name, 'pubmeds': []}

        result[row.drug_atccode]['pubmeds'].append(row.pubmed_id)

    logging.info(result)
