from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import pyarrow.csv as pv
import pyarrow.parquet as pq
from pathlib import Path
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType
import datetime
import json

import logging


def write_json(spark: SparkSession, datadir: Path):
    iter = spark.read.parquet(str(datadir / 'parquet-drug-pubmed')).toLocalIterator()

    result = {}

    count = 0
    max_allowed = 10000
    for row in iter:
        count = count + 1
        if count > max_allowed:
            raise Exception('too much data... bailing out')
        if result.get(row.drug_atccode) is None:
            result[row.drug_atccode] = {'name': row.drug_name, 'pubmeds': []}

        result[row.drug_atccode]['pubmeds'].append(row.pubmed_id)

    with open(str(datadir / 'result.json'), 'w') as fp:
        json.dump(result, fp)
