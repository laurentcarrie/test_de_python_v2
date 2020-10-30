from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import pyarrow.csv as pv
import pyarrow.parquet as pq
from pathlib import Path
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType
import datetime
import json
import test_de_python_v2.labels as labels

import logging


def write_json(spark: SparkSession, datadir: Path):
    result = {}

    def init_json_item(row):
        result[row.drug_atccode] = {'name': row.drug_name,
                                    labels.json_pubmed_array_name: [], labels.json_clinical_trials_array_name: []}

    def iterate_on_references(parquet_path, array_name, id_getter):
        iter = spark.read.parquet(str(parquet_path)).toLocalIterator()
        count = 0
        max_allowed = 10000
        for item in iter:
            count = count + 1
            if count > max_allowed:
                raise Exception('too much data... bailing out')
            if result.get(item.drug_atccode) is None:
                init_json_item(item)

            result[item.drug_atccode][array_name].append(id_getter(item))

    iterate_on_references(parquet_path=datadir / 'parquet-drug-pubmed',
                          array_name=labels.json_pubmed_array_name,
                          id_getter=lambda item: item.pubmed_id)

    iterate_on_references(parquet_path=datadir / 'parquet-drug-clinical_trial',
                          array_name=labels.json_clinical_trials_array_name,
                          id_getter=lambda item: item.clinical_trial_id)

    with open(str(datadir / labels.json_output_filename), 'w') as fp:
        json.dump(result, fp)
