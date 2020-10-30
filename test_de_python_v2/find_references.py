from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import pyarrow.csv as pv
import pyarrow.parquet as pq
from pathlib import Path
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType
import datetime

import logging

from test_de_python_v2 import labels


def find_references_drug_pubmed(spark: SparkSession, datadir: Path):

    datadrugs = spark.read.parquet(str(datadir / labels.parquet_drug))
    datadrugs.createOrReplaceTempView('drug')

    datapubmed = spark.read.parquet(str(datadir / labels.parquet_pubmed))
    datapubmed.createOrReplaceTempView('pubmed')

    cross = spark.sql(
        'select drug.atccode as drug_atccode, drug.drug as drug_name,pubmed.id as pubmed_id,pubmed.title as pubmed_title,pubmed.date as pubmed_date from drug cross join pubmed')

    def filter_cross(row):
        return row.drug_name.upper() in row.pubmed_title.upper()

    cross = cross.rdd.filter(filter_cross)

    schema = StructType([StructField('drug_atccode', StringType(), False),
                         StructField('drug_name', StringType(), True),
                         StructField('pubmed_id', IntegerType(), True),
                         StructField('pubmed_title', StringType(), True),
                         StructField('pubmed_date', DateType(), True),
                         ])

    cross.toDF(schema).write.parquet(str(datadir / labels.parquet_drug_pubmed))


def find_references_drug_clinical_trial(spark: SparkSession, datadir: Path):

    datadrugs = spark.read.parquet(str(datadir / labels.parquet_drug))
    datadrugs.createOrReplaceTempView('drug')

    dataclinicaltrial = spark.read.parquet(str(datadir / labels.parquet_clinical_trial))
    dataclinicaltrial.createOrReplaceTempView('clinical_trial')

    cross = spark.sql(
        'select drug.atccode as drug_atccode, drug.drug as drug_name,clinical_trial.id as clinical_trial_id, \
        clinical_trial.scientific_title as clinical_trial_scientific_title,\
        clinical_trial.date as clinical_trial_date from drug cross join clinical_trial')

    def filter_cross(row):
        return row.drug_name.upper() in row.clinical_trial_scientific_title.upper()

    cross = cross.rdd.filter(filter_cross)

    schema = StructType([StructField('drug_atccode', StringType(), False),
                         StructField('drug_name', StringType(), True),
                         StructField('clinical_trial_id', StringType(), True),
                         StructField('clinical_trial_scientific_title',
                                     StringType(), True),
                         StructField('clinical_trial_date', DateType(), True),
                         ])

    cross.toDF(schema).write.parquet(str(datadir / labels.parquet_drug_clinical_trial))
