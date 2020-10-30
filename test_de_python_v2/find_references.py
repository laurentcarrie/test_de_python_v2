from pyspark.sql import SparkSession
from pathlib import Path
import logging
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType
from test_de_python_v2 import labels


def find_references_drug_pubmed(spark: SparkSession, datadir: Path):

    dfdrugs = spark.read.parquet(str(datadir / labels.parquet_drug))
    dfdrugs.createOrReplaceTempView('drug')

    dfpubmed = spark.read.parquet(str(datadir / labels.parquet_pubmed))
    dfpubmed.createOrReplaceTempView('pubmed')

    cross = spark.sql(
        'select drug.atccode as drug_atccode, drug.drug as drug_name,pubmed.id as pubmed_id,pubmed.title \
        as pubmed_title,pubmed.date as pubmed_date from drug cross join pubmed')

    def filter_cross(row):
        return row.drug_name.upper() in row.pubmed_title.upper()

    cross = cross.rdd.filter(filter_cross)

    schema = StructType([StructField('drug_atccode', StringType(), False),
                         StructField('drug_name', StringType(), True),
                         StructField('pubmed_id', IntegerType(), True),
                         StructField('pubmed_title', StringType(), True),
                         StructField('pubmed_date', DateType(), True),
                         ])

    cross.toDF(schema).write.parquet(
        path=str(datadir / labels.parquet_drug_pubmed), mode='overwrite')


def find_references_drug_clinical_trial(spark: SparkSession, datadir: Path):

    dfdrugs = spark.read.parquet(str(datadir / labels.parquet_drug))
    dfdrugs.createOrReplaceTempView('drug')

    dfclinicaltrial = spark.read.parquet(str(datadir / labels.parquet_clinical_trial))
    dfclinicaltrial.createOrReplaceTempView('clinical_trial')

    cross = spark.sql(
        'select drug.atccode as drug_atccode, drug.drug as drug_name,clinical_trial.id as clinical_trial_id, \
        clinical_trial.scientific_title as clinical_trial_scientific_title,\
        clinical_trial.date as clinical_trial_date from drug cross join clinical_trial')

    cross = cross.rdd.filter(lambda item: item.drug_name.upper()
                             in item.clinical_trial_scientific_title.upper())

    schema = StructType([StructField('drug_atccode', StringType(), False),
                         StructField('drug_name', StringType(), True),
                         StructField('clinical_trial_id', StringType(), True),
                         StructField('clinical_trial_scientific_title',
                                     StringType(), True),
                         StructField('clinical_trial_date', DateType(), True),
                         ])

    cross.toDF(schema).write.parquet(
        path=str(datadir / labels.parquet_drug_clinical_trial), mode='overwrite')


def find_references_drug_journal(spark: SparkSession, datadir: Path):
    dfdrugs = spark.read.parquet(str(datadir / labels.parquet_drug))
    dfdrugs.createOrReplaceTempView('drug')

    def get_journal_df(parquet_name, title_label):
        dfother = spark.read.parquet(str(datadir / parquet_name))
        dfother.createOrReplaceTempView('other')

        cross = spark.sql(
            f'select drug.atccode as drug_atccode, drug.drug as drug_name, \
            other.{title_label} as title, other.date as date, other.journal as journal \
            from drug cross join other \
            ')

        cross = cross.rdd.filter(
            lambda item: item.drug_name.upper() in item.title.upper())

        return cross.toDF().select('drug_atccode', 'journal', 'date').distinct()

    df1 = get_journal_df(labels.parquet_pubmed, 'title')

    df2 = get_journal_df(labels.parquet_clinical_trial, 'scientific_title')

    df = df1.union(df2).distinct()
    df.write.parquet(
        path=str(datadir / labels.parquet_drug_journal), mode='overwrite')
