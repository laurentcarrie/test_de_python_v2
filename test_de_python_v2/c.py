from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import pyarrow.csv as pv
import pyarrow.parquet as pq
from pathlib import Path
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType
import datetime


def rmdir_f(d: Path):
    if not d.exists():
        return
    if d.is_file():
        d.unlink()
    else:
        for f in d.iterdir():
            rmdir_f(f)
        d.rmdir()


out_dir = Path('tmp/pyarrow_out')
rmdir_f(out_dir)

out_dir.mkdir(parents=True, exist_ok=True)

in_file_drugs = Path('../enonce-du-probleme/drugs.csv')
in_file_pubmed = Path('../enonce-du-probleme/pubmed.csv')
out_file_drugs = out_dir / 'drugs.parquet'
out_file_pubmed = out_dir / 'pubmed.parquet'

conf = SparkConf().setAppName('toto').setMaster('local')
sc = SparkContext(conf=conf)


spark = SparkSession.builder.appName('SimpleApp').getOrCreate()

spark.read.option('header', True)\
    .csv(str(in_file_drugs))\
    .write.parquet(str(out_file_drugs))

spark.read.option('header', True).csv(
    str(in_file_pubmed)).write.parquet(str(out_file_pubmed))


datadrugs = spark.read.parquet(str(out_file_drugs))
print(datadrugs.count())
print(datadrugs.collect())
datadrugs.show()


datapubmed = spark.read.parquet(str(out_file_pubmed))
print(type(datapubmed))
print(datapubmed.count())
print(datapubmed.collect())
datapubmed.show()

schema = StructType([StructField('id', StringType(), True),
                     StructField('title', StringType(), True),
                     StructField('date', DateType(), True),
                     StructField('journal', StringType(), True),
                     ])


def pubmedmap(x):
    possible_formats = ['%d/%m/%Y', '%Y-%m-%d']
    date = None
    for format in possible_formats:
        try:
            date = datetime.datetime.strptime(x.date, format)
        except Exception:
            pass
    return x.id, x.title, date, x.journal


d2 = datapubmed.rdd.map(pubmedmap)

d2.toDF(schema).show()

datapubmed.printSchema()


spark.stop()
