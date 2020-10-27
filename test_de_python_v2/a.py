from pyspark.sql import SparkSession
import pyarrow.csv as pv
import pyarrow.parquet as pq
from pathlib import Path

out_dir = Path('tmp/pyarrow_out')
out_dir.mkdir(parents=True, exist_ok=True)
in_file = Path('../enonce-du-probleme/clinical_trials.csv')
out_file = out_dir / 'clinical_trials.parquet'

table = pv.read_csv(str(in_file))
pq.write_table(table, str(out_file))


spark = SparkSession.builder.appName('SimpleApp').getOrCreate()
logData = spark.read.text(str(in_file)).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print('Lines with a: %i, lines with b: %i' % (numAs, numBs))

print(logData.count())
print(logData.collect())

data = spark.read.parquet(str(out_file))
print(data.count())
print(data.collect())
data.show()

spark.stop()
