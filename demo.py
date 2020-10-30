from pyspark.sql import SparkSession
from pathlib import Path
import json
import datetime
from test_de_python_v2.ingest_drug import ingest as ingest_drug
from test_de_python_v2.ingest_pubmed import ingest as ingest_pubmed
from test_de_python_v2.ingest_clinical_trial import ingest as ingest_clinical_trial
from test_de_python_v2.find_references import find_references_drug_pubmed, find_references_drug_clinical_trial
from test_de_python_v2.json_result import write_json
from test_de_python_v2 import labels

import pprint


def main():
    parquetdir = Path('./tmp')

    drugs_csv = Path('enonce-du-probleme/drugs.csv')
    clinical_trials_csv = Path('enonce-du-probleme/clinical_trials.csv')
    pubmed_csv = Path('enonce-du-probleme/pubmed.csv')

    # ingest the csv files into parquet
    spark = SparkSession.builder.getOrCreate()
    ingest_drug(spark, drugs_csv, parquetdir)
    ingest_pubmed(spark, pubmed_csv, parquetdir)
    ingest_clinical_trial(spark, clinical_trials_csv, parquetdir)

    #
    find_references_drug_pubmed(spark, parquetdir)
    find_references_drug_clinical_trial(spark, parquetdir)
    write_json(spark, parquetdir)

    with open(str(parquetdir / labels.json_output_filename), 'r') as fin:
        j = json.load(fin)
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(j)


if __name__ == '__main__':
    main()
