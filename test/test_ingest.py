import pytest
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


class Test_ingest:

    def test_ingest_drug(self, datadir):
        spark = SparkSession.builder.getOrCreate()
        ingest_drug(spark, datadir / 'drugs.csv', datadir)
        data = spark.read.parquet(str(datadir / labels.parquet_drug)).collect()
        assert len(data) == 7
        for row in data:
            if row.atccode == 'A03BA':
                assert row.drug == 'ATROPINE'

    def test_ingest_pubmed(self, datadir):
        spark = SparkSession.builder.getOrCreate()
        ingest_pubmed(spark, datadir / 'pubmed.csv', datadir)
        data = spark.read.parquet(str(datadir / labels.parquet_pubmed)).collect()
        assert len(data) == 8
        for row in data:
            if row.id == 6:
                assert row.title == 'Rapid reacquisition of contextual fear following extinction in mice: effects of amount of extinction, tetracycline acute ethanol withdrawal, and ethanol intoxication.'
                assert row.date == datetime.date(day=1, month=1, year=2020)
            if row.id == 7:
                assert row.title == 'The High Cost of Epinephrine Autoinjectors and Possible Alternatives.'
                assert row.date == datetime.date(day=1, month=2, year=2020)

    def test_ingest_clinical_trial(self, datadir):
        spark = SparkSession.builder.getOrCreate()
        ingest_clinical_trial(spark, datadir / 'clinical_trials.csv', datadir)
        data = spark.read.parquet(str(datadir / labels.parquet_clinical_trial)).collect()
        assert len(data) == 8
        for row in data:
            if row.id == 'NCT01967433':
                assert row.scientific_title == 'Use of Diphenhydramine as an Adjunctive Sedative for Colonoscopy in Patients Chronically on Opioids'
                assert row.date == datetime.date(day=1, month=1, year=2020)
            if row.id == 7:
                assert row.title == 'The High Cost of Epinephrine Autoinjectors and Possible Alternatives.'
                assert row.date == datetime.date(day=1, month=2, year=2020)


class Test_find_references:

    def test_find_references(self, datadir):
        spark = SparkSession.builder.getOrCreate()
        ingest_drug(spark, datadir / 'drugs.csv', datadir)
        ingest_pubmed(spark, datadir / 'pubmed.csv', datadir)
        ingest_clinical_trial(spark, datadir / 'clinical_trials.csv', datadir)
        find_references_drug_pubmed(spark, datadir)
        find_references_drug_clinical_trial(spark, datadir)
        write_json(spark, datadir)
        with open(str(datadir / labels.json_output_filename)) as fjson:
            data = json.load(fjson)

        assert set([item['id'] for item in data['S03AA']
                    [labels.json_pubmed_array_name]]) == {4, 5, 6}
        assert len(data['V03AB'][labels.json_pubmed_array_name]) == 1
        assert data['V03AB'][labels.json_pubmed_array_name] == [
            {'id': 6, 'date': datetime.date(day=1, month=1, year=2020).strftime(labels.json_date_format)}]
        assert data['R01AD'][labels.json_clinical_trials_array_name] == [
            {'id': 'NCT04153396', 'date': datetime.date(
                day=1, month=1, year=2020).strftime(labels.json_date_format)}
        ]
