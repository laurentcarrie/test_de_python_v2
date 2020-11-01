from pyspark import Row
from pyspark.sql import SparkSession
from pathlib import Path
import datetime
import json
from typing import Callable, Any, Iterator
from test_de_python_v2 import labels


def write_json(spark: SparkSession, datadir: Path):
    result = {}

    def init_json_item(row):
        result[row.drug_atccode] = {'name': row.drug_name,
                                    labels.json_pubmed_array_name: [], labels.json_clinical_trials_array_name: [],
                                    labels.json_journal_array_name: []}

    def iterate_on_references(parquet_path, array_name, id_getter, date_getter: Callable[[Any], datetime.date]):
        item_iterator: Iterator[Row] = spark.read.parquet(
            str(parquet_path)).toLocalIterator()
        count = 0
        max_allowed = 10000
        for item in item_iterator:
            count = count + 1
            if count > max_allowed:
                raise Exception('too much data to write a json file... bailing out')
            if result.get(item.drug_atccode) is None:
                init_json_item(item)

            result[item.drug_atccode][array_name].append(
                {'id': id_getter(item),
                 'date': date_getter(item).strftime(labels.json_date_format)
                 })

    iterate_on_references(parquet_path=datadir / labels.parquet_drug_pubmed,
                          array_name=labels.json_pubmed_array_name,
                          id_getter=lambda item: item.pubmed_id,
                          date_getter=lambda item: item.pubmed_date)

    iterate_on_references(parquet_path=datadir / labels.parquet_drug_clinical_trial,
                          array_name=labels.json_clinical_trials_array_name,
                          id_getter=lambda item: item.clinical_trial_id,
                          date_getter=lambda item: item.clinical_trial_date)

    iterate_on_references(parquet_path=datadir / labels.parquet_drug_journal,
                          array_name=labels.json_journal_array_name,
                          id_getter=lambda item: item.journal,
                          date_getter=lambda item: item.date)

    with open(str(datadir / labels.json_output_filename), 'w') as fp:
        json.dump(result, fp)
