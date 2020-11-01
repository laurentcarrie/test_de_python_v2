# parquet file names
parquet_drug = 'parquet-drug'
parquet_pubmed = 'parquet-pubmed'
parquet_clinical_trial = 'parquet-clinical_trial'
parquet_drug_pubmed = 'parquet-drug_pubmed'
parquet_drug_clinical_trial = 'parquet-drug_clinical_trial'
parquet_drug_journal = 'parquet-drug_journal'

# json output filename
json_output_filename = 'result.json'
# json does not have a date type,
# we force our own conversion to be sure of what is going on
# when converting dates to string and conversely
json_date_format = '%d/%m/%Y'

# array name for items in json output file
json_pubmed_array_name = 'pubmeds'
json_clinical_trials_array_name = 'clinical_trials'
json_journal_array_name = 'journaux'
