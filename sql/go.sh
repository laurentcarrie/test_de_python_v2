#!/bin/bash

# generate random data in transaction.csv and nomenclature.csv
python generate_data.py --max_transactions 100000 --max_clients 100 --max_products 20

# create the database schema, drop existing tables, load the csv data into sql tables
psql -f init.sql

# run the request for the first part of the test
# the data will be stored in results_1.csv
psql -f requete_1.sql

# run a script that will compare the data calculated
# by python code and data computed by sql
python check_1.py

# run thre request for the first part of the test
# the data will be stored in results_2.csv
psql -f requete_2.sql

# run a script that will compare the data calculated
# by python code and data computed by sql
python check_2.py
