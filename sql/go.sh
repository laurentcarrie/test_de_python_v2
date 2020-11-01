#!/bin/bash

# generate data : a csv file.
python generate_data.py --max_transactions 10 --max_clients 2 --max_products 3

# create the database schema, drop existing tables
psql -f init.sql

# run thre request for the first part of the test
# the data will be stored in results.csv
psql -f requete.sql

# run a script that will compare the data calculated
# by python code and data computed by sql
python check.py
