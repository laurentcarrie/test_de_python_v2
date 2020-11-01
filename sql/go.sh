#!/bin/bash

# generate data : a csv file. The argument is the number of row
python generate_data.py 100000

# create the database schema, drop existing tables
psql -f init.sql

# run thre request for the first part of the test
# the data will be stored in results.csv
psql -f requete.sql

# run a script that will compare the data calculated
# by python code and data computed by sql
python check.py
