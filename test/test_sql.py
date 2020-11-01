import pytest
import subprocess
from pathlib import Path
import logging
import random
import datetime
import json
import psycopg2


class Test_sql:

    def reset_database(self, datadir):
        connection = psycopg2.connect(
            port='5432',
            database='laurent')
        cursor = connection.cursor()
        cursor.execute('drop table if exists transaction ;')
        cursor.execute(
            'CREATE TABLE transaction( date date, order_id int, client_id int, prod_id int, prod_price float, prod_qty float );')
        with open(str(datadir / 'transaction.csv'), 'r') as f:
            next(f)
            cursor.copy_from(f, 'transaction', sep=';', columns=[
                             'date', 'prod_price', 'prod_qty'])

        connection.commit()
        return connection

    def test_1(self, datadir):
        connection = self.reset_database(datadir)

        collected_data = {}
        with open(datadir / 'transaction.json', 'r') as fin:
            j = json.load(fin)
            for row in j:
                date = datetime.datetime.strptime(row[0], '%d-%m-%Y')
                price = row[1]
                qty = row[2]
                old = collected_data.get(date, 0)
                collected_data[date] = old + price * qty

        sql_command = """
\\set start '2019-01-01'

with tmp as (
select days.entry as date,
       transaction.prod_price * transaction.prod_qty as summed_prices
       from generate_series(date:'start',
                            date:'start' + interval '1 year' - interval '1 month',
                            interval '1 day'
       ) as days(entry)
       left join transaction on transaction.date = days.entry
)
select date,sum(summed_prices) from tmp
group by date
order by date
;
        """

        cursor = connection.cursor()
        cursor.execute("\\set start '2019-01-01'")

        cursor.execute(sql_command)
        for row in cursor.fetchall():
            logging.info(row)

        assert 1 == 0
