import datetime
import random
import subprocess
import sys
from pathlib import Path
import click
date_format = '%d-%m-%Y'


@click.command()
@click.option('--max_transactions', type=int, default=False, help='max_transactions')
@click.option('--max_clients', type=int, default=False, help='max_clients')
@click.option('--max_products', type=int, default=False, help='max_products')
def main(max_transactions, max_clients, max_products):

    with open('nomenclature.csv', 'w') as fout:
        fout.write('product_id;product_type;product_name\n')
        product_types = ['MEUBLE', 'DECO']
        for i in range(max_products):
            product_id = i + 1
            product_type = product_types[random.randint(0, len(product_types) - 1)]
            product_name = f'product_{i}'
            fout.write(f'{product_id};{product_type};{product_name}\n')

    with open('transaction.csv', 'w') as fout:
        fout.write('order_id,date,prod_price,prod_qty,client_id,prod_id\n')
        for i in range(max_transactions):
            order_id = i + 1
            date = datetime.date(day=1, month=1, year=2019) + \
                datetime.timedelta(days=random.randint(0, 364))
            prod_price = random.randint(100, 1000)
            prod_qty = random.randint(1, 10)
            client_id = random.randint(1, max_clients)
            prod_id = random.randint(1, max_products)

            fout.write(
                f'{order_id};{date.strftime(date_format)};{prod_price};{prod_qty};{client_id};{prod_id}\n')


if __name__ == '__main__':
    main()
