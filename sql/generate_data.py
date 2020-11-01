import datetime
import random
import subprocess
import sys
from pathlib import Path
import json
json_date_format = '%d-%m-%Y'


def main(max_rows, here: Path):
    rows = []
    for i in range(max_rows):
        date = datetime.date(day=1, month=1, year=2019) + \
            datetime.timedelta(days=random.randint(0, 364))
        prod_price = random.randint(100, 1000)
        prod_qty = random.randint(1, 10)
        rows.append([date.strftime(json_date_format), prod_price, prod_qty])

    with open(str(here / 'transaction.csv'), 'w') as fout:
        fout.write('date,prod_price,prod_qty\n')
        for row in rows:
            fout.write(f"'{row[0]}';{row[1]};{row[2]}\n")


if __name__ == '__main__':
    here = Path(__file__).parent
    max_rows = int(sys.argv[1])
    main(max_rows, here)
