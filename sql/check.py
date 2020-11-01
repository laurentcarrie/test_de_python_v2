import datetime
import json
import csv


def main():
    collected_data_with_python = {}
    with open('transaction.csv', newline='\n') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        next(reader)
        for row in reader:
            try:
                date = datetime.datetime.strptime(row[0], "'%d-%m-%Y'")
                price = float(row[1])
                qty = float(row[2])
                collected_data_with_python[date] = price * \
                    qty + collected_data_with_python.get(date, 0)
            except Exception as e:
                print(f'could not understand row {row}')
                raise e

    collected_data_with_sql = {}
    with open('result.csv', newline='\n') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        for row in reader:
            try:
                date = datetime.datetime.strptime(row[0], '%Y-%m-%d')
                if row[1] == '\\N':
                    chiffre = 0
                else:
                    chiffre = float(row[1])
                collected_data_with_sql[date] = chiffre
            except Exception as e:
                print(f'could not understand row {row}')
                raise e

    for date, chiffre in collected_data_with_python.items():
        chiffre2 = collected_data_with_sql.get(date, 'Does not exist !')
        if chiffre != chiffre2:
            print(f'{date}, python:{chiffre}, sql:{chiffre2}')
            exit(1)
    else:
        print('OK !')


if __name__ == '__main__':
    main()