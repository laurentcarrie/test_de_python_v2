import datetime
import json
import csv


def main():

    collected_nomenclatures_with_python = {}
    with open('nomenclature.csv', newline='\n') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        next(reader)
        for row in reader:
            try:
                product_id = int(row[0])
                product_type = row[1]
                collected_nomenclatures_with_python[product_id] = product_type
            except Exception as e:
                print(f'could not understand row {row}')
                raise e

    collected_client_product = {}
    with open('transaction.csv', newline='\n') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        next(reader)
        for row in reader:
            try:
                product_id = float(row[4])
                product_type = collected_nomenclatures_with_python[product_id]
                collected_client_product[product_type] = collected_client_product.get(
                    product_type, 0)
            except Exception as e:
                print(f'could not understand row {row}')
                raise e

    collected_data_with_sql = {}
    with open('result_2.csv', newline='\n') as csvfile:
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

    for date, chiffre in collected_client_product.items():
        chiffre2 = collected_data_with_sql.get(date, 'Does not exist !')
        if chiffre != chiffre2:
            print(f'{date}, python:{chiffre}, sql:{chiffre2}')
            exit(1)
    else:
        print(f'OK , {len(collected_data_with_sql)} items compared !')


if __name__ == '__main__':
    main()
