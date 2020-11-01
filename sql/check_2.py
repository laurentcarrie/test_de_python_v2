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

    collected_client_product_with_python = {}
    with open('transaction.csv', newline='\n') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        next(reader)
        for row in reader:
            try:
                client_id = int(row[4])
                product_id = int(row[5])
                product_type = collected_nomenclatures_with_python[product_id]
                if collected_client_product_with_python.get(client_id) is None:
                    collected_client_product_with_python[client_id] = {
                        'MEUBLE': 0, 'DECO': 0}
                collected_client_product_with_python[client_id][product_type] = 1 + collected_client_product_with_python[client_id].get(
                    product_type, 0)
            except Exception as e:
                print(f'could not understand row {row}')
                raise e

    collected_data_with_sql = {}
    with open('result_2.csv', newline='\n') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        for row in reader:
            try:
                client_id = int(row[0])
                if row[1] == '\\N':
                    meuble = 0
                else:
                    meuble = int(row[1])
                if row[2] == '\\N':
                    deco = 0
                else:
                    deco = int(row[2])
                if collected_data_with_sql.get(client_id) is None:
                    collected_data_with_sql[client_id] = {'MEUBLE': 0, 'DECO': 0}
                collected_data_with_sql[client_id]['MEUBLE'] = meuble + \
                    collected_data_with_sql[client_id].get('MEUBLE', 0)
                collected_data_with_sql[client_id]['DECO'] = deco + \
                    collected_data_with_sql[client_id].get('DECO', 0)
            except Exception as e:
                print(f'could not understand row {row}')
                raise e

    for client_id, d in collected_client_product_with_python.items():
        d2 = collected_data_with_sql.get(client_id, {'MEUBLE': 0, 'DECO': 0})
        if d != d2:
            print(f'{client_id}, python:{d}, sql:{d2}')
            exit(1)
    else:
        print(f'OK , {len(collected_data_with_sql)} items compared !')


if __name__ == '__main__':
    main()
