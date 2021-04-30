import sys
import boto3
import csv

dynamodb =  boto3.resource('dynamodb', 'us-east-2')
def read(csv_file, list):
    rows = csv.DictReader(open(csv_file, encoding="utf8"))
    
    for row in rows:
        list.append(row)
        
def write(table_name, rows):
    table = dynamodb.Table(table_name)
    
    with table.batch_writer() as batch:
        for row in rows:
            batch.put_item(Item=row)
    return True

table_name = 'data_customers'
file_name = 'datacsv.csv'
items = []

if __name__ == "__main__":
    read(file_name, items)
    status = write(table_name, items)
    if(status):
        print("exito")
    else:
        print("fallo")
        
        