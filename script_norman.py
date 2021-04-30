import sys
import pandas as pd
import pyodbc
from datetime import datetime


def storeToSql(driver, server, database, query_path, table_name, log_path):
    connection_string= "driver={%s};server=%s;database=%s" % (driver, server, database)
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    f = open(query_path, "r")
    query=str(f.read())
    x=pd.read_sql(query,connection)
    cols = ",".join([str(i) for i in x.columns.tolist()])
    sql = f"INSERT INTO {table_name} (" +cols + ") %s" % query
    #print(sql)
    cursor.execute(sql)
    connection.commit()
    connection.close()
    date = datetime.now()
    now= date.strftime("%d/%m/%Y %H:%M:%S")
    if(cursor.rowcount>0):
         log = "Fueron insertados %s registros: %s\n" % (cursor.rowcount, now)
    else:  log = "No se insertaron los registros: %s\n" % (now)
    f = open(log_path, "a")
    f.write(log)
    return log


def main(driver, server, database, query_path, table_name, log_path):
    return storeToSql(driver, server, database, query_path, table_name, log_path)
    
if __name__ == "__main__":
    driver = str(sys.argv[1])
    server = str(sys.argv[2])
    database = str(sys.argv[3])
    query_path = str(sys.argv[4])
    table_name = str(sys.argv[5])
    log_path = str(sys.argv[6])
    main(driver, server, database, query_path, table_name, log_path)