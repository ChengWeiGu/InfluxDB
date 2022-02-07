import numpy as np
import pandas as pd
from influxdb import InfluxDBClient
import random
import time


def initial():
    client = InfluxDBClient('localhost','8086','admin','admin','') # no name of database is given
    print("Following DB are available: ",client.get_list_database())
    return client

def database_op(client, delete_name = None, create_name = None):
    if delete_name != None:
        try:
            client.drop_database(delete_name)
        except:
            print('deletion of DB error!')
    if create_name != None:
        try:
            client.create_database(create_name)
        except:
            print('creation of DB error!')
    
    print("Following DB are available: ",client.get_list_database())
    

def write_data(client,data = {}):
    
    json_body = [
                {
                    "measurement":"spc",
                    "tags":{
                                "topic":"microphone/spc"
                            },
                    "fields":{
                                "score":data['score'],
                                "Qty":data['qty']
                            }
                }
            
            ]
    #寫入數據
    client.write_points(json_body)


def connect_db(db_name):
    client = InfluxDBClient('localhost','8086','admin','admin',db_name)
    print("Following DB are available: ",client.get_list_database())
    print("\n\n")
    return client


def del_measurement(client,table_name):
    client.query(f"drop measurement {table_name}")


def main():
    db_name = 'testdb'
    client = initial()
    database_op(client, create_name = db_name)
    client = connect_db(db_name)
    
    data = {}
    for i in range(10):
        data = dict(score = np.random.random()*10, qty = np.random.randint(4,8))
        write_data(client,data)
        time.sleep(1)
    
    # 查詢語句
    res = client.query('select * from spc')
    print(list(res.get_points()),'\n\n')
    
    #刪除spc
    # client.query('delete from spc')
    # res = client.query('select * from spc')
    # print(list(res.get_points()))
    
    
if __name__ == '__main__':
    main()