import numpy as np
import pandas as pd

from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json
import datetime
import time

def get_setting():
    settings = {
        "index": {
            "number_of_shards": 3,
            "number_of_replicas": 2
        }
    }
    
    return settings


def get_mappings():
    mappings = {
        "properties": {
            "date": {
                "type": "date"
            },
            "score": {
                "type": "float"   
            },
            "UCL": {
                "type": "float"
            },
            "LCL": {
                "type": "float"
            }
        }
    }

    return mappings


def create_index(es):
    index_name = 'spc_data'
    body = dict()
    body['settings'] = get_setting()
    body['mappings'] = get_mappings()
    # print(json.dumps(body)) #可以用json.dumps輸出來看格式有沒有包錯 type = str
    es.indices.create(index=index_name, body=body)


def create_data(es, data):
    for i, d in enumerate(data,1):
        es.index(index = 'spc_data', body = d, id = i) #逐筆匯入數據

     
def delete_data():
    es = Elasticsearch(hosts='127.0.0.1', port=9200,timeout=30, max_retries=10, retry_on_timeout=True) # default: 127.0.0.1
    for i in range(15,19):
        es.delete(index = 'spc_data', id = i+1) #刪除資料
        
        
def insert_loop_data():
    es = Elasticsearch(hosts='127.0.0.1', port=9200,timeout=30, max_retries=10, retry_on_timeout=True) # default: 127.0.0.1
    
    count = 0
    while True:
        t = datetime.datetime.now()+datetime.timedelta(days = count*4-18)
        print('insert data @time: ',t)
        
        data = {'time': t, 'score':np.random.random()*10, 'UCL':10.0, 'LCL':-10.0}
        es.index(index = 'spc_data', body = data, id = count) #逐筆匯入數據
        
        time.sleep(20)
        count += 1
        if count >= 10:
            break
            
            
def test():
    es = Elasticsearch(hosts='127.0.0.1', port=9200,timeout=30, max_retries=10, retry_on_timeout=True) # default: 127.0.0.1
    create_index(es)
    # change_mappings(es)
    es.indices.put_alias(index = 'spc_data', name = 'spc')#使用/指定別名
    # es.indices.delete_alias(index = 'school_members', name = 'school') #刪除別名
    # get_index_info(es)
    
    data = list()
    shift = 0
    for i in range(10):
        data += [{'time':(datetime.datetime.now()+datetime.timedelta(days = shift)), 'score':np.random.random()*10, 'UCL':10.0, 'LCL':-10.0}]
        shift += 1
    create_data(es, data)
    
    


if __name__ == "__test__":
    # test()
    insert_loop_data()
    # delete_data()
    
    