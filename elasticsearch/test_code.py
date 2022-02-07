import numpy as np
import pandas as pd

from time import time
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json


def get_index_info(es, index_name = 'school_members'):
    result = es.indices.get(index=index_name)
    print(result)


def create_index(es):
    index_name = 'school_members'
    body = dict()
    body['settings'] = get_setting()
    body['mappings'] = get_mappings()
    # print(json.dumps(body)) #可以用json.dumps輸出來看格式有沒有包錯 type = str
    es.indices.create(index=index_name, body=body)


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
            "sid": {
                "type": "integer"
            },
            "name": {
                "type": "text"
            },
            "age": {
                "type": "integer"
            },
            "class": {
                "type": "keyword"
            }
        }
    }

    return mappings


def change_mappings(es):
    body = get_teacher_mappings()
    es.indices.put_mapping(index = 'school_members', body = body)
    
    
def get_teacher_mappings():
    mappings = {
        "properties": {
            "tid": {
                "type": "integer"
            },
            "name": {
                "type": "text"
            },
            "age": {
                "type": "integer"
            },
            "class": {
                "type": "keyword"
            },
            "salary": {
                "type": "integer"
            }
        }
    }
    return mappings


def create_data(es, data):
    for i, d in enumerate(data,1):
        es.index(index = 'school_members', body = d, id = i) #逐筆匯入數據


def main_basic():
    # es = Elasticsearch('http://localhost:9200/',timeout=30, max_retries=10, retry_on_timeout=True)
    es = Elasticsearch(hosts='127.0.0.1', port=9200,timeout=30, max_retries=10, retry_on_timeout=True) # default: 127.0.0.1
    create_index(es)
    # change_mappings(es)
    es.indices.put_alias(index = 'school_members', name = 'school')#使用/指定別名
    # es.indices.delete_alias(index = 'school_members', name = 'school') #刪除別名
    
    # get_index_info(es)



def main_deliver_data():
    data = []
    csv_itr = pd.read_csv('student.csv', iterator = True, chunksize = 2)
    for i, df in enumerate(csv_itr):
       records = df.where(pd.notnull(df), None).T.to_dict()
       data += [records[df_key] for df_key in records]
    
    es = Elasticsearch(hosts = '127.0.0.1', port = 9200, timeout = 30, max_retries = 10, retry_on_timeout=True)
    create_data(es, data)
    # then open elastichead → 資料瀏覽 → 索引 → school_members


def main_edit_data():
    es = Elasticsearch(hosts = '127.0.0.1', port = 9200, timeout = 30, max_retries = 10, retry_on_timeout=True)
    es.delete(index = 'school_members', id = 2) #刪除資料
    body = {
            'doc':{' age':28}
            }
    es.update(index = 'school_members', id = 4, body = body) #更新資料
    

def main_deliver_bulk_data():
    es = Elasticsearch(hosts = '127.0.0.1', port = 9200, timeout = 30, max_retries = 10, retry_on_timeout=True)
    csv_itr = pd.read_csv('student.csv', iterator = True, chunksize = 2)
    
    data = []
    for i, df in enumerate(csv_itr):
        data_ = df.where(pd.notnull(df), None).values
        for tid, name, age, class_ in data_:
            data.append({
                            "_index":"school_members",
                            "_op_type":"index", #"index"增加資料，"delete"則刪除資料，"update"則更新資料
                            "_source":{
                                       "tid":int(tid),
                                       "name":name,
                                       "age": int(age),
                                       "class":class_
                                       
                                       }
                        
                        })
                        
    # for _id in ["o4pHlnUB2pgIr_Osc4xl","pIpHlnUB2pgIr_Osc4xl","oYpHlnUB2pgIr_Osc4xl","oopHlnUB2pgIr_Osc4xl"]:
        # data.append({
                            # "_index":"school_members",
                            # "_op_type":"delete", #增加資料，"delete"則刪除資料，"update"則更新資料
                            # "_id":_id
                        
                        # })
        
                        
    # helpers.bulk(es,data)
    
    
def main_search_data():
    es = Elasticsearch(hosts = '127.0.0.1', port = 9200, timeout = 30, max_retries = 10, retry_on_timeout=True)
    query1 = {
             "query":{
                        "bool":{
                                "must":{
                                        "term":{
                                                " age":20
                                        }
                                }
                        }
             }
    
    }
    
    query2 = {
                "query":{
                            "bool":{
                                    "should":[
                                                {"term":{" age":20}},
                                                {"match":{" name":" Michael2"}}
                                    ]
                            }
                }
    
    }
    
    result = es.search(index = "school_members", body = query2)
    print(json.dumps(result, ensure_ascii=False))
    

def main():
    index_name = 'school_members'
    es = Elasticsearch(hosts='127.0.0.1', port=9200,timeout=30, max_retries=10, retry_on_timeout=True) # default: 127.0.0.1
    es.indices.create(index=index_name, body=body) # create the index and body
    es.indices.put_alias(index = 'school_members', name = 'school')
    
    data = []
    csv_itr = pd.read_csv('student.csv', iterator = True, chunksize = 2)
    for i, df in enumerate(csv_itr):
       records = df.where(pd.notnull(df), None).T.to_dict()
       data += [records[df_key] for df_key in records]
    
    for i, d in enumerate(data,1):
        es.index(index = index_name, body = d, id = i) #逐筆匯入數據
    



if __name__ == "__main__":
    data = []
    csv_itr = pd.read_csv('student.csv', iterator = True, chunksize = 2)
    for i, df in enumerate(csv_itr):
       records = df.where(pd.notnull(df), None).T.to_dict()
       data += [records[df_key] for df_key in records]
    print(data)
    # main_basic()
    # main_deliver_data()
    # main_edit_data()
    # main_deliver_bulk_data()
    # main_search_data()
    
