import os, ssl
from elasticsearch.connection import create_ssl_context
import pandas
import datetime
from datetime import timedelta
sc = create_ssl_context()
sc.check_hostname = False
sc.verify_mode = ssl.CERT_NONE

if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
getattr(ssl, '_create_unverified_context', None)):
    ssl._create_default_https_context = ssl._create_unverified_context
from elasticsearch import Elasticsearch 
es = Elasticsearch([{'host':'aims.eos.s7.aero','port':9200,'use_ssl': True}], http_auth=('admin', 'U7c5JTtYd4i4oRLP'), ssl_context=sc)
print(es)
beg = ['heartbeat-aims-crew---management-7.2.1-2020.02.07']
'''
Запрос в Кибане
PUT heartbeat*/_settings
{
  "index.max_result_window" : "100000"
}
'''
n = 0
day = beg[0][39:]
format_str = '%Y.%m.%d'
datetime_obj = datetime.datetime.strptime(day, format_str)
for i in range(n):
    cont = beg[0][:39] + str(datetime_obj.date() + timedelta(days=i+1)).replace("-", ".")
    beg.append(cont)

print(beg)
indices = (es.indices.get_alias("*"))
 #['heartbeat-aims-crew---management-7.1.1-2020.01.19','heartbeat-aims-crew---management-7.1.1-2020.01.20']
docs = pandas.DataFrame()
for i in beg:
    if i in indices.keys():
        #print(i)
        #print(indices.keys())
        res = es.search(index=i, body={
            #"query": { "match_all": {} },
            "_source": ["@timestamp","summary.up"],
            #"query": { "match": { "account_number": 20 } }
            "size": 100000
        }
            )

        elastic_docs = res["hits"]["hits"]
        #print(elastic_docs)
        print ("\ncreating objects from Elasticsearch data.")
        for num, doc in enumerate(elastic_docs):

            # get _source data dict from document
            source_data = doc["_source"]

            # get _id from document
            _id = doc["_id"]
            print(doc["_source"])
            #print(doc["_id"])
            # create a Series object from doc dict object
            doc_data = pandas.Series(source_data, name = _id)

            # append the Series object to the DataFrame object
            docs = docs.append(doc_data, ignore_index=True)


docs = docs.astype(str)
print(docs)
if not  docs.empty:
    docs['summary'] = docs['summary'].map(lambda x: x.replace("'", "").replace(" ", "").strip('}{up:'))
    #docs['agent'] =  docs['agent'].str[14:].str[:-2]
    docs.to_csv("data_feb.csv", mode='a')
