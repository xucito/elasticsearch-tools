from datetime import datetime
from elasticsearch import Elasticsearch
import ssl
from elasticsearch.connection import create_ssl_context
from elasticsearch.helpers import scan

username = 'elastic'
password = 'Z4oO2zrTP5gnuhFf0Bta'
url = '10.10.10.9'  # url or ip-address of the node
port = 9200
scheme = 'https'
index = 'logs'
output = 'output.csv'

query = {
    "query": {
        "match_all": {}
    },
    "sort": [
        {
            "@timestamp": {
                "order": "desc"
            }
        }
    ]
}

ssl_context = create_ssl_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

es = Elasticsearch([url],
                   http_auth=(username, password),
                   scheme=scheme,
                   port=port,
                   ssl_context=ssl_context,
                   timeout=30000)
res = scan(es,
           query=query,
           index=index,
           size=10000,
           request_timeout=30000)

file1 = open(output, "w")

finalString = ""
logsWritten = 1
failedMessages = []
for item in res:
    try:
        file1.write(item["_source"]["host"] + "," + item["_source"]
                    ["@timestamp"] + ","+item["_source"]["message"] + "\n")
    except:
        print("An exception occurred on item " + item["_source"]["message"])
        failedMessages.append(item["_source"]["message"])
print("ALL FAILED MESSAGES")
for item in failedMessages:
    print(item)