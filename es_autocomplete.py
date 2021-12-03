# Import the necessary libraries
from elasticsearch import Elasticsearch, helpers
import ssl
import json
import warnings
warnings.filterwarnings('ignore')
from elasticsearch.connection import create_ssl_context

open_distro_ssl_context = create_ssl_context()

open_distro_ssl_context.check_hostname = False
open_distro_ssl_context.verify_mode = ssl.CERT_NONE

client = Elasticsearch(scheme = "https",
                    hosts = [{"port":9200, "host":"localhost"}],
                    ssl_context = open_distro_ssl_context,
                    http_auth = ("admin", "admin")
                    ) 


body = {
  "settings": {
    "index": {
      "analysis": {
        "filter": {},
        "analyzer": {
          "keyword_analyzer": {
            "filter": [
              "lowercase",
              "asciifolding",
              "trim"
            ],
            "char_filter": [],
            "type": "custom",
            "tokenizer": "keyword"
          },
          "edge_ngram_analyzer": {
            "filter": [
              "lowercase"
            ],
            "tokenizer": "edge_ngram_tokenizer"
          },
          "edge_ngram_search_analyzer": {
            "tokenizer": "lowercase"
          }
        },
        "tokenizer": {
          "edge_ngram_tokenizer": {
            "type": "edge_ngram",
            "min_gram": 2,
            "max_gram": 5,
            "token_chars": [
              "letter"
            ]
          }
        }
      }
    }
  },
  "mappings": {
      "properties": {
        "Interests": {
          "type": "text",
          "fields": {
            "keywordstring": {
              "type": "text",
              "analyzer": "keyword_analyzer"
            },
            "edgengram": {
              "type": "text",
              "analyzer": "edge_ngram_analyzer",
              "search_analyzer": "edge_ngram_search_analyzer"
            },
            "completion": {
              "type": "completion"
            }
          },
          "analyzer": "standard"
        }
      }
    }
  }

index_create = client.indices.create(
    index="employees_db",
    body=body,
    ignore=400 # ignore 400 already exists code
)

docs = []
for line in open("Employees100K.json", 'r'):
    docs.append(json.loads(line))

try:
    print ("\nAttempting to index the list of docs using helpers.bulk()")

    response = helpers.bulk(client, docs, index = "employees_db")

    #print the response returned by Elasticsearch
    print ("helpers.bulk() RESPONSE:", response)
    print ("helpers.bulk() RESPONSE:", json.dumps(response, indent=4))

except Exception as err:
    # print any errors returned w
    ## Prerequisiteshile making the helpers.bulk() API call
    print("Elasticsearch helpers.bulk() ERROR:", err)
    quit()

res = client.search(index="test_index", body ={"query":{"match_all":{}},"size":10})

# if __name__=='__main__':
#     main()
