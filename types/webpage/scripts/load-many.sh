#!/bin/sh

# Get objects that we want to load.
#
curl -XGET "http://karma-dig-service.cloudapp.net:55310/dig/page/_search?pretty=true" -d'
{   
    "query": {"match_all": {}},
    "size":  3000
}' > files-to-load.json

# Clear the local elastic search
#
curl -XDELETE localhost:9200/dig

curl -XPOST localhost:9200/dig/ -d @../webpage-settings.json

# Reinitialize the mapping
#
#curl -XPUT localhost:9200/dig -d @../webpage-mapping.json
curl -XPUT localhost:9200/dig/page/_mapping -d @../webpage-mapping-english.json

# Load the objects we retrieved
#
python insert.py
