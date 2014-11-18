#!/bin/sh

# Get objects that we want to load.
#
curl -XGET "http://karma-dig-service.cloudapp.net:55310/swebpages/swebpage/_search?pretty=true" -d'
{   
    "query": {"match_all": {}},
    "size":  100
}' > files-to-load.json

# Clear the local elastic search
#
curl -XDELETE localhost:9200/dig

# Reinitialize the mapping
#
curl -XPUT localhost:9200/dig -d @../webpage-mapping.json

# Load the objects we retrieved
#
python insert.py
