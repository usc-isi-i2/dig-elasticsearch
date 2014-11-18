#!/usr/bin/python

import sys
try:
    import simplejson as json
except:
    import json

import requests

inputFile = "files-to-load.json"

with open(inputFile, 'r') as f:
    js = json.load(f)

es = "http://localhost:9200/dig/page"

counter = 1
for o in js["hits"]["hits"]:
    _source = o["_source"]
    url = "%s/%s" % (es, counter)
    #print >> sys.stderr, json.dumps(_source["hasBodyPart"], indent=4)
    print >> sys.stderr, "url = %r" % url
    requests.put(url, json.dumps(_source))
    counter +=1


