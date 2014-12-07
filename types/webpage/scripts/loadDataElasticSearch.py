"""
Load data from json file to ElasticSearch
Read an array of json objects, indexing each json object
"""

__author__ = 'saggu'

import json
from elasticsearch import Elasticsearch
from sys import stderr
import sys
import argparse

def loadDatainES(hostname, port,filename, index, doctype):
    try:
        es = Elasticsearch([{'host': hostname, 'port': port}])
        with open(filename) as f:
            d = json.load(f)
            for wp in d:
              res = es.index(index=index,doc_type=doctype,body=wp)
              print "indexing id: " + res["_id"] + " for uri: " + wp["uri"]
    except Exception, e:
        print >> stderr.write('ERROR: %s\n' % str(e))


if __name__ == '__main__':
    
    argp = argparse.ArgumentParser()
    argp.add_argument("hostname",help="Elastic Search Server hostname")
    argp.add_argument("port",type=int, help="Elastic Search Server port")
    argp.add_argument("filepath",help="json file to be loaded in ElasticSearch")
    argp.add_argument("indexname",help="desired name of the index in ElasticSearch")
    argp.add_argument("doctype",help="type of the document to be indexed")
    
    arguments = argp.parse_args()
    
    
    loadDatainES(arguments.hostname, arguments.port, arguments.filepath, arguments.indexname, arguments.doctype)


    print "Done!"
