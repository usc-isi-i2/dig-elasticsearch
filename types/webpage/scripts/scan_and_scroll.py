__author__ = 'amandeep'

from elasticsearch import Elasticsearch
import argparse


def scanandscroll(index, doctype, hostname="localhost", port=9200):

    query = {"query" : {"match_all" : {}}}

    # Initialize the scroll
    es = Elasticsearch(['https://memex:j9fd0LK8.a6olH24U7X81@'+hostname + ":" + str(port)],show_ssl_warnings=False)
    page = es.search(index = index,doc_type = doctype,scroll = '30m',search_type = 'scan',size = 1000,body = query)
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']
    #print "Total hits:" + str(scroll_size)
    # Start scrolling
    while scroll_size > 0:
        #print "Scrolling..."
        page = es.scroll(scroll_id=sid, scroll='30m')
        # Update the scroll ID
        sid = page['_scroll_id']
        # Get the number of results that we returned in the last scroll
        scroll_size = len(page['hits']['hits'])
        #print page['hits']['hits']

        for i in range(len(page['hits']['hits'])):
            print page['hits']['hits'][i]
        #print "scroll size: " + str(scroll_size)
        # Do something with the obtained page



if __name__ == '__main__':

    argp = argparse.ArgumentParser()
    argp.add_argument("-hostname",help="Elastic Search Server hostname, defaults to 'localhost'",default="localhost")
    argp.add_argument("-port",type=int, help="Elastic Search Server port,defaults to 9200",default=9200)
    argp.add_argument("indexname",help="desired name of the index in ElasticSearch")
    argp.add_argument("doctype",help="type of the document to be indexed")
    arguments = argp.parse_args()

    if arguments.hostname and arguments.port:
        scanandscroll(arguments.indexname, arguments.doctype, arguments.hostname, arguments.port)
    else:
        scanandscroll(arguments.indexname, arguments.doctype)

    #print "Thats all folks!"
