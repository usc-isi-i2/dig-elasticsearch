
from elasticsearch import Elasticsearch
import argparse
import codecs



def scanandscroll(index, doctype, query, hostname="localhost", port=9200, username = None, password = None):

    #query = {"query" : {"match_all" : {}}}
    #query = {"query": {"filtered": {"query": {"match_phrase": {"url": "http://ieeexplore.ieee.org"}},"filter": {"and": {"filters": [{ "term": {"team": "hyperion-gray"}}]}}}}}

    # Initialize the scroll

    if username and password:
        es = Elasticsearch(['https://' + username + ':' + password + '@' + hostname + ":" + str(port)],show_ssl_warnings=False)
    else:
        es = Elasticsearch(['https://' + hostname + ":" + str(port)],show_ssl_warnings=False)

    page = es.search(index = index,doc_type = doctype,scroll = '20m',search_type = 'scan',size = 5,body = query)
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']
    #print "Total hits:" + str(scroll_size)

    with codecs.open("els_results.json", "w", "utf-8") as f:
        with codecs.open("scroll_id.txt","w","utf-8") as sf:
            with codecs.open("els_scan.log", "w" ,"utf-8") as log:
                # Start scrolling
                while scroll_size > 0:
                    try:
                        #print "Scrolling..."
                        page = es.scroll(scroll_id=sid, scroll='20m')
                        # Update the scroll ID
                        sid = page['_scroll_id']
                        sf.write(str(sid) + '\n')
                        # Get the number of results that we returned in the last scroll
                        scroll_size = len(page['hits']['hits'])
                        #print page['hits']['hits']

                        for i in range(len(page['hits']['hits'])):
                            #print page['hits']['hits'][i]

                            url = page['hits']['hits'][i]['_source']['url']
                            if isinstance(url,list):
                                first_url = url[0]
                            else:
                                first_url = url

                            f.write(first_url + "\t" + str(page['hits']['hits'][i]) + '\n')
                    except Exception as e:
                        log.write(e.message + "\n")
                        pass

            #print "scroll size: " + str(scroll_size)
            # Do something with the obtained page
    f.close()
    sf.close()
    log.close()




if __name__ == '__main__':

    argp = argparse.ArgumentParser()
    argp.add_argument("-hostname",help="Elastic Search Server hostname, defaults to 'localhost'",default="localhost")
    argp.add_argument("-port",type=int, help="Elastic Search Server port,defaults to 9200",default=9200)
    argp.add_argument("-username", help = "username for Elasticsearch cluster", default = None)
    argp.add_argument("-password", help = "password for Elasticsearch cluster", default=None)
    argp.add_argument("indexname",help="desired name of the index in ElasticSearch")
    argp.add_argument("doctype",help="type of the document to be indexed")
    argp.add_argument("query",help="query to the elasticsearch")
    arguments = argp.parse_args()

    if arguments.hostname and arguments.port:
        scanandscroll(arguments.indexname, arguments.doctype, arguments.query, arguments.hostname, arguments.port, arguments.username, arguments.password)
    else:
        scanandscroll(arguments.indexname, arguments.doctype, arguments.query, arguments.username, arguments.password)

    #print "Thats all folks!"