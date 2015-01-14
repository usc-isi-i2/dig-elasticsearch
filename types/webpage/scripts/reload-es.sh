#!/bin/sh
#
curl -XDELETE localhost:9200/dig
#
curl -XPUT localhost:9200/dig -d @../webpage-mapping-02.json
#
curl -XPUT localhost:9200/dig/page/1 -d @../datasets/1.json
curl -XPUT localhost:9200/dig/page/2 -d @../datasets/2.json
curl -XPUT localhost:9200/dig/page/3 -d @../datasets/3.json
