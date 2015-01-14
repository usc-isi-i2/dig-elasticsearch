curl -XGET 'http://karma-dig-service.cloudapp.net:55310/swebpages/swebpage/L7xqMunORu2LgjY8NMHA0Q/_source' > 1.json
curl -XGET 'http://karma-dig-service.cloudapp.net:55310/swebpages/swebpage/a244vkhvQfuF78s-xw0gOg/_source' > 2.json
curl -XGET 'http://karma-dig-service.cloudapp.net:55310/swebpages/swebpage/y6KvnHydRPmGMDJnK4HEIg/_source' > 3.json
curl -XGET 'http://karma-dig-service.cloudapp.net:55310/swebpages/swebpage/QBpi_t29QtyBs3QJAeBlUw/_source' > 4.json
curl -XGET 'http://karma-dig-service.cloudapp.net:55310/swebpages/swebpage/idqnu2GARviLVv4EG_HvHA/_source' > 5.json
curl -XGET 'http://karma-dig-service.cloudapp.net:55310/swebpages/swebpage/7D4SFqHiRS6-RPv8ZAR7fQ/_source' > 6.json


curl -XDELETE localhost:9200/dig

curl -XPUT localhost:9200/dig/page/1 -d @data/1.json


curl -XGET localhost:9200/dig/page/1

curl -XGET localhost:9200/dig/_mapping


curl -XPUT localhost:9200/dig -d @mapping.json


curl -XGET 'http://karma-dig-service.cloudapp.net:55310/swebpages/swebpage/_search' 