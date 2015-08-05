To run ScanAndScroll

'''mvn exec:java -Dexec.mainClass="edu.isi.dig.elasticsearch.ScanAndScroll" -Dexec.args="--esprotocol https/http --esindex INDEXNAME --esdoctype DOCTYPE --esusername USERNAME --espassword PASSWORD --esport PORT --eshostname HOSTNAME --esquery \"{\"query\": {\"match_all\": {}}}\" --outputfile /tmp/test.json"'''

The default number of documents retrieved is 100, get more by specifying the parameter "doclimit", set it to -1 to get every document in the index
