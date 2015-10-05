To run ScanAndScroll 

1. ```git clone https://github.com/usc-isi-i2/dig-elasticsearch.git```
2. ```cd dig-elasticsearch/elasticsearch```
3. ```mvn clean install```
4. Use the command below to run ScanAndScroll
```
mvn exec:java -Dexec.mainClass="edu.isi.dig.elasticsearch.ScanAndScroll" -Dexec.args="--esprotocol https/http --esindex INDEXNAME --esdoctype DOCTYPE --esusername USERNAME --espassword PASSWORD --esport PORT --eshostname HOSTNAME --esquery PATH_TO_FILE_WITH_QUERY --outputfile /tmp/test.json"
```
The following options are available with the api
```
usage: ScanAndScroll
 -doclimit,--doclimit <arg>       number of documents retrieved, -1 to get trillion
 -esdoctype,--esdoctype <arg>     elasticsearch doc type
 -eshostname,--eshostname <arg>   elasticsearch hostname
 -esindex,--esindex <arg>         elasticsearch index name
 -espassword,--espassword <arg>   elasticsearch password
 -esport,--esport <arg>           elasticsearch port
 -esprotocol,--esprotocol <arg>   http or https
 -esquery,--esquery <arg>         elasticsearch query file
 -esusername,--esusername <arg>   elasticsearch username
 -outputfile,--outputfile <arg>   output file path
 -pagesize,--pagesize <arg>       number of documents per shard to get at one time
 -outputfile,--outputfile         path of the output file
	-doclimit,--doclimit             number of documents retrieved, -1 to get trillion
	-outputtype,--outputtype         0 for json array, 1 for json lines
	-runtika,--runtika               0 for no, 1 for yes
	-esurl,--esurl                   url for the es server, should be used instead of esprotocol, esport and eshostname
	-htmlfield,--htmlfield           name of the html field in json which contains raw html

```
The default number of documents retrieved is 100, get more by specifying the parameter "doclimit", set it to -1 to get every document in the index
