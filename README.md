dig-elasticsearch
=================
<b>Prerequisites</b>:
* Install and Run Elastic Search locally: http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/_installing_elasticsearch.html
* Clone this repository. Let ```<DIG-ES>``` be the directory that clones this repository
* Install the elasticsearch python package
 ```sudo pip install elasticsearch```

<b>Steps to load data into Elastic Search locally.</b>

  1. Download the 100kWebPages.json file from the Hue -             http://karma-dig-service.cloudapp.net:55320/filebrowser/#/user/worker/data-ElasticSearch/100kWebPages.json

  2. In the sense extension of chrome (or elastic search, if you have the Marvel plugin installed locally)

  a. In the server field - http://localhost:9200/ 
  
  b. Copy the contents of the file at the location -           https://github.com/usc-isi-i2/dig-elasticsearch/blob/master/types/webpage/elasticsearch_dig_WebPage_mappingsandsettings.json
  
    into the editor area of sense and send request.
  
   c. This will create an index named 'dig' with  a document type 'WebPage' on your local machine with all the settings and           mappings as we have on the Elastic Search ISI server.
  
  
3 . Load data into your elastic search server

   a. Change directory to ```<DIG-ES>/types/webpage/scripts```
   
   b. Type ```python loadDataElasticSearch.py -h```. This will provide help for the script as below
    ```
    usage: loadDataElasticSearch.py [-h] [-hostname HOSTNAME] [-port PORT] filepath indexname doctype
          positional arguments:
            filepath            json file to be loaded in ElasticSearch
            indexname           desired name of the index in ElasticSearch
            doctype             type of the document to be indexed
          
          optional arguments:
            -h, --help          show this help message and exit
            -hostname HOSTNAME  Elastic Search Server hostname, defaults to 'localhost'
            -port PORT          Elastic Search Server port,defaults to 9200
    ```       
  
    Execute:
    ```
    python loadDataElasticSearch.py $FilePath/100kWebPages.json dig WebPage
    ```
  
    Please note that $FilePath is the path where the json file is stored locally, downloaded in step 1.
    'dig' and 'WebPage' is the name of index and document type as created in the step 2.
    hostname and port default to 'localhost' and 9200 as required in this case but can be specified with the optional               parameters.
