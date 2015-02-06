#!/bin/sh

# can't use unbound variables
set -o nounset
# exit on error
set -o errexit
# when glob doesn't match, don't fallback to literal glob
# shopt -s nullglob



# general
#JARPATH="./es.jar"
#LOADERCLS=edu.isi.dig.elasticsearch.BulkLoadSequenceFile
#BEGINFILE=150
#ENDFILE=798
#BUCKET=hadoop

# DEFAULTS
#BULKSIZE=10000
#NAPTIME=10
#ESPROTOCOL=https

# ISTR ES
#ESPORT=9200
#ESINDEXNAME=dig-latest
#ESHOST=els.istresearch.com
#ESPROTOCOL=https
#NAPTIME=0
#BUCKET=aws

# KD cluster ES
ESHOST=localhost
ESPORT=9200
#ESINDEXNAME=dig-2
#NAPTIME=0
#ESPROTOCOL=http
#BUCKET=hadoop
#FILELOCATION=/user/worker
#echo $#


if [ $# -ne 22 ]; then
	echo "Usage:"
	echo "-j|--jarname		 local path to the jar file"
	echo "-c|--class		 Java class name with namespace"
	echo "-D|--inputdirectory	 if -b=hadoop or oozie, -D should be the absolute path to directory on hdfs. If -b=aws, -D should be the s3 bucket name with trailing '/'. eg : data/files/"
	echo "-b|--bucket 		 aws or hadoop or oozie"
	echo "-s|--bulksize		 bulk size"
	echo "-n|--naptime		 sleep time  for the program"
	echo "-p|--protocol		 http or https"
	echo "-P|--port			 Elasticsearch port, default 9200"
	echo "-i|--index		 Elasticsearch index name"
	echo "-h|--host			 Elasticsearch host name"
	echo "-m|--mappingfile		 Use Mapping file ? Yes or no"
	exit
fi

#read command line arguments

while [[ $# > 1 ]]
do
        key="$1"

        case $key in
                -j|--jarpath)
                        JARPATH="$2"
                        shift
                        ;;
                -c|--class)
                        LOADERCLS="$2"
                        shift
                        ;;
                -D|--inputdirectory)
                        INPUTDIRECTORY="$2"
                        shift
                        ;;
                 -b|--bucket)
                        BUCKET="$2"
                        shift
                        ;;
                 -s|--bulksize)
                        BULKSIZE="$2"
                        shift
                        ;;
                 -n|--naptime)
                        NAPTIME="$2"
                        shift
                        ;;
                 -p|--protocol)
                        ESPROTOCOL="$2"
                        shift
                        ;;
                 -P|--port)
                        ESPORT="$2"
                        shift
                        ;;
                 -i|--index)
                        ESINDEXNAME="$2"
                        shift
                        ;;
                 -h|--host)
                        ESHOST="$2"
                        shift
                        ;;
		 -m|--mappingfile)
                        USEMAPPINGFILE="$2"
                        shift
                        ;;
                *)
                        # unknown option
                        ;;
        esac
        shift
done

#if -m is set to yes, create the index first using the mapping file
if [ $USEMAPPINGFILE == "yes" ]; then
	echo
	echo "curl -k -XDELETE '${ESPROTOCOL}://${ESHOST}:$ESPORT/$ESINDEXNAME' if needed"
	echo

	echo	
	echo "Fetch ES mapping file"	
	echo
	wget https://raw.githubusercontent.com/usc-isi-i2/dig-elasticsearch/master/types/webpage/esMapping-dig-Ads.json -O esMapping-dig-Ads.json

	echo 
	echo "Install mapping file"
	echo "curl -k -XPUT \"${ESPROTOCOL}://${ESHOST}:$ESPORT/$ESINDEXNAME\" -d @esMapping-dig-Ads.json"
	echo
	curl -k -XPUT "${ESPROTOCOL}://${ESHOST}:$ESPORT/$ESINDEXNAME" -d @esMapping-dig-Ads.json
fi

#echo
#echo "Fetch jar"
#echo
#wget https://s3-us-west-2.amazonaws.com/dig-pilotdocker/c1/es.jar -O $JARPATH

if [ $BUCKET == "hadoop" ]; then
	for i in $(hadoop fs -ls $INPUTDIRECTORY | awk '{$2=$2}1'  | cut -d' '  -f8) 
	do
		#echo $i
		hadoop fs -get $i 
		fileName=$(echo $i | rev | cut -d'/' -f1 | rev)
		echo $fileName
		fileSize=$(du -k $fileName | awk '{print $1}')
		#echo $fileSize
		if [ $fileSize -gt 0 ]; then
			java -classpath "$JARPATH" $LOADERCLS --hostname $ESHOST --index $ESINDEXNAME --type WebPage --protocol $ESPROTOCOL --bulksize $BULKSIZE --sleep $NAPTIME --port $ESPORT --filepath $fileName
		fi
		rm $fileName
	done
elif [ $BUCKET == "oozie" ]; then
	hadoop fs -get $JARPATH
        JARFILENAME=$(echo $JARPATH | rev | cut -d'/' -f1 | rev)
	for i in $(hadoop fs -ls $INPUTDIRECTORY | awk '{$2=$2}1'  | cut -d' '  -f8)
        do
		hadoop fs -get $i
		fileName=$(echo $i | rev | cut -d'/' -f1 | rev)
		fileSize=$(du -k $fileName | awk '{print $1}')
		#echo "java -classpath $JARFILENAME $LOADERCLS --hostname $ESHOST --index $ESINDEXNAME --type WebPage --protocol $ESPROTOCOL --bulksize $BULKSIZE --sleep $NAPTIME --port $ESPORT --filepath $fileName"
                if [ $fileSize -gt 0 ]; then 
			java -classpath $JARFILENAME $LOADERCLS --hostname $ESHOST --index $ESINDEXNAME --type WebPage --protocol $ESPROTOCOL --bulksize $BULKSIZE --sleep $NAPTIME --port $ESPORT --filepath $fileName
		fi
                rm $fileName
	done
	 rm $JARFILENAME
elif [ $BUCKET == "aws" ]; then
	for i in $(aws s3 ls s3://$INPUTDIRECTORY |  awk '{$2=$2}1'  | cut -d' '  -f4)
	do
		 echo $i
		 wget -q https://s3-us-west-2.amazonaws.com/$INPUTDIRECTORY$i -O $i
		 #echo "wget " $?
		 fileSize=$(du -k $i | awk '{print $1}')
		 #echo "filesize: " $?
		 if [ $fileSize -gt 0 ]; then
		 	java -classpath "$JARPATH" $LOADERCLS --hostname $ESHOST --index $ESINDEXNAME --type WebPage --protocol $ESPROTOCOL --bulksize $BULKSIZE --sleep $NAPTIME --port $ESPORT --filepath $i
		 fi
		 rm $i
	done
fi


echo "Done!"

