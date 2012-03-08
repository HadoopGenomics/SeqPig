
#!/bin/bash

filename=`basename $1`
mybasename=`dirname $0`

if [ -e "$1" ]
then
	hadoop fs -put $1 $filename

	java -classpath ${mybasename}/build/jar/BamUDF.jar:${mybasename}/lib3.3/sam-1.56.jar:${mybasename}/lib3.3/hadoop-bam-3.4-pre.jar bamudf.SAMFileHeaderReader $1

	hadoop fs -put ${1}.asciiheader ${filename}.asciiheader
else
	echo "error: input file $1 does not exist"
fi
