
#!/bin/bash

if [ -e "$1" ]
then
	bamfilename=`basename $1`
	hadoop fs -put $1 ${bamfilename}

	java -classpath build/jar/SeqPig.jar:lib/sam-1.56.jar:lib/hadoop-bam-4.0.jar fi.aalto.seqpig.SAMFileHeaderReader $1

	hadoop fs -put ${1}.asciiheader ${bamfilename}.asciiheader
else
	echo "error: input file $1 does not exist"
fi
