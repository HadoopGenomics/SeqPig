#!/bin/bash

if [ -z "$1" ]
then
        echo "error: usage $0 <bam_inputfile>"
	exit 0
fi

if [ "$HADOOP" = "" ]; then
	HADOOP="$HADOOP_HOME/bin/hadoop"
fi

if [ "$SEQPIG_HOME" = "" ]; then
	SEQPIG_HOME=`dirname $0`
	SEQPIG_HOME="${SEQPIG_HOME}/../"
fi

if [ "$CLASSPATH" = "" ]; then
	CLASSPATH="${SEQPIG_HOME}/build/jar/SeqPig.jar:${SEQPIG_HOME}/lib/sam-1.56.jar:${SEQPIG_HOME}/lib/hadoop-bam-4.0.jar"
fi

if [ -e "$1" ]
then
	bamfilename=`basename $1`

	${HADOOP} fs -rmr ${bamfilename}
        ${HADOOP} fs -rmr ${bamfilename}.asciiheader

	${HADOOP} fs -put $1 ${bamfilename}

	$JAVA_HOME/bin/java -classpath $CLASSPATH fi.aalto.seqpig.SAMFileHeaderReader $1

	${HADOOP} fs -put ${1}.asciiheader ${bamfilename}.asciiheader
else
	echo "error: input file $1 does not exist"
fi
