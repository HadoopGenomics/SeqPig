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
	CLASSPATH="${SEQPIG_HOME}/build/jar/SeqPig.jar"
	
	for i in lib/*.jar; do
	CLASSPATH="${CLASSPATH}:${SEQPIG_HOME}/${i}";
	done
fi

echo "CLASSPATH: $CLASSPATH"

if [ -e "$1" ]
then
	bamfilename=`basename $1`
	${HADOOP} fs -put $1 ${bamfilename}

	$JAVA_HOME/bin/java -classpath $CLASSPATH fi.aalto.seqpig.SAMFileHeaderReader $1

	${HADOOP} fs -put ${1}.asciiheader ${bamfilename}.asciiheader
else
	echo "error: input file $1 does not exist"
fi
