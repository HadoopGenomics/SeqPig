#!/bin/bash

if [ -z "$1" ]
then
        echo "error: usage $0 <sam_inputfile>"
	exit 0
fi

source "${SEQPIG_HOME}/bin/seqpigEnv.sh"

if [ -e "$1" ]
then
	bamfilename=`basename $1`

	${HADOOP} fs -rmr ${bamfilename} > /dev/null 2>&1
	${HADOOP} fs -rmr ${bamfilename}.asciiheader > /dev/null 2>&1

	${HADOOP} fs -put $1 ${bamfilename}

	$JAVA_HOME/bin/java -classpath ${SEQPIG_JARS}:${CLASSPATH} fi.aalto.seqpig.SAMFileHeaderReader $1

	${HADOOP} fs -put ${1}.asciiheader ${bamfilename}.asciiheader
else
	echo "error: input file $1 does not exist"
fi
