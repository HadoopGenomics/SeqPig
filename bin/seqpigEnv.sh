#!/bin/bash

if [ "$HADOOP" = "" ]; then
        HADOOP="$HADOOP_HOME/bin/hadoop"
fi

if [ "$SEQPIG_HOME" = "" ]; then
        SEQPIG_HOME=`dirname $0`
        SEQPIG_HOME="${SEQPIG_HOME}/../"
fi

if [ "$CLASSPATH" = "" ]; then
        CLASSPATH="${SEQPIG_HOME}/build/jar/SeqPig.jar"

        for i in ${SEQPIG_HOME}/lib/*.jar; do
        CLASSPATH="${CLASSPATH}:${i}";
        done
fi
