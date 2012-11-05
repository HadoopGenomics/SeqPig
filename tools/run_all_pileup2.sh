#!/bin/bash

REPETITIONS=1
BASENAME=batch_pileup2_experiment2
INPUTFILE=input7.bam
REDUCESLOTS=84
SEQPIG_HOME=/root/seqpig
PIG_HOME=/root/pig-0.10.0
REDUCE_MEMORY=3000
MAP_MEMORY=500
CHILD_MEMORY=3000

for i in `seq 1 $REPETITIONS`; do
	OUTPUTFILE=${INPUTFILE}_${BASENAME}_${i}_out;
	COMMAND="${PIG_HOME}/bin/pig -Dpig.additional.jars=${SEQPIG_HOME}/lib/hadoop-bam-5.0.jar:${SEQPIG_HOME}/build/jar/SeqPig.jar:${SEQPIG_HOME}/lib/seal.jar:${SEQPIG_HOME}/lib/picard-1.76.jar:${SEQPIG_HOME}/lib/sam-1.76.jar -Dmapred.job.map.memory.mb=${MAP_MEMORY} -Dmapred.job.reduce.memory.mb=${REDUCE_MEMORY} -Dmapred.child.java.opts=-Xmx${CHILD_MEMORY}M -Dudf.import.list=fi.aalto.seqpig -param inputfile=$INPUTFILE -param outputfile=$OUTPUTFILE -param pparallel=${REDUCESLOTS} ${SEQPIG_HOME}/scripts/pileup2.pig";
	echo "command: $COMMAND";
	/usr/bin/time --format "%E" -o ${BASENAME}.${i}.time ${COMMAND} > ${BASENAME}.${i}.out 2>&1;
	echo "command was: $COMMAND" >> ${BASENAME}.${i}.out;
	/usr/bin/hadoop fs -rmr "$OUTPUTFILE";
done
