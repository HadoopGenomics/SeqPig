#!/bin/bash

# runs pileup2.pig for a certain number of repetitions for a
# single input file and given memory settings

REPETITIONS=20
BASENAME=batch_pileup2_experiment2_6red_new
INPUTFILE=input8.bam
REDUCE_SLOTS=252
SEQPIG_HOME=/root/seqpig
PIG_HOME=/root/pig-0.10.0
REDUCE_MEMORY=1000
MAP_MEMORY=500
CHILD_MEMORY=1000

for i in `seq 1 $REPETITIONS`; do
	OUTPUTFILE=${INPUTFILE}_${BASENAME}_${i}_out;
	COMMAND="${PIG_HOME}/bin/pig -Dpig.additional.jars=${SEQPIG_HOME}/lib/hadoop-bam-5.0.jar:${SEQPIG_HOME}/build/jar/SeqPig.jar:${SEQPIG_HOME}/lib/seal.jar:${SEQPIG_HOME}/lib/picard-1.76.jar:${SEQPIG_HOME}/lib/sam-1.76.jar -Dmapred.job.map.memory.mb=${MAP_MEMORY} -Dmapred.job.reduce.memory.mb=${REDUCE_MEMORY} -Dmapred.child.java.opts=-Xmx${CHILD_MEMORY}M -Dudf.import.list=fi.aalto.seqpig -param inputfile=$INPUTFILE -param outputfile=$OUTPUTFILE -param pparallel=${REDUCE_SLOTS} ${SEQPIG_HOME}/scripts/pileup2.pig";
	echo "command: $COMMAND";
	/usr/bin/time --format "%E" -o ${BASENAME}.${i}.time ${COMMAND} > ${BASENAME}.${i}.out 2>&1;
	echo "command was: $COMMAND" >> ${BASENAME}.${i}.out;
	/usr/bin/hadoop fs -rmr "$OUTPUTFILE";
done
