#!/bin/bash

# runs pileup2.pig for a certain number of repetitions for a
# single input file and given memory settings

REPETITIONS=2
BASENAME=batch_pileup2_experiment2_6red_new
INPUTFILE=input.bam
REDUCE_SLOTS=252
SEQPIG_HOME=/media/data/dev/SeqPig2
PIG_HOME=/media/data/dev/pig-0.15.0
REDUCE_MEMORY=1000
MAP_MEMORY=500
CHILD_MEMORY=1000

for i in `seq 1 $REPETITIONS`; do
	OUTPUTFILE=${INPUTFILE}_${BASENAME}_${i}_out;
	COMMAND="${PIG_HOME}/bin/pig -Dpig.additional.jars=${SEQPIG_HOME}/lib/hadoop-bam-7.3.1.jar:${SEQPIG_HOME}/target/seqpig-1.0-SNAPSHOT.jar:${SEQPIG_HOME}/lib/seal.jar -Dmapred.job.map.memory.mb=${MAP_MEMORY} -Dmapred.job.reduce.memory.mb=${REDUCE_MEMORY} -Dmapred.child.java.opts=-Xmx${CHILD_MEMORY}M -Dudf.import.list=fi.aalto.seqpig:fi.aalto.seqpig.io:fi.aalto.seqpig.pileup::fi.aalto.seqpig.stats -param inputfile=$INPUTFILE -param outputfile=$OUTPUTFILE -param pparallel=${REDUCE_SLOTS} ${SEQPIG_HOME}/scripts/pileup2.pig";
	echo "command: $COMMAND";
	/usr/bin/time --format "%E" -o ${BASENAME}.${i}.time ${COMMAND} > ${BASENAME}.${i}.out 2>&1;
	echo "command was: $COMMAND" >> ${BASENAME}.${i}.out;
    /usr/bin/hadoop fs -rm -r "$OUTPUTFILE";
done
