#!/bin/bash

# script for adding a header and footer to bam file created by BamUDFStorer

if [ $# -lt 1 ]
then
        echo "error: usage $0 <outputfile.bam>"
        exit 0
fi

if [ -z "${SEQPIG_HOME}" ]; then
        SEQPIG_HOME="`dirname $(readlink -f $0)`/../"
fi

source "${SEQPIG_HOME}/bin/seqpigEnv.sh"

bamoutputfilename=$(readlink -f $1);
baminputfilename=$(basename $1);

rm -f $bamoutputfilename

${HADOOP} jar ${SEQPIG_HOME}/lib/hadoop-bam-${HADOOP_BAM_VERSION}.jar -libjars ${SEQPIG_HOME}/lib/picard-${SAM_VERSION}.jar,${SEQPIG_HOME}/lib/sam-${SAM_VERSION}.jar cat "file://${bamoutputfilename}" "hdfs:///user/${USER}/$baminputfilename/part-r-*"

if [ -e "$bamoutputfilename" ]
then
        echo "writing to file $bamoutputfilename";

        if [ -e "${SEQPIG_HOME}/data/bgzf-terminator.bin" ]
        then
		echo "adding terminator!! (disable if you encounter problems)"
                cat ${SEQPIG_HOME}/data/bgzf-terminator.bin >> $bamoutputfilename
        else
                echo "error: cannot find bgzf-terminator.bin"
        fi
else
        echo "error: could not find $bamoutputfilename!";
fi

if [ -e ".${baminputfilename}.crc" ]
then
        echo "removing (now incorrect) hadoop checksum to allow later import";
        rm -f .${baminputfilename}.crc
fi
