#!/bin/bash

# script for adding a header and footer to bam file created by BamUDFStorer

if [ $# -lt 1 ]
then
        echo "error: usage $0 <outputfile.sam>"
        exit 0
fi

source "${SEQPIG_HOME}/bin/seqpigEnv.sh"

bamoutputfilename="$1";

rm -f $bamoutputfilename

${HADOOP} fs -getmerge ${1} ${1}

if [ -e "./$bamoutputfilename" ]
then
	echo "writing to file $bamoutputfilename";

	# NOTE: adding the terminator seems to cause problems
	# when later importing again.
        #if [ -e "${SEQPIG_HOME}/data/bgzf-terminator.bin" ]
        #then
        	#cat ${SEQPIG_HOME}/data/bgzf-terminator.bin >> $bamoutputfilename
        #else
        #        echo "error: cannot find bgzf-terminator.bin"
        #fi
else
        echo "error: could not find $bamoutputfilename in HDFS!";
fi

if [ -e ".${bamoutputfilename}.crc" ]
then
        echo "removing (now incorrect) hadoop checksum to allow later import";
        rm -f .${bamoutputfilename}.crc
fi
