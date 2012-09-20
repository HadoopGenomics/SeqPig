#!/bin/bash

# script for adding a header and footer to bam file created by BamUDFStorer

if [ $# -lt 2 ]
then
        echo "error: usage $0 <outputfile.bam> <original_source.bam>"
        exit 0
fi

source "${SEQPIG_HOME}/bin/seqpigEnv.sh"

bamoutputfilename="$1";
baminputfilename="$2"; # required for sam file header

rm -f $bamoutputfilename

${HADOOP} fs -getmerge ${1} ${1}

if [ -e "./$bamoutputfilename" ]
then
        echo "writing to file $bamoutputfilename";

	# first we need to find the asciiheader: we try: local file, extract from bam local file, HDFS
	# (in that order)
	if [ -e "${baminputfilename}.asciiheader" ]
	then
		echo "using local header file ${baminputfilename}.asciiheader"
	else
		if [ -e "$baminputfilename" ]
        	then
			echo "extracting header ${baminputfilename}.asciiheader from local bam file"

			$JAVA_HOME/bin/java -classpath $CLASSPATH fi.tkk.ics.hadoop.bam.util.GetSortedBAMHeader $baminputfilename ${baminputfilename}.asciiheader
		else
			echo "trying to find header ${baminputfilename}.asciiheader in HDFS"

			${HADOOP} fs -copyToLocal ${baminputfilename}.asciiheader ${baminputfilename}.asciiheader
		fi
	fi

        if [ -e "${baminputfilename}.asciiheader" ]
        then
		cp ${baminputfilename}.asciiheader tmphdr
                cat $bamoutputfilename > tmphdr

                if [ -e "${SEQPIG_HOME}/data/bgzf-terminator.bin" ]
                then
			echo "adding terminator!! (disable if you encounter problems)"
                        cat ${SEQPIG_HOME}/data/bgzf-terminator.bin >> tmphdr
                        mv tmphdr ${1}
                else
                        echo "error: cannot find bgzf-terminator.bin"
                fi
        else
                echo "error: cannot find header file ${baminputfilename}.asciiheader";
        fi
else
        echo "error: could not find $bamoutputfilename in HDFS!";
fi

if [ -e ".${bamoutputfilename}.crc" ]
then
        echo "removing (now incorrect) hadoop checksum to allow later import";
        rm -f .${bamoutputfilename}.crc
fi
