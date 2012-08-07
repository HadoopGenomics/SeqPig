#!/bin/bash

# script for adding a header and footer to bam file created by BamUDFStorer

if [ $# -lt 2 ]
then
        echo "error: usage $0 <outputfile.bam> <original_source.bam>"
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

bamoutputfilename="$1";
baminputfilename="$2"; # required for sam file header

${HADOOP} fs -getmerge ${1} ${1}

if [ -e "./$bamoutputfilename" ]
then
        echo "writing to file $bamoutputfilename";

        if [ -e "./$baminputfilename" ]
        then
                $JAVA_HOME/bin/java -classpath $CLASSPATH fi.tkk.ics.hadoop.bam.util.GetSortedBAMHeader $baminputfilename tmphdr
                cat $bamoutputfilename >> tmphdr

                if [ -e "${SEQPIG_HOME}/data/bgzf-terminator.bin" ]
                then
                        cat ${SEQPIG_HOME}/data/bgzf-terminator.bin >> tmphdr
                        mv tmphdr ${1}
                else
                        echo "error: cannot find bgzf-terminator.bin"
                fi
        else
                echo "error: bam input file $baminputfilename does not exist";
        fi
else
        echo "error: file does not exist: $bamoutputfilename";
fi

if [ -e ".${bamoutputfilename}.crc" ]
then
        echo "removing (now incorrect) hadoop checksum to allow later import";
        rm -f .${bamoutputfilename}.crc
fi
