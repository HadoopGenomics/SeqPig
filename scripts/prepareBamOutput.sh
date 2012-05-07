#!/bin/bash

# script for adding a header and footer to bam file created by BamUDFStorer

bamoutputfilename="$1/part-r-00000";
baminputfilename="$2";

hadoop fs -get /user/hadoop/${1} ${1}

if [ -e "./$bamoutputfilename" ]
then
	echo "writing to file $bamoutputfilename";

	if [ -e "./$baminputfilename" ]
	then
		java -cp lib/hadoop-bam-4.0.jar:lib/sam-1.56.jar fi.tkk.ics.hadoop.bam.util.GetSortedBAMHeader $baminputfilename tmphdr
		cat $bamoutputfilename >> tmphdr
	
		if [ -e "./bgzf-terminator.bin" ]
        	then
			cat ./bgzf-terminator.bin >> tmphdr
			mv tmphdr ${1}.bam
		else
			echo "error: cannot find bgzf-terminator.bin"
		fi	
	else
		echo "error: bam input file $baminputfilename does not exist";
	fi
else
	echo "error: file does not exist: $bamoutputfilename";
fi
