#!/bin/bash

# script for adding a header and footer to bam file created by BamUDFStorer

mybasename=`dirname $0`
bamoutputfilename="${1}.bam"
bamheaderfilename="${bamoutputfilename}.asciiheader"

hadoop fs -getmerge ${1} ${bamoutputfilename}
hadoop fs -copyToLocal $2 ${bamheaderfilename}

if [ -e "$bamoutputfilename" ]
then
	echo "writing to file ${bamoutputfilename}";

	if [ -e "${bamheaderfilename}" ]
	then
		cat $bamoutputfilename >> ${bamheaderfilename}
	
		if [ -e "${mybasename}/bgzf-terminator.bin" ]
        	then
			cat ${mybasename}/bgzf-terminator.bin >> ${bamheaderfilename}
			mv ${bamheaderfilename} ${bamoutputfilename}
		else
			echo "error: cannot find bgzf-terminator.bin"
		fi	
	else
		echo "error: header file does not exist";
	fi
else
	echo "error: hadoop fs -getmerge failed";
fi
