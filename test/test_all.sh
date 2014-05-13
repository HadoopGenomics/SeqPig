#!/bin/bash

# testing seqpig scripts to check whether all is set up correctly

function init_tests() {
	echo TEST: initializing tests
        cat > /tmp/test.reads <<EOF
SRR062634.4310012	99	20	59993	37	14S69M17S	=	60062	168	AAGTTAATAGAGAGGTGACTCAGATCCAGAGGTGGAAGAGGAAGGAAGCTTGGAACCCTATAGAGTTGCTGAGGGCCAGGACCAGATCCTGGCCCTAAAC	0ED@FFGFFEFEFDCEEFDFEFEFFEEFEFEEAEEBCEFEDBBEDAFEB>GEFCDBB&B36.6*.:-6,:76*%"017;6A8/:?;7:9:@?<;9B@A><	MD:Z:0N0N0N0N0N0N0N0N51T9	RG:Z:SRR062634	AM:i:37	NM:i:9	SM:i:37	XN:i:8	BQ:Z:@@@@@@@@@@@@@@NPOPMNLKF@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:37	XT:Z:M
SRR062634.4310012	147	20	60062	37	100M	=	59993	-168	AGATCCTGGCCCTAAACAGGTGGTAAGGAAGGAGAGAGTGAAGGAACTGCCAGGTGACACACTCCCACCATGGACCTCTGGGATCCTAGCTTTAAGAGAT	?6<;>;15;4?/@AACCB@ABBA=CC7ACDBECACACACABCEEDBCDEEECECDECEEECEFEBEAEEEFEECEEFEFEEEEFEEEFEEFDEFFDFDE0	X0:i:1	X1:i:0	MD:Z:100	RG:Z:SRR062634	AM:i:37	NM:i:0	SM:i:37	BQ:Z:@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:37	XT:Z:U
SRR062634.2213824	163	20	6845719	60	100M	=	6845864	190	CATCATTTCCATAATATTTACAAAAAGCAGAGCAACGAAAGGGCTTTCTACTTGAATCAAAATGATGTACCAAAGACCAATCTGTTTTCAGGATGTGGCA	0EEDEEFFDEFEEFEEEFFECFFDFFEEFEFEEDFC=FFFB2BBCFFBFECFFCFCCEFEGEFECFCBFDCDCCFCAFECCBDFCCHFFC?CBAAABAA:	X0:i:1	X1:i:0	MD:Z:100	RG:Z:SRR062634	AM:i:37	NM:i:0	SM:i:37	BQ:Z:@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:60	XT:Z:U
SRR062634.2330476	147	20	21099624	60	100M	=	21099546	-177	AAATAAATGGTACTTATTATGCAAACCCTTTGGTTTTAACCTGCCTGCGAATTGTAAGAACAGTCCTAGAGCGTCCCTTTGTTTCCAACCCTTGTAGTTA	=A??A>;?:1>=B7?>>@A??@@>;?@@@>?@>A>A?=@BBCBBB?B9@BACA?=FCABBAC@BBAACACB;@BCEEBFDCFBFECFEEEEFECEFCFB0	X0:i:1	X1:i:0	MD:Z:100	RG:Z:SRR062634	AM:i:37	NM:i:0	SM:i:37	BQ:Z:@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:60	XT:Z:U
SRR062634.5869679	147	20	30758670	60	100M	=	30758589	-180	CACACTCTTTTCTAAGGTGGTGTATCATTTTATGTTACCATCAGCAGTATACAAGAGTGCCAGTTCCTTTACATCCTAATAACACTTGGTATGGTTAATA	@?A@A@A?@.BBABDCA0CAECD<EABC?BB@B:B7@CB?ABA=BC@>BBBC=DCD?B>BAD?DD>CBDCABA;BBCDCCB>A>@>C@A8BCBADBCBB0	X0:i:1	X1:i:0	MD:Z:100	RG:Z:SRR062634	AM:i:37	NM:i:0	SM:i:37	BQ:Z:@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:60	XT:Z:U
SRR062634.1465399	99	20	30758672	60	100M	=	30758764	191	CACTCTTTTCTAAAGTGGTGTATCATTTTATGTTACCATCAGCAGTATACAAGAGTGCCAGTTCCTTTACATCCTAATAACACTTGGTATGGTTAATATT	0EBEEFFFFEDCB\$C>@A=@ECEEFFFFFFEEEFECEFEEFEEFEBECFCFGCFEBEEFFFCEEFGGEBBGFCDEFADBCEGEGCFDCAEFCBCACBADA	X0:i:1	X1:i:0	MD:Z:13G86	RG:Z:SRR062634	AM:i:37	NM:i:1	SM:i:37	BQ:Z:@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@FF	MQ:i:60	XT:Z:U
SRR062634.1465399	147	20	30758764	60	100M	=	30758672	-191	TTAATATTTTTAATTTTAGCCATTCTTACAGGTATATAACATCTTGTGATTTTAATTTCTATTTCCCTAATGACATGTTGCACATCTTTTCATGTGCTTA	EDBECEEDEHGDGHHHGDFGFFGGFGFFEDBDFFCFFGEECDEFFCFCFFFDEFEFFFEFCDFFEECEFEFECEEFCFFEEEEAFEFFFFDEECEEDCE0	X0:i:1	X1:i:0	MD:Z:100	RG:Z:SRR062634	AM:i:37	NM:i:0	SM:i:37	BQ:Z:C@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:60	XT:Z:U
SRR062634.3234936	163	20	37484140	60	100M	=	37484233	162	ATGACTTTCTTCTCCATTTGCTGCCCACGGACGCCGAAAATAACTTGTTTTCTGGCAAGCTTGGGATGCGATTCCTTATGATTTAATCAGATCCATAATG	0BBAADCABCDBDBCCCDDCCDCCCCDA;DBA2AC;BB@DCC@@CDC?BB>4CB=9@AAA@A5AB>>>'8=.>;4BA?@C2AAB<?@*@?>=<'>??*6@	X0:i:1	X1:i:0	MD:Z:68A31	RG:Z:SRR062634	AM:i:37	NM:i:1	SM:i:37	BQ:Z:@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:60	XT:Z:U
SRR062634.3234936	83	20	37484233	60	30S70M	=	37484140	-162	GGATGAGATTCCTTATGATTTAATGAGATCCATAATGAAGCTCTCCCAGCTTTCAGGAGAAAACATCCTGAACTGCAGGGCTGGTTAAGGGCAGGGCCCT	###############################>4>:0;=:1;6=?3;;=;9<:;>?>A@=B?=>B>>BA?<B>A?>C>B@AC>C>D<EDEEBEFECA?:>/$OPTARG	X0:i:1	X1:i:0	XC:i:70	MD:Z:70	RG:Z:SRR062634	AM:i:37	NM:i:0	SM:i:37	BQ:Z:@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:60	XT:Z:U
SRR062634.6381597	99	20	44350574	60	100M	=	44350656	181	AAATAGCACCTGGAATTAAGTAGTACTCTAGGTTATAACTAACCCTGTTACTATTATTATAGTGCTATTGCACTCTTTCTCTTGTGCCATGATTAGTTTC	0EEECEEFCEFEFEFFFFFEEFECFDFEFFEEEFEEFFCFFGDEEFBEGFDFFFFFFGFDFFBCEGFFCECBBDBDCCBDDDDCCCCCDBCBACABB@@@	X0:i:1	X1:i:0	MD:Z:100	RG:Z:SRR062634	AM:i:37	NM:i:0	SM:i:37	BQ:Z:@JG@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:60	XT:Z:U
SRR062634.6381597	147	20	44350656	60	100M	=	44350574	-181	TGTGCCATGATTAGTTTCTTCTGTGCATAAAAGGTCAAAAAGAGAAAGGAAGAAATCCCTTTTTAATTCCTCGCTCTGATGATTCTACCCTTCATGATTA	B?BAB?ACBCCCFBDDCCDCFCA@FDFBGCCCBACBCGGDFEFAFDFCEFFEFFABEEEFFFFEFEFFEEF=EEFEFEEFEEFFEEAEEEFFDEEDEFE0	X0:i:1	X1:i:0	MD:Z:72A27	RG:Z:SRR062634	AM:i:37	NM:i:1	SM:i:37	BQ:Z:@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:60	XT:Z:U
SRR062634.6307212	99	20	57951956	60	100M	=	57952073	216	GTAGCTGAGAAACTTGCCTAAGATCACATTGAACATTGTCTGCAAAGGAGATCAGGGGAGCATTCTGTGAATGTCCTCAGTGATGGACGGCCTGGACGCG	0@DDEFEFECFFCEFECEFFFEFFEFDFFFEDFCFEFEEEFEEFFCEFDECBAFCCAFCFEDBCAGBBBE@CBAABCBDCACBBCCBA6?C@BAA@=9A9	X0:i:1	X1:i:0	MD:Z:100	RG:Z:SRR062634	AM:i:37	NM:i:0	SM:i:37	BQ:Z:@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:60	XT:Z:U
EOF
}

function cleanup_tests() {
	echo TEST: cleaning up
	echo rm -f /tmp/test.reads /tmp/test.reads2 /tmp/convert_reads.pig output.sam input_sorted.bam input_sorted.bam.asciiheader
	rm -f /tmp/test.reads /tmp/test.reads2 /tmp/convert_reads.pig output.sam input_sorted.bam input_sorted.bam.asciiheader
}

function cleanup_hdfs() {
	echo TEST: cleaning up HDFS
	echo $HADOOP fs -rmr /user/${USER}/input.bam
	$HADOOP fs -rmr /user/${USER}/input.bam
	echo $HADOOP fs -rmr /user/${USER}/input_sorted.bam
	$HADOOP fs -rmr /user/${USER}/input_sorted.bam
	echo $HADOOP fs -rmr /user/${USER}/output.sam
	$HADOOP fs -rmr /user/${USER}/output.sam
}

function cleanup_local() {
	echo TEST: cleaning up local files
	echo rm -rf input_sorted.bam
	rm -rf input_sorted.bam
	echo rm -rf input_sorted
	rm -rf input_sorted
	echo rm -rf output.sam
	rm -rf output.sam
	echo rm -rf output
	rm -rf output
}

function test_sorting_hdfs() {
	echo TEST: importing ${SEQPIG_HOME}/data/input.bam
	echo ${SEQPIG_HOME}/bin/prepareBamInput.sh ${SEQPIG_HOME}/data/input.bam
	${SEQPIG_HOME}/bin/prepareBamInput.sh ${SEQPIG_HOME}/data/input.bam
	echo $HADOOP fs -rmr /user/${USER}/input_sorted.bam
	$HADOOP fs -rmr /user/${USER}/input_sorted.bam		
	echo TEST: starting sorting
	echo ${SEQPIG_HOME}/bin/seqpig -param inputfile=/user/${USER}/input.bam -param outputfile=/user/${USER}/input_sorted.bam ${SEQPIG_HOME}/scripts/sort_bam.pig
	${SEQPIG_HOME}/bin/seqpig -param inputfile=/user/${USER}/input.bam -param outputfile=/user/${USER}/input_sorted.bam ${SEQPIG_HOME}/scripts/sort_bam.pig
	check_output
}

function test_sorting_s3() {
	s3_path=${1}
	echo $HADOOP fs -rmr /user/${USER}/input_sorted.bam
	$HADOOP fs -rmr /user/${USER}/input_sorted.bam
	echo TEST: starting sorting
	echo ${SEQPIG_HOME}/bin/seqpig -param inputfile=s3://${s3_path} -param outputfile=/user/${USER}/input_sorted.bam ${SEQPIG_HOME}/scripts/sort_bam.pig
	${SEQPIG_HOME}/bin/seqpig -param inputfile=s3://${s3_path} -param outputfile=/user/${USER}/input_sorted.bam ${SEQPIG_HOME}/scripts/sort_bam.pig
	check_output
}

function check_output() {
	echo TEST: exporting
	echo ${SEQPIG_HOME}/bin/prepareBamOutput.sh input_sorted.bam
	${SEQPIG_HOME}/bin/prepareBamOutput.sh input_sorted.bam
	if [ ! -f input_sorted.bam ]; then
    		echo TEST: sorting BAM test failed, could not find output file
		cleanup_hdfs
		exit 1
	fi
	echo TEST: ok
	echo TEST: importing sorted file
	${SEQPIG_HOME}/bin/prepareBamInput.sh input_sorted.bam
	cat > /tmp/convert_reads.pig <<EOF
A = load '/user/${USER}/input_sorted.bam' using BamLoader('yes');
store A into '/user/${USER}/output.sam' using SamStorer('/user/${USER}/input_sorted.bam.asciiheader');
EOF
	echo TEST: converting sorted BAM to SAM
	$HADOOP fs -rmr /user/${USER}/output.sam
	${SEQPIG_HOME}/bin/seqpig /tmp/convert_reads.pig
	echo TEST: exporting sam
	${SEQPIG_HOME}/bin/prepareSamOutput.sh output.sam
	if [ ! -f output.sam ]; then
                echo TEST: BAM to SAM conversion failed, could not find output file
		cleanup_hdfs
                exit 1
        fi
	echo TEST: ok
	echo TEST: comparing output
	grep ^SRR output.sam > /tmp/test.reads2
	cmp -s /tmp/test.reads /tmp/test.reads2 > /dev/null
	if [ $? -eq 1 ]; then
    		echo TEST: results differ!
		cleanup_hdfs
		exit 1
	else
    		echo TEST: ok
	fi
	echo TEST: sorting and BAM to SAM conversion test passed!
}

function test_sorting_local() {
	echo rm -rf input_sorted.bam
	rm -rf input_sorted.bam
	echo TEST: starting sorting
	echo ${SEQPIG_HOME}/bin/seqpig -x local -param inputfile=${SEQPIG_HOME}/data/input.bam -param outputfile=input_sorted ${SEQPIG_HOME}/scripts/sort_bam.pig
	${SEQPIG_HOME}/bin/seqpig -x local -param inputfile=${SEQPIG_HOME}/data/input.bam -param outputfile=input_sorted ${SEQPIG_HOME}/scripts/sort_bam.pig
	echo ${HADOOP} jar ${SEQPIG_HOME}/lib/hadoop-bam-${HADOOP_BAM_VERSION}.jar fi.tkk.ics.hadoop.bam.cli.Frontend -libjars ${SEQPIG_LIBJARS} cat --validation-stringency=STRICT "file://$(pwd)/input_sorted.bam" "file://$(pwd)/input_sorted/part-r-*"
	${HADOOP} jar ${SEQPIG_HOME}/lib/hadoop-bam-${HADOOP_BAM_VERSION}.jar fi.tkk.ics.hadoop.bam.cli.Frontend -libjars ${SEQPIG_LIBJARS} cat --validation-stringency=STRICT "file://$(pwd)/input_sorted.bam" "file://$(pwd)/input_sorted/part-r-*"
	if [ ! -f input_sorted.bam ]; then
    		echo TEST: sorting BAM test failed, could not find output file
		cleanup_local
		exit 1
	fi
	echo cp -a ${SEQPIG_HOME}/data/input.bam.asciiheader input_sorted.bam.asciiheader
	cp -a ${SEQPIG_HOME}/data/input.bam.asciiheader input_sorted.bam.asciiheader
	echo TEST: ok
	cat > /tmp/convert_reads.pig <<EOF
A = load '$(pwd)/input_sorted.bam' using BamLoader('yes');
store A into '$(pwd)/output' using SamStorer('$(pwd)/input_sorted.bam.asciiheader');
EOF
	echo TEST: converting sorted BAM to SAM
	${SEQPIG_HOME}/bin/seqpig -x local /tmp/convert_reads.pig
	if [ ! -f $(pwd)/output/part-m-00000 ]; then
                echo TEST: BAM to SAM conversion failed, could not find output file
		cleanup_local
                exit 1
        fi
	echo TEST: ok
	echo TEST: comparing output
	grep ^SRR $(pwd)/output/part-m-00000 > /tmp/test.reads2
	cmp -s /tmp/test.reads /tmp/test.reads2 > /dev/null
	if [ $? -eq 1 ]; then
    		echo TEST: results differ!
		cleanup_local
		exit 1
	else
    		echo TEST: ok
	fi
	echo TEST: sorting and BAM to SAM conversion test passed!
}

if [ -z "${SEQPIG_HOME}" ]; then
        SEQPIG_HOME="`dirname $(readlink -f $0)`/../"
fi

source "${SEQPIG_HOME}/bin/seqpigEnv.sh"

init_tests

test_chosen=2
s3_path=""

while getopts ":hs:l" opt; do
  case $opt in
    h)
      test_chosen=1
      ;;
    l)
      test_chosen=2
      ;;
    s)
      test_chosen=3
      s3_path="$OPTARG"
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      test_chosen=4
      ;;
  esac
done

if [ "$test_chosen" -eq "1" ]; then
      echo "TEST: starting HDFS tests"
      test_sorting_hdfs
      cleanup_hdfs
fi

if [ "$test_chosen" -eq "2" ]; then
      echo "TEST: starting local tests"
      test_sorting_local
      cleanup_local
fi

if [ "$test_chosen" -eq "3" ]; then
      echo "TEST: starting S3 tests"
      test_sorting_s3 "$s3_path"
      cleanup_hdfs
fi

cleanup_tests

echo TEST: all tests passed
