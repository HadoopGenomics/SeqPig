#!/bin/bash

# testing seqpig scripts to check whether all is set up correctly

function test_sorting() {
        cat > /tmp/test.reads <<EOF
SRR062634.4310012	99	20	59993	37	14S69M17S	=	60062	168	AAGTTAATAGAGAGGTGACTCAGATCCAGAGGTGGAAGAGGAAGGAAGCTTGGAACCCTATAGAGTTGCTGAGGGCCAGGACCAGATCCTGGCCCTAAAC	0ED@FFGFFEFEFDCEEFDFEFEFFEEFEFEEAEEBCEFEDBBEDAFEB>GEFCDBB&B36.6*.:-6,:76*%"017;6A8/:?;7:9:@?<;9B@A><	MD:Z:0N0N0N0N0N0N0N0N51T9	RG:Z:SRR062634	AM:i:37	NM:i:9	SM:i:37	XN:i:8	BQ:Z:@@@@@@@@@@@@@@NPOPMNLKF@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:37	XT:Z:M
SRR062634.4310012	147	20	60062	37	100M	=	59993	-168	AGATCCTGGCCCTAAACAGGTGGTAAGGAAGGAGAGAGTGAAGGAACTGCCAGGTGACACACTCCCACCATGGACCTCTGGGATCCTAGCTTTAAGAGAT	?6<;>;15;4?/@AACCB@ABBA=CC7ACDBECACACACABCEEDBCDEEECECDECEEECEFEBEAEEEFEECEEFEFEEEEFEEEFEEFDEFFDFDE0	X0:i:1	X1:i:0	MD:Z:100	RG:Z:SRR062634	AM:i:37	NM:i:0	SM:i:37	BQ:Z:@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:37	XT:Z:U
SRR062634.2213824	163	20	6845719	60	100M	=	6845864	190	CATCATTTCCATAATATTTACAAAAAGCAGAGCAACGAAAGGGCTTTCTACTTGAATCAAAATGATGTACCAAAGACCAATCTGTTTTCAGGATGTGGCA	0EEDEEFFDEFEEFEEEFFECFFDFFEEFEFEEDFC=FFFB2BBCFFBFECFFCFCCEFEGEFECFCBFDCDCCFCAFECCBDFCCHFFC?CBAAABAA:	X0:i:1	X1:i:0	MD:Z:100	RG:Z:SRR062634	AM:i:37	NM:i:0	SM:i:37	BQ:Z:@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:60	XT:Z:U
SRR062634.2330476	147	20	21099624	60	100M	=	21099546	-177	AAATAAATGGTACTTATTATGCAAACCCTTTGGTTTTAACCTGCCTGCGAATTGTAAGAACAGTCCTAGAGCGTCCCTTTGTTTCCAACCCTTGTAGTTA	=A??A>;?:1>=B7?>>@A??@@>;?@@@>?@>A>A?=@BBCBBB?B9@BACA?=FCABBAC@BBAACACB;@BCEEBFDCFBFECFEEEEFECEFCFB0	X0:i:1	X1:i:0	MD:Z:100	RG:Z:SRR062634	AM:i:37	NM:i:0	SM:i:37	BQ:Z:@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:60	XT:Z:U
SRR062634.5869679	147	20	30758670	60	100M	=	30758589	-180	CACACTCTTTTCTAAGGTGGTGTATCATTTTATGTTACCATCAGCAGTATACAAGAGTGCCAGTTCCTTTACATCCTAATAACACTTGGTATGGTTAATA	@?A@A@A?@.BBABDCA0CAECD<EABC?BB@B:B7@CB?ABA=BC@>BBBC=DCD?B>BAD?DD>CBDCABA;BBCDCCB>A>@>C@A8BCBADBCBB0	X0:i:1	X1:i:0	MD:Z:100	RG:Z:SRR062634	AM:i:37	NM:i:0	SM:i:37	BQ:Z:@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:60	XT:Z:U
SRR062634.1465399	99	20	30758672	60	100M	=	30758764	191	CACTCTTTTCTAAAGTGGTGTATCATTTTATGTTACCATCAGCAGTATACAAGAGTGCCAGTTCCTTTACATCCTAATAACACTTGGTATGGTTAATATT	0EBEEFFFFEDCB\$C>@A=@ECEEFFFFFFEEEFECEFEEFEEFEBECFCFGCFEBEEFFFCEEFGGEBBGFCDEFADBCEGEGCFDCAEFCBCACBADA	X0:i:1	X1:i:0	MD:Z:13G86	RG:Z:SRR062634	AM:i:37	NM:i:1	SM:i:37	BQ:Z:@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@FF	MQ:i:60	XT:Z:U
SRR062634.1465399	147	20	30758764	60	100M	=	30758672	-191	TTAATATTTTTAATTTTAGCCATTCTTACAGGTATATAACATCTTGTGATTTTAATTTCTATTTCCCTAATGACATGTTGCACATCTTTTCATGTGCTTA	EDBECEEDEHGDGHHHGDFGFFGGFGFFEDBDFFCFFGEECDEFFCFCFFFDEFEFFFEFCDFFEECEFEFECEEFCFFEEEEAFEFFFFDEECEEDCE0	X0:i:1	X1:i:0	MD:Z:100	RG:Z:SRR062634	AM:i:37	NM:i:0	SM:i:37	BQ:Z:C@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:60	XT:Z:U
SRR062634.3234936	163	20	37484140	60	100M	=	37484233	162	ATGACTTTCTTCTCCATTTGCTGCCCACGGACGCCGAAAATAACTTGTTTTCTGGCAAGCTTGGGATGCGATTCCTTATGATTTAATCAGATCCATAATG	0BBAADCABCDBDBCCCDDCCDCCCCDA;DBA2AC;BB@DCC@@CDC?BB>4CB=9@AAA@A5AB>>>'8=.>;4BA?@C2AAB<?@*@?>=<'>??*6@	X0:i:1	X1:i:0	MD:Z:68A31	RG:Z:SRR062634	AM:i:37	NM:i:1	SM:i:37	BQ:Z:@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:60	XT:Z:U
SRR062634.3234936	83	20	37484233	60	30S70M	=	37484140	-162	GGATGAGATTCCTTATGATTTAATGAGATCCATAATGAAGCTCTCCCAGCTTTCAGGAGAAAACATCCTGAACTGCAGGGCTGGTTAAGGGCAGGGCCCT	###############################>4>:0;=:1;6=?3;;=;9<:;>?>A@=B?=>B>>BA?<B>A?>C>B@AC>C>D<EDEEBEFECA?:>/	X0:i:1	X1:i:0	XC:i:70	MD:Z:70	RG:Z:SRR062634	AM:i:37	NM:i:0	SM:i:37	BQ:Z:@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:60	XT:Z:U
SRR062634.6381597	99	20	44350574	60	100M	=	44350656	181	AAATAGCACCTGGAATTAAGTAGTACTCTAGGTTATAACTAACCCTGTTACTATTATTATAGTGCTATTGCACTCTTTCTCTTGTGCCATGATTAGTTTC	0EEECEEFCEFEFEFFFFFEEFECFDFEFFEEEFEEFFCFFGDEEFBEGFDFFFFFFGFDFFBCEGFFCECBBDBDCCBDDDDCCCCCDBCBACABB@@@	X0:i:1	X1:i:0	MD:Z:100	RG:Z:SRR062634	AM:i:37	NM:i:0	SM:i:37	BQ:Z:@JG@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:60	XT:Z:U
SRR062634.6381597	147	20	44350656	60	100M	=	44350574	-181	TGTGCCATGATTAGTTTCTTCTGTGCATAAAAGGTCAAAAAGAGAAAGGAAGAAATCCCTTTTTAATTCCTCGCTCTGATGATTCTACCCTTCATGATTA	B?BAB?ACBCCCFBDDCCDCFCA@FDFBGCCCBACBCGGDFEFAFDFCEFFEFFABEEEFFFFEFEFFEEF=EEFEFEEFEEFFEEAEEEFFDEEDEFE0	X0:i:1	X1:i:0	MD:Z:72A27	RG:Z:SRR062634	AM:i:37	NM:i:1	SM:i:37	BQ:Z:@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:60	XT:Z:U
SRR062634.6307212	99	20	57951956	60	100M	=	57952073	216	GTAGCTGAGAAACTTGCCTAAGATCACATTGAACATTGTCTGCAAAGGAGATCAGGGGAGCATTCTGTGAATGTCCTCAGTGATGGACGGCCTGGACGCG	0@DDEFEFECFFCEFECEFFFEFFEFDFFFEDFCFEFEEEFEEFFCEFDECBAFCCAFCFEDBCAGBBBE@CBAABCBDCACBBCCBA6?C@BAA@=9A9	X0:i:1	X1:i:0	MD:Z:100	RG:Z:SRR062634	AM:i:37	NM:i:0	SM:i:37	BQ:Z:@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@	MQ:i:60	XT:Z:U
EOF
	echo TEST: importing ${SEQPIG_HOME}/data/input.bam
	${SEQPIG_HOME}/bin/prepareBamInput.sh ${SEQPIG_HOME}/data/input.bam
	$HADOOP fs -rmr input_sorted.bam		
	echo TEST: starting sorting
	${SEQPIG_HOME}/bin/seqpig -param inputfile=input.bam -param outputfile=input_sorted.bam ${SEQPIG_HOME}/scripts/sort_bam.pig
	echo TEST: exporting
	${SEQPIG_HOME}/bin/prepareBamOutput.sh input_sorted.bam
	if [ ! -f input_sorted.bam ]; then
    		echo TEST: sorting bam test failed, could not find output file
		exit 1
	fi
	echo TEST: ok
	echo TEST: importing sorted file
	${SEQPIG_HOME}/bin/prepareBamInput.sh input_sorted.bam
	cat > /tmp/convert_reads.pig <<EOF
A = load 'input_sorted.bam' using BamUDFLoader('yes');
store A into 'output.sam' using SamUDFStorer('input_sorted.bam.asciiheader');
EOF
	echo TEST: converting sorted bam to sam
	$HADOOP fs -rmr output.sam
	${SEQPIG_HOME}/bin/seqpig /tmp/convert_reads.pig
	echo TEST: exporting sam
	${SEQPIG_HOME}/bin/prepareSamOutput.sh output.sam
	if [ ! -f output.sam ]; then
                echo TEST: bam to sam conversion failed, could not find output file
                exit 1
        fi
	echo TEST: ok
	echo TEST: comparing output
	grep ^SRR output.sam > /tmp/test.reads2
	cmp -s /tmp/test.reads /tmp/test.reads2 > /dev/null
	if [ $? -eq 1 ]; then
    		echo TEST: results differ!
		exit 1
	else
    		echo TEST: ok
	fi
	rm /tmp/test.reads /tmp/test.reads2 output.sam input_sorted.bam input_sorted.bam.asciiheader
	echo TEST: sorting and bam to sam conversion test passed!
}

if [ -z "${SEQPIG_HOME}" ]; then
        SEQPIG_HOME="`dirname $(readlink -f $0)`/../"
fi

source "${SEQPIG_HOME}/bin/seqpigEnv.sh"

test_sorting
