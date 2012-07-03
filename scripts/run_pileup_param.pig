REGISTER lib/hadoop-bam-4.0.jar ;
REGISTER lib/picard-1.56.jar ;
REGISTER lib/sam-1.56.jar ;
REGISTER lib/seal.jar ;
REGISTER build/jar/SeqPig.jar ;
A = load 'input2.bam' using fi.aalto.seqpig.BamUDFLoader('no') AS (name:chararray, start:int, end:int, read:chararray, cigar:chararray, basequal:chararray, flags:int, insertsize:int, mapqual:int, matestart:int, indexbin:int, materefindex:int, refindex:int, refname:chararray);
A = FOREACH A GENERATE read, flags, refname, start, cigar, mapqual;
A = FILTER A BY (flags/4)%2==0;
A = SAMPLE A $samplevar;
RefPos = FOREACH A GENERATE fi.aalto.seqpig.ReadRefPositions(read, flags, refname, start, cigar), mapqual;
flatset = FOREACH RefPos GENERATE flatten($0), mapqual;
grouped = GROUP flatset BY ($0, $1, $2) PARALLEL 16;
base_counts = FOREACH grouped GENERATE group.chr, group.pos, group.base, COUNT(flatset);
base_counts = ORDER base_counts BY chr,pos PARALLEL 16;
store base_counts into 'base_counts_sorted_$samplevar';
