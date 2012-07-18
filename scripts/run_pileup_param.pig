REGISTER lib/hadoop-bam-4.0.jar ;
REGISTER lib/picard-1.56.jar ;
REGISTER lib/sam-1.56.jar ;
REGISTER lib/seal.jar ;
REGISTER build/jar/SeqPig.jar ;
A = load 'example.bam' using fi.aalto.seqpig.BamUDFLoader('yes') AS (name:chararray, start:int, end:int, read:chararray, cigar:chararray, basequal:chararray, flags:int, insertsize:int, mapqual:int, matestart:int, indexbin:int, materefindex:int, refindex:int, refname:chararray);
A = FOREACH A GENERATE read, flags, refname, start, cigar, basequal, mapqual;
A = FILTER A BY (flags/4)%2==0;
A = SAMPLE A $samplevar;
RefPos = FOREACH A GENERATE fi.aalto.seqpig.ReadRefPositions(read, flags, refname, start, cigar, basequal), mapqual;
flatset = FOREACH RefPos GENERATE flatten($0), mapqual;
describe flatset;
grouped = GROUP flatset BY ($0, $1) PARALLEL $numreducers;
/*B = FOREACH grouped {
   BasesQuals = FOREACH flatset GENERATE base, basequal;
   BasesQualsTuple = fi.aalto.seqpig.PileupOutputFormatting(BasesQuals);
   GENERATE group.chr, group.pos, BasesQuals;
}*/
describe grouped;
B = FOREACH grouped GENERATE group.chr, group.pos, fi.aalto.seqpig.PileupOutputFormatting(flatset);
dump B;
