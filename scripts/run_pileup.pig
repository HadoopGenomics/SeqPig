A = load 'input_large11-175710-176200.bam' using fi.aalto.seqpig.BamUDFLoader('no') AS (name:chararray, start:int, end:int, read:chararray, cigar:chararray, basequal:chararray, flags:int, insertsize:int, mapqual:int, matestart:int, indexbin:int, materefindex:int, refindex:int, refname:chararray);
A = FILTER A BY (flags/1024)%2==0;
RefPos = FOREACH A GENERATE udf_refcoord.RefPositions(read, flags, refname, start, cigar), mapqual;
flatset = FOREACH RefPos GENERATE flatten($0), mapqual;
grouped = GROUP flatset BY ($0, $1, $2) PARALLEL 1;
describe grouped;
base_counts = FOREACH grouped GENERATE group.chr, group.pos, group.base, COUNT(flatset);
base_counts = ORDER base_counts BY chr,pos PARALLEL 1;
store base_counts into 'base_counts_sorted_12';
