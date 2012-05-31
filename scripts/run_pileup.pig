A = load 'input.bam' using fi.aalto.seqpig.BamUDFLoader('no') AS (name:chararray, start:int, end:int, read:chararray, cigar:chararray, basequal:chararray, flags:int, insertsize:int, mapqual:int, matestart:int, indexbin:int, materefindex:int, refindex:int, refname:chararray);
RefPos = FOREACH A GENERATE udf_refcoord.RefPositions(read, flags, refname, start, cigar);
flatset = FOREACH RefPos GENERATE flatten($0);
grouped = GROUP flatset BY ($0, $1, $2);
base_counts = FOREACH grouped GENERATE group, COUNT(flatset);
store base_counts into 'base_counts';
