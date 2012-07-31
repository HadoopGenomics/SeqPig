A = load 'input.bam' using fi.aalto.seqpig.BamUDFLoader('yes');
A = FOREACH A GENERATE read, flags, refname, start, cigar, basequal, mapqual, attributes;
A = FILTER A BY (flags/4)%2==0;
B = SAMPLE A $samplevar;
C = FOREACH B GENERATE ReadPileup(read, flags, refname, start, cigar, basequal, attributes#'MD', mapqual ), read, start, cigar, attributes;
describe C;
D = FOREACH C GENERATE flatten($0), read, start, cigar, attributes;
E = GROUP D BY (chr, pos) PARALLEL 4;
describe E;
F = FOREACH E { G = FOREACH D GENERATE refbase, pileup, qual, read, start, cigar, attributes#'MD'; GENERATE group.chr, group.pos, PileupOutputFormatting(G, group.pos); }
F = ORDER F BY chr, pos PARALLEL 4;
G = FOREACH F GENERATE chr, pos, flatten($2);
store G into '$outputfile' using PigStorage('\t');
