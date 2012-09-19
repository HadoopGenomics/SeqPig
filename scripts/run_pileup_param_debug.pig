A = load 'input.bam' using fi.aalto.seqpig.BamUDFLoader('yes');
A = FILTER A BY (flags/4)%2==0;
B = SAMPLE A $samplevar;
C = FOREACH B GENERATE ReadPileup(read, flags, refname, start, cigar, basequal, attributes#'MD', mapqual ), read, start, cigar, attributes;
D = FOREACH C GENERATE flatten($0), read, start, cigar, attributes;
E = GROUP D BY (chr, pos) PARALLEL 8;
F = FOREACH E { G = FOREACH D GENERATE refbase, pileup, qual, read, start, cigar, attributes#'MD'; G = ORDER G BY start; GENERATE group.chr, group.pos, PileupOutputFormatting(G, group.pos); }
F = ORDER F BY chr, pos PARALLEL 8;
G = FOREACH F GENERATE chr, pos, flatten($2);
store G into '$outputfile' using PigStorage('\t');
