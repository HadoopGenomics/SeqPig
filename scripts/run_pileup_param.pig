A = load '$inputfile' using fi.aalto.seqpig.BamUDFLoader('yes');
B = FILTER A BY (flags/4)%2==0 and (flags/1024)==0;
C = FOREACH B GENERATE ReadPileup(read, flags, refname, start, cigar, basequal, attributes#'MD', mapqual), start, flags;
D = FOREACH C GENERATE flatten($0), start, flags;
E = GROUP D BY (chr, pos) PARALLEL 8;
F = FOREACH E { G = FOREACH D GENERATE refbase, pileup, qual, flag, start, (flags/16)%2; G = ORDER G BY start, $5; GENERATE group.chr, group.pos, PileupOutputFormatting(G, group.pos); }
F = ORDER F BY chr, pos PARALLEL 8;
G = FOREACH F GENERATE chr, pos, flatten($2);
store G into '$outputfile' using PigStorage('\t');
