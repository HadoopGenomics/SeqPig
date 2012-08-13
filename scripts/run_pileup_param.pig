A = load '$inputfile' using fi.aalto.seqpig.BamUDFLoader('yes');
A = FOREACH A GENERATE read, flags, refname, start, cigar, basequal, mapqual, attributes;
B = FILTER A BY (flags/4)%2==0 and (flags/1024)==0;
C = FOREACH B GENERATE ReadPileup(read, flags, refname, start, cigar, basequal, attributes#'MD', mapqual), start;
D = FOREACH C GENERATE flatten($0), start;
E = GROUP D BY (chr, pos) PARALLEL 8;
F = FOREACH E { G = FOREACH D GENERATE refbase, pileup, qual, flag, start; G = ORDER G BY flag, start; GENERATE group.chr, group.pos, PileupOutputFormatting(G, group.pos); }
F = ORDER F BY chr, pos PARALLEL 8;
G = FOREACH F GENERATE chr, pos, flatten($2);
store G into '$outputfile' using PigStorage('\t');
