A = load 'input.bam' using fi.aalto.seqpig.BamUDFLoader('yes');
A = FOREACH A GENERATE read, flags, refname, start, cigar, basequal, mapqual, attributes;
A = FILTER A BY (flags/4)%2==0;
B = SAMPLE A $samplevar;
C = FOREACH B GENERATE ReadPileup(read, flags, refname, start, cigar, basequal, attributes#'MD', mapqual );
D = FOREACH C GENERATE flatten($0);
E = GROUP D BY (chr, pos) PARALLEL 4;
F = FOREACH E { G = FOREACH D GENERATE refbase, pileup, qual; GENERATE group.chr, group.pos, PileupOutputFormatting(G); }
F = ORDER F BY chr, pos PARALLEL 4;
G = FOREACH F GENERATE chr, pos, flatten($2);
store G into '$outputfile' using PigStorage('\t');
