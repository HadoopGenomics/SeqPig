A = load 'input.bam' using fi.aalto.seqpig.BamUDFLoader('yes');
A = FOREACH A GENERATE read, flags, refname, start, cigar, basequal, mapqual, attributes;
A = FILTER A BY (flags/4)%2==0;
B = SAMPLE A $samplevar;
C = FOREACH B GENERATE ReadPileup(read, flags, refname, start, cigar, basequal, attributes#'MD' );
D = FOREACH C GENERATE flatten($0);
E = GROUP D BY (chr, pos, refbase);
F = FOREACH E { G = FOREACH D GENERATE pileup, qual; GENERATE group.chr, group.pos, group.refbase, PileupOutputFormatting(G); }
F = ORDER F BY chr, pos;
G = FOREACH F GENERATE chr, pos, refbase, flatten($3);
store G into '$outputfile' using PigStorage('\t');
