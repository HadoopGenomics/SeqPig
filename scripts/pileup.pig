A = load '$inputfile' using BamUDFLoader('yes');
B = FILTER A BY (flags/4)%2==0 and (flags/1024)%2==0;
C = FOREACH B GENERATE ReadPileup(read, flags, refname, start, cigar, basequal, attributes#'MD', mapqual), start, flags, name;
C = FILTER C BY $0 is not null;
D = FOREACH C GENERATE flatten($0), start, flags, name;
E = GROUP D BY (chr, pos) PARALLEL $pparallel;
F = FOREACH E { G = FOREACH D GENERATE refbase, pileup, qual, start, (flags/16)%2, name; G = ORDER G BY start, $4, name; GENERATE group.chr, group.pos, PileupOutputFormatting(G, group.pos); }
F = ORDER F BY chr, pos PARALLEL $pparallel;
G = FOREACH F GENERATE chr, pos, flatten($2);
store G into '$outputfile' using PigStorage('\t');
