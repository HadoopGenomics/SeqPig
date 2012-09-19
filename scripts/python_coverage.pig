Register 'python/SAMReadStreaming.py' using jython as myfuncs;
B = load '$inputfile' using BamUDFLoader('yes');
C = foreach B generate name, start, end, read, cigar, basequal, flags, insertsize, mapqual, matestart, indexbin, materefindex, refindex, attributes, start / 1000;
D = foreach B generate name, start, end, read, cigar, basequal, flags, insertsize, mapqual, matestart, indexbin, materefindex, refindex, attributes, end / 1000;
E = UNION C, D;
F = group E by $14;
G = foreach F {H = DISTINCT E; H = order H by start; generate group, H; }
I = foreach G generate group, myfuncs.readCoverage(group*1000,(group+1)*1000,'coverageFunc',H);
dump I
