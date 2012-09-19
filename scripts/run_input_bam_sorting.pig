A = load '$inputfile' using BamUDFLoader('yes');
B = FOREACH A GENERATE name, start, end, read, cigar, basequal, flags, insertsize, mapqual, matestart, indexbin, materefindex, refindex, refname, attributes, (flags/16)%2;
C = ORDER B BY refname, start, $15, name;
D = FOREACH C GENERATE name, start, end, read, cigar, basequal, flags, insertsize, mapqual, matestart, indexbin, materefindex, refindex, refname, attributes;
store D into '$outputfile' using BamUDFStorer('$inputfile.asciiheader');
