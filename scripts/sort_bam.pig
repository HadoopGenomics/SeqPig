--
-- start of script: sort given reads by refname, start position, strand, readname
--
--   import BAM file
A = load '$inputfile' using BamUDFLoader('yes');
--   we want to consider strand information in sorting, so we need to generate the corresponding flag
B = FOREACH A GENERATE name, start, end, read, cigar, basequal, flags, insertsize, mapqual, matestart, materefindex, refindex, refname, attributes, (flags/16)%2;
--   do the actual sorting
C = ORDER B BY refname, start, $14, name;
--   getting rid of the last field which is not needed anymore
D = FOREACH C GENERATE name, start, end, read, cigar, basequal, flags, insertsize, mapqual, matestart, materefindex, refindex, refname, attributes;
--   write output to HDFS
store D into '$outputfile' using BamUDFStorer('$inputfile.asciiheader');
