%default min_map_qual '0'
%default min_base_qual '0'
DEFINE filteredReadPileup ReadPileup('$min_base_qual');
--
-- start of script
--
--   import BAM file
A = load '$inputfile' using BamUDFLoader('yes');
--   filter reads based on flags (unmapped or duplicates) and mapping quality
B = FILTER A BY (flags/4)%2==0 and (flags/1024)%2==0 and mapqual>=$min_map_qual;
--   generate pileup data for each read (one record per position)
C = FOREACH B GENERATE filteredReadPileup(read, flags, refname, start, cigar, basequal, attributes#'MD', mapqual), start, flags, name;
--   filter out incomplete records (see ReadPileup.java for more information)
C = FILTER C BY $0 is not null;
--   form the union of all (read, position) values
D = FOREACH C GENERATE flatten($0), start, flags, name;
--   group all records by refname and position
E = GROUP D BY (chr, pos) PARALLEL $pparallel;
--   for each of the reference position, generate samtools-like pileup text output
F = FOREACH E { G = FOREACH D GENERATE refbase, pileup, qual, start, (flags/16)%2, name; G = ORDER G BY start, $4, name; GENERATE group.chr, group.pos, PileupOutputFormatting(G, group.pos); }
--   since the previous operations do not necessarily produce output in the same order as samtools, sort output
F = ORDER F BY chr, pos PARALLEL $pparallel;
--   break up records into simple tuples and write these to HDFS
G = FOREACH F GENERATE chr, pos, flatten($2);
store G into '$outputfile' using PigStorage('\t');
