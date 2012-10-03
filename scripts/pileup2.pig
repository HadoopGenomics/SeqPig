%default min_map_qual '0'
%default min_base_qual '0'
%default binsize '100'
DEFINE filteredReadPileup ReadPileup('$min_base_qual');
--
-- start of script: generate samtools-like pileup for given set of reads
--
--   import BAM file
A = load '$inputfile' using BamUDFLoader('yes');
--   filter reads based on flags (unmapped or duplicates) and mapping quality
B = FILTER A BY (flags/4)%2==0 and (flags/1024)%2==0 and mapqual>=$min_map_qual;
B = LIMIT B 50;
--   generate pileup data for each read (one record per position)
C = FOREACH B GENERATE read, flags, refname, start, cigar, basequal, attributes#'MD', mapqual;
D = GROUP C BY start/$binsize;
E = FOREACH D GENERATE BinReadPileup(C,(group+1)*$binsize);
dump E;
