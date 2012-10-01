%default min_map_qual '0'
%default min_base_qual '0'
%default binsize '10'
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
C = GROUP B BY start/$binsize;
D = FOREACH C GENERATE BinReadPileup(B);
dump D;
