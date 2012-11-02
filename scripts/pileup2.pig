%default min_map_qual 0
%default min_base_qual '0'
%default pparallel 1
%default binsize 500
%default reads_cutoff 8000
DEFINE filteredReadPileup BinReadPileup('$min_base_qual', '$reads_cutoff');
--
-- start of script: generate samtools-like pileup for given set of reads
--
--   import BAM file
A = load '$inputfile' using BamUDFLoader('yes');
--   filter reads based on flags (unmapped or duplicates), mapping quality and MD tag
B = FILTER A BY (flags/4)%2==0 and (flags/1024)%2==0 and mapqual>=$min_map_qual and attributes#'MD' is not null;
--   generate pileup data for each read (one record per position)
C = FILTER B BY start/$binsize!=end/$binsize;
--
D = FOREACH C GENERATE read, flags, refname, start, cigar, basequal, attributes#'MD', mapqual, name, end/$binsize;
E = FOREACH B GENERATE read, flags, refname, start, cigar, basequal, attributes#'MD', mapqual, name, start/$binsize;
--
F = UNION D, E;
G = GROUP F BY (refname, $9) PARALLEL $pparallel;
--
H = FOREACH G GENERATE BinReadPileup(F,group.$1*$binsize,(group.$1+1)*$binsize);
I = FILTER H BY $0 is not null;
--
J = FOREACH I GENERATE flatten($0);
K = ORDER J BY chr, pos PARALLEL $pparallel;
--
store K into '$outputfile' using PigStorage('\t');
