--   sort_bam.pig: sort given reads by refname, start position, strand, readname
--
%default min_map_qual 0
%default pparallel 1
--
--   macro definitions
DEFINE ReadUnmapped fi.aalto.seqpig.SAMFlagsFilter('HasSegmentUnmapped');
DEFINE IsDuplicate fi.aalto.seqpig.SAMFlagsFilter('IsDuplicate');
--
--   start of script
--
--   import BAM file
A = load '$inputfile' using BamLoader('yes');
--   filter reads based on flags (unmapped or duplicates) and mapping quality
A = FILTER A BY not ReadUnmapped(flags) and not IsDuplicate(flags) and mapqual>=$min_map_qual;
--   we want to consider strand information in sorting, so we need to generate the corresponding flag
B = FOREACH A GENERATE name, start, end, read, cigar, basequal, flags, insertsize, mapqual, matestart, materefindex, refindex, refname, attributes, (flags/16)%2;
--   do the actual sorting
C = ORDER B BY refname, start, $14, name PARALLEL $pparallel;
--   getting rid of the last field which is not needed anymore
D = FOREACH C GENERATE name, start, end, read, cigar, basequal, flags, insertsize, mapqual, matestart, materefindex, refindex, refname, attributes;
--   write output to HDFS
store D into '$outputfile' using BamStorer('$inputfile.asciiheader');
