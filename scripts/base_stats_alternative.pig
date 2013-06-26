--   base_stats.pig: compute per-base statistics for given bam file
--
%default min_map_qual 0
%default pparallel 1
--
--   macro definitions
--
DEFINE REVERSE org.apache.pig.piggybank.evaluation.string.Reverse();
DEFINE ReadReverseStrand fi.aalto.seqpig.filter.SAMFlagsFilter('IsReverseComplemented');
--
--   filter reads based on flags (unmapped or duplicates) and mapping quality
--
DEFINE filter_reads_unmapdupl(A, min_map_qual) RETURNS B {
        $B = FILTER A BY (flags/4)%2==0 and (flags/1024)%2==0 and mapqual>=$min_map_qual;
};
--
--   start of script
--
--   import BAM file
read_data = load '$inputfile' using FastqLoader('yes');
--   filter reads based on flags (unmapped or duplicates) and mapping quality
-- B = filter_reads_unmapdupl(A, $min_map_qual);
forward_reads = FILTER read_data BY not ReadReverseStrand(flags);
reverse_reads = FILTER read_data BY ReadReverseStrand(flags);
reverse_reads_orig = FOREACH reverse_reads GENERATE name, start, end, ReverseComplement(read) AS read, cigar, REVERSE(basequal) AS basequal, flags, insertsize, mapqual, matestart, materefindex, refindex, refname, attributes;
reads = UNION forward_reads, reverse_reads_orig;
--   split reads into entries for each base
reads_by_bases = FOREACH reads GENERATE ReadSplit(name,start,read,cigar,basequal,flags,mapqual,refindex,refname,attributes#'MD');
bases = FOREACH reads_by_bases GENERATE FLATTEN($0);
--   calculate base frequencies based on position inside read and reference base
-- base_stats_data = FOREACH D GENERATE refbase, basepos, UPPER(readbase) AS readbase;
-- base_stats_grouped = GROUP base_stats_data BY (refbase, basepos, readbase);
-- base_stats_grouped_count = FOREACH base_stats_grouped GENERATE group.$0 AS refbase, group.$1 AS basepos, group.$2 as readbase, COUNT($1) AS bcount;
-- base_stats_grouped = GROUP base_stats_grouped_count by (refbase, basepos);
-- base_stats = FOREACH base_stats_grouped {
--	TMP1 = FOREACH base_stats_grouped_count GENERATE readbase, bcount;
--	TMP2 = ORDER TMP1 BY bcount desc;
--	GENERATE group.$0, group.$1, TMP2;
-- }
-- STORE base_stats into '$outputfile';
base_stats_data = FOREACH bases GENERATE basepos, basequal; 
base_stats_grouped = GROUP base_stats_data BY basepos;
base_stats_grouped_count = FOREACH base_stats_grouped GENERATE group AS basepos,AVG($1.basequal) AS avg_basequal;
dump base_stats_grouped_count
