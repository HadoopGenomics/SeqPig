--   base_stats.pig: compute per-base statistics for given bam file
--
%default min_map_qual 0
%default pparallel 1
--
--   macro definitions
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
A = load '$inputfile' using BamUDFLoader('yes');
--   filter reads based on flags (unmapped or duplicates) and mapping quality
B = filter_reads_unmapdupl(A, $min_map_qual);
--   split reads into entries for each base
C = FOREACH B GENERATE ReadSplit(name,start,read,cigar,basequal,flags,mapqual,refindex,refname,attributes#'MD');
D = FOREACH C GENERATE FLATTEN($0);
--   calculate base frequencies based on position inside read and reference base
base_stats_data = FOREACH D GENERATE refbase, basepos, UPPER(readbase) AS readbase;
base_stats_grouped = GROUP base_stats_data BY (refbase, basepos, readbase);
base_stats_grouped_count = FOREACH base_stats_grouped GENERATE group.$0 AS refbase, group.$1 AS basepos, group.$2 as readbase, COUNT($1) AS bcount;
base_stats_grouped = GROUP base_stats_grouped_count by (refbase, basepos);
base_stats = FOREACH base_stats_grouped {
	TMP1 = FOREACH base_stats_grouped_count GENERATE readbase, bcount;
	TMP2 = ORDER TMP1 BY bcount desc;
	GENERATE group.$0, group.$1, TMP2;
}
STORE base_stats into '$outputfile';
