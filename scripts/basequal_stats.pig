%default min_map_qual 0
%default min_base_qual 0
%default pparallel 1
--   import BAM file
A = load '$inputfile' using BamLoader('yes');
--   filter reads based on flags (unmapped or duplicates) and mapping quality
B = FILTER A BY (flags/4)%2==0 and (flags/1024)%2==0 and mapqual>=$min_map_qual;
--   split reads into entries for each base
C = FOREACH B GENERATE ReadSplit(name,start,read,cigar,basequal,flags,mapqual,refindex,refname,attributes#'MD');
D = FOREACH C GENERATE FLATTEN($0);
--   calculate base frequencies based on position inside read and reference base
base_stats_data = FOREACH D GENERATE basepos, basequal;
base_stats_grouped = GROUP base_stats_data BY (basepos, basequal);
base_stats_grouped_count = FOREACH base_stats_grouped GENERATE group.$0 as basepos, group.$1 AS basequal, COUNT($1) AS qcount;
base_stats_grouped = GROUP base_stats_grouped_count BY basepos;
base_stats = FOREACH base_stats_grouped {
	TMP1 = FOREACH base_stats_grouped_count GENERATE basequal, qcount;
        TMP2 = ORDER TMP1 BY basequal;
        GENERATE group, TMP2;
}
STORE base_stats into '$outputfile';
