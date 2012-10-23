--   filter_mappability.pig: filter reads in bam file based on mappability 
--
%default min_map_qual 0
%default threshold 90
%default regionfile /user/root/mappability.100kbp.txt
--
--   macro definitions
--
--   mappability filter
DEFINE MapFilter MappabilityFilter('$threshold');
--
--   filter reads based on flags (unmapped or duplicates) and mapping quality
--
DEFINE filter_reads_unmapdupl(A, min_map_qual) RETURNS B {
        $B = FILTER A BY (flags/4)%2==0 and (flags/1024)%2==0 and mapqual>=$min_map_qual and MapFilter(refindex, start, end);
};
--
--   place mappability data and samfileheader into distributed cache
SET mapred.cache.files hdfs://hadoop-master:9000$inputfile.asciiheader#input_asciiheader,hdfs://hadoop-master:9000$regionfile#input_regionfile
SET mapred.create.symlink yes
--
--   start of script
--
--   import BAM file
A = load '$inputfile' using BamUDFLoader('yes');
--   filter reads based on flags (unmapped or duplicates) and mapping quality
B = filter_reads_unmapdupl(A, $min_map_qual);
--   filter reads based on mappability data
C = FILTER B BY MapFilter(refindex, start, end);
--   write output to HDFS
store C into '$outputfile' using BamUDFStorer('$inputfile.asciiheader');
