--   qual_stats_unaligned.pig: compute per-base quality statistics for given fastq/qseq file
--
%default pparallel 1
--
--   start of script
--
--   import reads
reads = load '$inputfile' using FastqLoader();
reads_by_bases = FOREACH reads GENERATE UnalignedReadSplit(sequence, quality);
bases = FOREACH reads_by_bases GENERATE FLATTEN($0);
--   calculate base frequencies based on position inside read
base_stats_grouped = GROUP bases BY pos PARALLEL $pparallel;
base_stats_grouped_avg = FOREACH base_stats_grouped GENERATE group AS basepos,AVG($1.basequal) AS avg_basequal;
base_stats_grouped_avg_sorted = ORDER base_stats_grouped_avg BY basepos PARALLEL $pparallel;
STORE base_stats_grouped_avg_sorted INTO 'output';
