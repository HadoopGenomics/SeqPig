--   base_stats_unaligned.pig: compute per-base statistics for given fastq/qseq file
--
%default pparallel 1
--
--   start of script
--
--   import reads
reads = load '$inputfile' using FastqUDFLoader();
reads_by_bases = FOREACH reads GENERATE UnalignedReadSplit(sequence, quality);
bases = FOREACH reads_by_bases GENERATE FLATTEN($0);
--   calculate base frequencies based on position inside read
grouped_by_base = GROUP bases BY (pos, readbase) PARALLEL $pparallel;
bases_count = FOREACH grouped_by_base GENERATE group.$0 AS basepos, group.$1 AS base, COUNT($1) AS count;

count_by_pos = FOREACH (GROUP bases BY pos PARALLEL $pparallel) GENERATE group AS pos, COUNT($1) AS total;

bases_total_count = JOIN bases_count BY basepos, count_by_pos BY pos PARALLEL $pparallel;

freq_by_pos = FOREACH bases_total_count GENERATE $0 AS pos, $1 AS base, (100*$2)/$4 AS freq;

freq_by_pos_grouped = FOREACH (GROUP freq_by_pos BY pos PARALLEL $pparallel) {
	tmp1 = ORDER $1 BY base;
	tmp2 = FOREACH tmp1 GENERATE $1, $2;
	GENERATE group AS basepos, tmp2;
}

freq_by_pos_sorted = ORDER freq_by_pos_grouped BY basepos PARALLEL $pparallel;
STORE freq_by_pos_sorted INTO 'output';
