--   base_stats_unaligned.pig: compute per-base statistics for given fastq/qseq file
--
%default pparallel 1
%default number_of_reads 50.0
--
--   start of script
--
--   import reads
reads = load '$inputfile' using FastqLoader();
reads_by_bases = FOREACH reads GENERATE UnalignedReadSplit(sequence, quality);
bases = FOREACH reads_by_bases GENERATE FLATTEN($0);
--   calculate base frequencies based on position inside read
grouped_by_base = GROUP bases BY (pos, readbase) PARALLEL $pparallel;
base_frequencies = FOREACH grouped_by_base GENERATE group.$0 AS basepos, group.$1 AS base, 100.0*COUNT($1)/$number_of_reads AS count;
--   now turn the (basepos, base) frequencies into (basepos, base1..base4) frequencies
freq_by_pos_grouped = FOREACH (GROUP base_frequencies BY basepos PARALLEL $pparallel) {
	tmp1 = ORDER $1 BY base;
	tmp2 = FOREACH tmp1 GENERATE $1, $2;
	GENERATE group AS basepos, tmp2;
}

freq_by_pos_sorted = ORDER freq_by_pos_grouped BY basepos PARALLEL $pparallel;
STORE freq_by_pos_sorted INTO 'output';
