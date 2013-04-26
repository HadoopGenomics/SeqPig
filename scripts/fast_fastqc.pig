
-- parameters:
--  * inputpath
--  * outputpath
--  * parallelism (optional)

-- Needs datafu for the variance function: https://github.com/linkedin/datafu
-- Needs piggybank for the string LENGTH

%default parallelism 1
set default_parallel $parallelism;

-- speculative execution isn't helping us much, so turn it off
set mapred.map.tasks.speculative.execution false;
set mapred.reduce.tasks.speculative.execution false;

-- Use in-memory aggregation, as possible
set pig.exec.mapPartAgg true;
set pig.exec.mapPartAgg.minReduction 5;

-- new experimental Pig feature -- generates specialized typed classes for the tuples
set pig.schematuple on;

define VAR datafu.pig.stats.VAR();
-- Calculate min, the 25th, 50th, 75th percentiles, and the max
define Quantile datafu.pig.stats.Quantile('5');
-- define StreamingQuantile datafu.pig.stats.StreamingQuantile('5');
define STRLEN org.apache.pig.piggybank.evaluation.string.LENGTH();

--
--   start of script
--

--   import reads
reads = load '$inputpath' using FastqLoader();
reads_by_bases = FOREACH reads GENERATE UnalignedReadSplit(sequence, quality);

------- read stats

-- read length
read_len = FOREACH reads GENERATE STRLEN(sequence);
read_len_counts = FOREACH (GROUP read_len BY $0) GENERATE group AS len, COUNT_STAR($1) as count;

-- per sequence avg base quality
-- read_q = FOREACH reads_by_bases GENERATE ROUND(AVG($0.basequal)) as read_qual;
-- read_q_counts = FOREACH (GROUP read_q BY read_qual) GENERATE group as avg_read_qual, COUNT_STAR($1) as count;

read_seq_qual = FOREACH reads GENERATE quality;
avgbase_qual_counts = FOREACH (GROUP read_seq_qual ALL) GENERATE AvgBaseQualCounts($1.$0);
formatted_avgbase_qual_counts = FOREACH avgbase_qual_counts GENERATE FormatAvgBaseQualCounts($0);

-- per sequence GC content
read_gc = FOREACH reads_by_bases {
  only_gc = FILTER $0 BY readbase == 'G' OR readbase == 'C';
  GENERATE COUNT_STAR(only_gc) as count;
}
read_gc_counts = FOREACH (GROUP read_gc BY count) GENERATE group as gc_count, COUNT_STAR($1) as count;


------- base stats

read_seq_qual = FOREACH reads GENERATE sequence, quality;
base_qual_counts = FOREACH (GROUP read_seq_qual ALL) GENERATE BaseCounts($1.$0), BaseQualCounts($1.$1);
formatted_base_qual_counts = FOREACH base_qual_counts GENERATE FormatBaseCounts($0), FormatBaseQualCounts($1);

------- generate output

STORE read_len_counts INTO '$outputpath/read_len_counts';
-- STORE read_q_counts INTO '$outputpath/read_q_counts';
STORE formatted_avgbase_qual_counts INTO '$outputpath/read_q_counts';
STORE read_gc_counts INTO '$outputpath/read_gc_counts';
STORE formatted_base_qual_counts INTO '$outputpath/base_qual_counts';
-- per base GC and N content can be generated from the counts above
