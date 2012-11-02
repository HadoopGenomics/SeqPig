--   clip_reads.pig: import a given fastq file, clip a specified number of leading and trailing based and their qualities
-- 	             and write out to output file
--
%default frontclip 0
%default backclip 0
--
--   definitions and settings
--
--   set base quality encoding (note: setting may depend on data source!)
SET hbam.fastq-input.base-quality-encoding illumina
--
DEFINE SUBSTRING org.apache.pig.piggybank.evaluation.string.SUBSTRING();
DEFINE LENGTH org.apache.pig.piggybank.evaluation.string.LENGTH();
--
--   start of script
--
--   import FASTQ file
A = load '$inputfile' using FastqUDFLoader();
--   clip reads 
B = FOREACH A GENERATE instrument, run_number, flow_cell_id, lane, tile, xpos, ypos, read, qc_passed, control_number, index_sequence, SUBSTRING(sequence, $frontclip, LENGTH(sequence) - $backclip), SUBSTRING(quality, $frontclip, LENGTH(quality) - $backclip);
--   write output to HDFS
store B into '$outputfile' using FastqUDFStorer();
