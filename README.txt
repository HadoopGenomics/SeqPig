SeqPig is a library of import and export functions for file formats commonly
used in bioinformatics for Apache Pig. Currently it supports BAM/SAM, FastQ
and Qseq input and output. It is built on top of the Hadoop-BAM library.

A) Setup instructions:

    Install Hadoop and Pig (tested with Hadoop 0.20.2 and Pig 0.10.0.
    Set HADOOP_HOME and PIG_HOME to the installation directories of
    Hadoop and Pig, respectively, and SEQPIG_HOME to the installtion
    directory of SeqPig. On a Cloudera Hadoop installation with
    a local installation of the most recent Pig release, this would be
    done for example by

    export HADOOP_HOME=/usr/lib/hadoop
    export PIG_HOME=/root/pig-0.10.0
    export SEQPIG_HOME=/root/seqpig 

    To make life simpler also add the directory ${SEQPIG_HOME}/bin to your
    PATH:

    export PATH=${PATH}:${SEQPIG_HOME}/bin

B) Instructions for building SeqPig.jar

 B1. Download hadoop-bam-5.0 from
    
	https://sourceforge.net/projects/hadoop-bam/

 B2. Download and compile the latest biodoop/seal git master version from

	http://biodoop-seal.sourceforge.net/

     (requires to set HADOOP_BAM to the installation directory of hadoop-bam)

 B3. Inside the cloned git repository ($SEQPIG_HOME), create a lib/ subdirectory
    and copy the following jar files contained in the
    hadoop-bam release to this location:
 
    seal.jar	hadoop-bam-5.0.jar	sam-1.76.jar	picard-1.76.jar

    Note: the Picard and Sam jar files are contained in the hadoop-bam release
    for convenience.

 B4. Run ant to build SeqPig.jar.

C) Usage:

 C1. Using the pig grunt shell for interactive operations (assumes pig is in your
   PATH); inside the seqpig repository execute:

    pig -Dpig.additional.jars=lib/hadoop-bam-5.0.jar:build/jar/SeqPig.jar:lib/seal.jar:lib/picard-1.76.jar:lib/sam-1.76.jar -Dudf.import.list=fi.aalto.seqpig

    Note: for convenience it may be best to add the following entry to your .bashrc:

    alias pig='${PIG_HOME}/bin/pig -Dpig.additional.jars=${SEQPIG_HOME}/lib/hadoop-bam-5.0.jar:${SEQPIG_HOME}/build/jar/SeqPig.jar:${SEQPIG_HOME}/lib/seal.jar:${SEQPIG_HOME}/lib/picard-1.76.jar:${SEQPIG_HOME}/lib/sam-1.76.jar -Dudf.import.list=fi.aalto.seqpig' 

 C2. Alternatively to using the Pig grunt shell (which can lead to delays due
   to Hadoop queuing and exectution delays), users can write scripts that are
   then submitted to Pig/Hadoop for execution. This type of exectution has the
   advantage of being able to handle parameters, for example for input and oputput
   files. See /scripts inside the seqpig directory and the examples below.

D) Examples for operations on BAM files:

  All examples assume that an input BAM file is initially imported to HDFS via

    prepareBamInput.sh input.bam

  and then loaded in the grunt shell via

    grunt> A = load 'input.bam' using BamUDFLoader('yes');

  (the 'yes' chooses read attributes to be loaded; choose 'no' whenever these
  are not required)

  Once some operations have been performed, the resulting (modified) read
  data can then be stored into a new BAM file via

    grunt> store A into 'output.bam' using BamUDFStorer('input.bam.asciiheader');

  and can also be exported from HDFS to the local filesystem via

    prepareBamOutput.sh output.bam

  (note: the Pig store operation requires a valid header for the BAM output file,
  for example the header of the source file used to generate it, which is
  generated automatically by the prepareBamInput.sh script used to import it)

  Note that dumping the BAM data to the screen (similarly to samtools view)
  can be done simply by

   grunt> dump A;

  Another very useful command is describe, which returns the schema that Pig
  uses for a given data bag. Example:

   grunt> describe A;

  which returns for BAM data
            
  A: {name: chararray,start: int,end: int,read: chararray,cigar: chararray,
   basequal: chararray,flags: int,insertsize: int,mapqual:int,matestart: int,
   materefindex: int,refindex: int,refname: chararray,attributes: map[]}

  Note that all fields except the attributes are standard data types (strings
  or integers). Specific attributes can be accessed via attributes#'name', for
  example

   grunt> B = FOREACH A GENERATE name, attributes#'MD';
   grunt> dump B;

  will output all read names and their corresponding MD tag.

  Another useful command is LIMIT and SAMPLE, which can be used for example for obtaining
  a subset of reads from a BAM/SAM file which can be useful for debugging.

   grunt> B = LIMIT A 20;

  will assign the first 20 records of A to B, while

   grunt> B = SAMPLE A 0.01;

  will sample from A with sampling probability 0.01.

 D1. Filtering out unmapped reads and PCR or optical duplicates:

    grunt> A = FILTER A BY (flags/4)%2==0 and (flags/1024)%2==0;

 D2. Filtering out reads with low mapping quality:

    grunt> A = FILTER A BY mapqual > 19;

 D3. Filtering by regions (samtools syntax):

    grunt> DEFINE myFilter CoordinateFilter('input.bam.asciiheader','20:0-44350673');
    grunt> B = FILTER A BY myFilter(refindex,start,end);

  Note that filtering by regions requires a valid ascii header for mapping
  sequence names to sequence indices.

 D4. Sorting BAM input file by chromosome, reference start coordinate, strand
  and readname (in this hierarchical order):

    grunt> A = FOREACH A GENERATE name, start, end, read, cigar, basequal, flags, insertsize,
mapqual, matestart, materefindex, refindex, refname, attributes, (flags/16)%2;
    grunt> A = ORDER A BY refname, start, $14, name;

  NOTE: this is roughly equivalent to executing from the command line:

    pig -param inputfile=input.bam -param outputfile=input_sorted.bam ${SEQPIG_HOME}/scripts/sort_bam.pig

 D5. Computing read coverage over reference-coordinate bins of a fixed size,
  for example:

    grunt> B = GROUP A BY start/200;
    grunt> C = FOREACH B GENERATE group, COUNT(A);
    grunt> dump C; 

   will output the number of reads that lie in any non-overlapping bin of size
   200 base pairs.

 D6. Computing base frequencies (counts) for each reference coordinate:

    grunt> A = FOREACH A GENERATE read, flags, refname, start, cigar, mapqual;
    grunt> A = FILTER A BY (flags/4)%2==0;
    grunt> RefPos = FOREACH A GENERATE ReadRefPositions(read, flags, refname, start, cigar), mapqual;
    grunt> flatset = FOREACH RefPos GENERATE flatten($0), mapqual;
    grunt> grouped = GROUP flatset BY ($0, $1, $2);
    grunt> base_counts = FOREACH grouped GENERATE group.chr, group.pos, group.base, COUNT(flatset);
    grunt> base_counts = ORDER base_counts BY chr,pos;
    grunt> store base_counts into 'input.basecounts';

  NOTE: this is roughly equivalent to executing from the command line:

    pig -param inputfile=input.bam -param outputfile=input.basecounts -param pparallel=1 ${SEQPIG_HOME}/scripts/basefreq.pig 

 D7. Generating samtools compatible pileup (for a correctly sorted BAM file
   with MD tags aligned to the same reference, should produce the same output as
   samtools mpileup -f ref.fasta -B input.bam):

    grunt> A = load 'input.bam' using BamUDFLoader('yes');
    grunt> B = FILTER A BY (flags/4)%2==0 and (flags/1024)%2==0;
    grunt> C = FOREACH B GENERATE ReadPileup(read, flags, refname, start, cigar,
      basequal, attributes#'MD', mapqual), start, flags, name;
    grunt> D = FOREACH C GENERATE flatten($0), start, flags, name;
    grunt> E = GROUP D BY (chr, pos);
    grunt> F = FOREACH E { G = FOREACH D GENERATE refbase, pileup, qual, start,
      (flags/16)%2, name; G = ORDER G BY start, $4, name; GENERATE group.chr,
      group.pos, PileupOutputFormatting(G, group.pos); }
    grunt> F = ORDER F BY chr, pos;
    grunt> G = FOREACH F GENERATE chr, pos, flatten($2);
    grunt> store G into 'input.pileup' using PigStorage('\t');

   NOTE: this is equivalent to executing from the command line:

    pig -param inputfile=input.bam -param outputfile=input.pileup -param pparallel=1 ${SEQPIG_HOME}/scripts/pileup.pig

  For more examples see also the wiki of two past COST hackathons:

  http://seqahead.cs.tu-dortmund.de/meetings:fastqpigscripting
  http://seqahead.cs.tu-dortmund.de/meetings:2012-05-hackathon:pileuptask
  http://seqahead.cs.tu-dortmund.de/meetings:2012-05-hackathon:seqpig_life_savers_page

E) Other supported file formats

 Besides BAM files, seqpig also supports the uncompressed file format SAM for
 aligned sequence data. For raw read data seqpig supports both FastQ and Qseq
 input and output. Loading and storing data follows along the same lines as
 for BAM.

F) Further comments

 For performance reasons it is typically advisable to enable compression of
 Hadoop map (and possible reduce) output, as well as temporary data generated
 by Pig. The details depend on which compression codecs are used, but it can
 be enabled by passing parameters along the lines of

  -Djava.library.path=/opt/hadoopgpl/native/Linux-amd64-64
  -Dpig.tmpfilecompression=true -Dpig.tmpfilecompression.codec=lzo
  -Dmapred.output.compress=true
  -Dmapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec

 to the pig command. Note that currently not all Hadoop compression codecs are
 supported by Pig.
