SeqPig is a library of import and export functions for file formats commonly
used in bioinformatics for Apache Pig.

For more examples see also the wiki of two past COST hackathons:

http://seqahead.cs.tu-dortmund.de/meetings:fastqpigscripting
http://seqahead.cs.tu-dortmund.de/meetings:2012-05-hackathon:pileuptask
http://seqahead.cs.tu-dortmund.de/meetings:2012-05-hackathon:seqpig_life_savers_page

A) Setup instructions:

 A1. Set HADOOP_HOME and PIG_HOME to the installation directories of
    Hadoop and Pig, respectively, and SEQPIG_HOME to the installtion
    directory of SeqPig. On a Cloudera Hadoop installation with
    a local installation of the most recent Pig release, this would be
    done for example by

    export HADOOP_HOME=/usr/lib/hadoop
    export PIG_HOME=/root/pig-0.10.0
    export SEQPIG_HOME=/root/seqpig 

 A2. Download hadoop-bam-4.0 from
    https://sourceforge.net/projects/hadoop-bam/

 A3. inside the cloned git repository ($SEQPIG_HOME), create a lib/ subdirectory
    and copy the following jar files contained in the
    hadoop-bam release to this location:
 
    hadoop-bam-4.0.jar  picard-1.56.jar  sam-1.56.jar

    Note: the Picard and Sam jar files are contained in the hadoop-bam release
    for convenience.

 A4. Run ant to build SeqPig.jar.

B) How to start the pig grunt shell for interactive operations (assumes pig is in
your path):

 B1. Inside the seqpig repository execute:

    pig -Dpig.additional.jars=lib/hadoop-bam-4.0.jar:build/jar/SeqPig.jar:lib/seal.jar:lib/picard-1.56.jar:lib/sam-1.56.jar -Dudf.import.list=fi.aalto.seqpig

    Note: for convenience it may be best to add the following entry to your
    .bashrc:

    alias pig='${PIG_HOME}/bin/pig -Dpig.additional.jars=${SEQPIG_HOME}/lib/hadoop-bam-4.0.jar:${SEQPIG_HOME}/build/jar/SeqPig.jar:${SEQPIG_HOME}/lib/seal.jar:${SEQPIG_HOME}/lib/picard-1.56.jar:${SEQPIG_HOME}/lib/sam-1.56.jar -Dudf.import.list=fi.aalto.seqpig' 
 

C) Examples for bam file manipulation inside the grunt shell:

  All examples assume that an input bam file is initially imported to HDFS via

    ${SEQPIG_HOME}/scripts/prepareBamInput.sh input.bam

  and then loaded in the grunt shell via

    grunt> A = load 'input.bam' using fi.aalto.seqpig.BamUDFLoader('yes');

  (the 'yes' chooses read attributes to be loaded; choose 'no' whenever these
  are not required)

  Once some operations have been performed, the resulting (modified) read
  data can then be stored into a new bam file via

    grunt> store A into 'output.bam' using BamUDFStorer('input.bam.asciiheader');

  and can also be exported from HDFS to the local filesystem via

    ./scripts/prepareBamOutput.sh output.bam input.bam

  (note: the export requires the original input bam in order to obtain the
  header of the bam file which is required for writing bam files)

  Note that dumping the bam data to the screen (similarly to samtools view)
  can be done simply by

   grunt> dump A;

  Another very useful command is describe, which returns the schema that Pig
  uses for a given data bag. Example:

   grunt> describe A;

  which returns for bam data
            
  A: {name: chararray,start: int,end: int,read: chararray,cigar:chararray,
   basequal: chararray,flags: int,insertsize: int,mapqual:int,matestart: int,
   indexbin: int,materefindex: int,refindex: int,refname:chararray,attributes: map[]}

  Note that all fields except the attributes are standard data types (strings
  or integers). Specific attributes can be accessed via attributes#'name', for
  example

   grunt> B = FOREACH A GENERATE name, attributes#'MD';
   grunt> dump B;

  will output all read names and their corresponding MD tag.

 C1. Filtering out unmapped reads and PCR or optical duplicates:

    grunt> A = FILTER A BY (flags/4)%2==0 and (flags/1024)%2==0;

 C2. Sorting bam input file by chromosome, reference start coordinate, strand
  and readname (in this hierarchical order):

    grunt> A = FOREACH A GENERATE name, start, end, read, cigar, basequal, flags, insertsize,
mapqual, matestart, indexbin, materefindex, refindex, refname, attributes, (flags/16)%2;
    grunt> A = ORDER A BY refname, start, $15, name;

  (see also scripts/run_input_bam_sorting.pig)

 C3. Computing read coverage over reference-coordinate bins of a fixed size,
  for example:

    grunt> B = GROUP A BY start/200;
    grunt> C = FOREACH B GENERATE group, COUNT(A);
    grunt> dump C; 

   will output the number of reads that lie in any non-overlapping bin of size
   200 base pairs.

 C4. Generating samtools compatible pileup (complete example, see also
     scripts/run_pileup_param.pig):

    grunt> A = load 'input.bam' using fi.aalto.seqpig.BamUDFLoader('yes');
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

