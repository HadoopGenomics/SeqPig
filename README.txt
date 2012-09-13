SeqPig is a library of import and export functions for file formats commonly
used in bioinformatics for Apache Pig.

For more examples see also the wiki of two past COST hackathons:

http://seqahead.cs.tu-dortmund.de/meetings:fastqpigscripting
http://seqahead.cs.tu-dortmund.de/meetings:2012-05-hackathon:pileuptask
http://seqahead.cs.tu-dortmund.de/meetings:2012-05-hackathon:seqpig_life_savers_page

A) Setup instructions:

A1. Set HADOOP_HOME and PIG_HOME to the installation directories of
    hadoop and pig, respectively.

A2. Download hadoop-bam-4.0 from
    https://sourceforge.net/projects/hadoop-bam/

A3. inside the cloned git repository, create a lib/ subdirectory
    and copy the following jar files contained in the
    hadoop-bam release to this location:
 
    hadoop-bam-4.0.jar  picard-1.56.jar  sam-1.56.jar

A4. Run ant to build SeqPig.jar.

B) How to start the pig grunt shell for interactive operations (assumes pig is in
your path):

B1. Inside the seqpig repository execute:

pig -Dpig.additional.jars=lib/hadoop-bam-4.0.jar:build/jar/SeqPig.jar:lib/seal.jar:lib/picard-1.56.jar:lib/sam-1.56.jar -Dudf.import.list=fi.aalto.seqpig

C) Examples for bam file manipulation inside the grunt shell:

  All examples assume that an input bam file is initially imported to HDFS via

    ./scripts/prepareBamInput.sh input.bam

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

 C1. Filtering out unmapped reads and PCR or optical duplicates:

    grunt> A = FILTER A BY (flags/4)%2==0 and (flags/1024)%2==0;

 C2. Sorting bam input file by chromosome, reference start coordinate, strand
  and readname (in this hierarchical order):

    grunt> A = FOREACH A GENERATE name, start, end, read, cigar, basequal, flags, insertsize,
mapqual, matestart, indexbin, materefindex, refindex, refname, attributes, (flags/16)%2;
    grunt> A = ORDER A BY refname, start, $15, name;

  (see also scripts/run_input_bam_sorting.pig)

 C3. Generating samtools compatible pileup (complete example, see also
     scripts/run_pileup_param.pig):

    grunt> A = load 'input.bam' using fi.aalto.seqpig.BamUDFLoader('yes');
    grunt> B = FILTER A BY (flags/4)%2==0 and (flags/1024)%2==0;
    grunt> C = FOREACH B GENERATE ReadPileup(read, flags, refname, start, cigar,
      basequal, attributes#'MD', mapqual), start, flags, name;
    grunt> D = FOREACH C GENERATE flatten($0), start, flags, name;
    grunt> E = GROUP D BY (chr, pos) PARALLEL 8;
    grunt> F = FOREACH E { G = FOREACH D GENERATE refbase, pileup, qual, start,
      (flags/16)%2, name; G = ORDER G BY start, $4, name; GENERATE group.chr,
      group.pos, PileupOutputFormatting(G, group.pos); }
    grunt> F = ORDER F BY chr, pos PARALLEL 8;
    grunt> G = FOREACH F GENERATE chr, pos, flatten($2);
    grunt> store G into 'input.pileup' using PigStorage('\t');

