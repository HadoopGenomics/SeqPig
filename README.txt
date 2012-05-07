SeqPig is a library of import and export functions for file formats commonly
used in bioinformatics for Apache Pig.

To run the example do the following:

A1. Set HADOOP_HOME and PIG_HOME to the installation directories of
    hadoop and pig, respectively.

A2. Download hadoop-bam-4.0 from
    https://sourceforge.net/projects/hadoop-bam/

A3. inside the cloned git repository, create a lib/ subdirectory
    and copy the following jar files contained in the
    hadoop-bam release to this location:
 
    hadoop-bam-4.0.jar  picard-1.56.jar  sam-1.56.jar

A4. Run ant to build SeqPig.jar.

A5. Download some bam file.
    (for example: NA19240.chrom11.SOLID.bfast.YRI.exome.20111114.bam
    from ftp://ftp.1000genomes.ebi.ac.uk//vol1/ftp/data/NA19240/exome_alignment/

A6. Start the pig grunt shell.

Inside the grunt shell execute the following commands:

B1. Run the following script from inside the grunt shell,
    which generates an ASCII version of the bam header and
    imports the header together with the bam file to HDFS
    (adjust the path to the input.bam accordingly):

    grunt> sh scripts/prepareBamInput.sh /root/input.bam

B2. grunt> exec scripts/setupBamUDF.pig

B3. grunt> exec scripts/run_coverage.pig

You should see output that counts the number of reads intersecting
with a certain genomic position (note that the script only uses
a part of the input bam as input in order to have a quick response
in this interactive mode).
   
