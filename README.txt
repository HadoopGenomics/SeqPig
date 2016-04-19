
SeqPig is a library of import and export functions for file formats
commonly used in bioinformatics for Apache Pig. Additionally, it
provides a collection of Pig user-defined functions (UDF's) that allow
for processing of aligned and unaligned sequence data. Currently
SeqPig supports BAM/SAM, FastQ and Qseq input and output and FASTA
input. It is built on top of the Hadoop-BAM library. Fore more
information see

http://seqpig.sourceforge.net/

and the documentation that comes with the release.

Releases of SeqPig come bundled with Picard/Samtools, which were developed at
the Wellcome Trust Sanger Institute, and Biodoop/Seal, which were developed
at the Center for Advanced Studies, Research and Development in Sardinia. See

http://samtools.sourceforge.net/
http://biodoop-seal.sourceforge.net/

Installation with precompiled Seal library
  > mvn install:install-file -Dfile=lib/seal-0.4.0-with-hadoop-bam-7.4.0.jar -DgroupId=it.crs4 -DartifactId=seal -Dversion=0.4.0-with-hadoop-bam-7.4.0 -Dpackaging=jar -DgeneratePom=true
  > mvn package -DskipTests


