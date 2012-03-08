
package fi.aalto.seqpig;

import org.apache.hadoop.fs.FSDataInputStream;

import net.sf.samtools.util.BinaryCodec;
import net.sf.samtools.SAMFileReader.ValidationStringency;
import net.sf.samtools.util.StringLineReader;
import net.sf.samtools.util.BlockCompressedInputStream;

import fi.tkk.ics.hadoop.bam.custom.samtools.SAMRecord;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMFileHeader;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMTextHeaderCodec;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMFileReader;
import fi.tkk.ics.hadoop.bam.custom.samtools.BAMRecordCodec;
import fi.tkk.ics.hadoop.bam.custom.samtools.BlockCompressedOutputStream;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMSequenceDictionary;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMSequenceRecord;

import java.io.IOException;
import java.io.StringWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.StringWriter;

public final class AppendSAMFileHeaderWriter {
    public static void main(String[] args) throws IOException {
	try {
	    //final SAMTextHeaderCodec codec = new SAMTextHeaderCodec();
	    //codec.setValidationStringency(ValidationStringency.SILENT);
	    //SAMFileHeader header = codec.decode(new StringLineReader(samfileheader), "SAMFileHeader.clone");

	    FileInputStream in = new FileInputStream(new File(args[0]));
	    SAMFileHeader header = (new SAMFileReader(in)).getFileHeader();
	    in.close();
	    //final SAMTextHeaderCodec codec = new SAMTextHeaderCodec();
	    //codec.setValidationStringency(ValidationStringency.SILENT);


	    //final FSDataInputStream in = new FSDataInputStream(new FileInputStream(new File(args[0])));
	    BAMRecordCodec incodec = new BAMRecordCodec(header);
	    BlockCompressedInputStream bci = new BlockCompressedInputStream(new File(args[1]));
	    //bci.seek(header.getTextHeader().length());
	    bci.seek(0);
	    incodec.setInputStream(bci);

	    BlockCompressedOutputStream bco = new BlockCompressedOutputStream(new File(args[2]));
	    BinaryCodec binaryCodec = new BinaryCodec(bco);
	    BAMRecordCodec outcodec = new BAMRecordCodec(header);
	    outcodec.setOutputStream(bco);

	    writeHeader(binaryCodec, header);

	    int counter = 0;

	    while(bci.available() > 0) {
		System.out.println("reading block #"+counter);
		final SAMRecord r = incodec.decode();
		outcodec.encode(r);
		counter++;
	    }

	    bco.close();
	} catch(IOException e) {
	    throw e;
	}
    }

    // adapted from BAMRecordWriter
    private static void writeHeader(BinaryCodec binaryCodec, final SAMFileHeader header) {
		binaryCodec.writeBytes ("BAM\001".getBytes());
		binaryCodec.writeString(header.getTextHeader(), true, false);

		final SAMSequenceDictionary dict = header.getSequenceDictionary();

		binaryCodec.writeInt(dict.size());
		for (final SAMSequenceRecord rec : dict.getSequences()) {
			binaryCodec.writeString(rec.getSequenceName(), true, true);
			binaryCodec.writeInt   (rec.getSequenceLength());
		}
	}
}
