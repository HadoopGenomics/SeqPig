
package fi.aalto.seqpig;

import fi.tkk.ics.hadoop.bam.custom.samtools.SAMRecord;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMFileHeader;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMTextHeaderCodec;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMFileReader;

import net.sf.samtools.SAMFileReader.ValidationStringency;
import net.sf.samtools.util.StringLineReader;

import java.io.IOException;
import java.io.StringWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.StringWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;

public final class SAMFileHeaderReader {
    public static void main(String[] args) throws IOException {
	try {
	    final SAMTextHeaderCodec codec = new SAMTextHeaderCodec();
	    codec.setValidationStringency(ValidationStringency.SILENT);
	    //SAMFileHeader header = codec.decode(new StringLineReader(samfileheader), "SAMFileHeader.clone");

	    SAMFileHeader header = (new SAMFileReader(new FileInputStream(new File(args[0])))).getFileHeader();
	    //final SAMTextHeaderCodec codec = new SAMTextHeaderCodec();
	    //codec.setValidationStringency(ValidationStringency.SILENT);

	    final StringWriter stringWriter = new StringWriter();
	    codec.encode(stringWriter, header);

	    BufferedWriter out = new BufferedWriter(new FileWriter(args[0]+".asciiheader"));
	    String datastr = stringWriter.toString();
	    out.write(datastr, 0, datastr.length());
	    out.flush();
	    out.close();

	    //System.out.println(stringWriter.toString());
	} catch(IOException e) {
	    System.err.println(e.toString());
	    throw e;
	}
    }
}
