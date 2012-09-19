// Copyright (c) 2012 Aalto University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

package fi.aalto.seqpig;

import org.apache.hadoop.fs.FSDataInputStream;

import net.sf.samtools.util.BinaryCodec;
import net.sf.samtools.SAMFileReader.ValidationStringency;
import net.sf.samtools.util.StringLineReader;
import net.sf.samtools.util.BlockCompressedInputStream;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMTextHeaderCodec;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.BAMRecordCodec;
import net.sf.samtools.util.BlockCompressedOutputStream;
import net.sf.samtools.SAMSequenceDictionary;
import net.sf.samtools.SAMSequenceRecord;

import java.io.IOException;
import java.io.StringWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.StringWriter;

public final class AppendSAMFileHeaderWriter {
    public static void main(String[] args) throws IOException {
	try {
	    FileInputStream in = new FileInputStream(new File(args[0]));
	    SAMFileHeader header = (new SAMFileReader(in)).getFileHeader();
	    in.close();
	    BAMRecordCodec incodec = new BAMRecordCodec(header);
	    BlockCompressedInputStream bci = new BlockCompressedInputStream(new File(args[1]));
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
