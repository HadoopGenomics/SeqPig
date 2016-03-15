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

package fi.aalto.seqpig.io;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileReader;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.ValidationStringency;

import java.io.*;

public final class SAMFileHeaderReader {
    public static void main(String[] args) throws IOException {
	try {
	    final SAMTextHeaderCodec codec = new SAMTextHeaderCodec();
	    codec.setValidationStringency(ValidationStringency.SILENT);

	    SAMFileHeader header = (new SAMFileReader(new FileInputStream(new File(args[0])))).getFileHeader();

	    final StringWriter stringWriter = new StringWriter();
	    codec.encode(stringWriter, header);

	    BufferedWriter out = new BufferedWriter(new FileWriter(args[0]+".asciiheader"));
	    String datastr = stringWriter.toString();

	    out.write(datastr, 0, datastr.length());
	    out.flush();
	    out.close();
	} catch(IOException e) {
	    System.err.println(e.toString());
	    throw e;
        }
    }
}
