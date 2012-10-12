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

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.PigException;
import org.apache.pig.impl.util.UDFContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.RecordWriter; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import fi.tkk.ics.hadoop.bam.BAMOutputFormat;
//import fi.tkk.ics.hadoop.bam.BAMRecordWriter;
//import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
//import fi.tkk.ics.hadoop.bam.KeyIgnoringBAMRecordWriter;
//import fi.tkk.ics.hadoop.bam.KeyIgnoringBAMOutputFormat;
//import fi.tkk.ics.hadoop.bam.custom.samtools.SAMRecord;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMFileHeader;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMTextHeaderCodec;
//import fi.tkk.ics.hadoop.bam.custom.samtools.SAMTagUtil;
//import fi.tkk.ics.hadoop.bam.custom.samtools.SAMReadGroupRecord;
//import fi.tkk.ics.hadoop.bam.custom.samtools.SAMProgramRecord;
import net.sf.samtools.SAMSequenceRecord;

import net.sf.samtools.SAMFileReader.ValidationStringency;
import net.sf.samtools.util.StringLineReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import java.util.regex.Pattern;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.commons.codec.binary.Base64;

public class BamUDFRegionFilter extends FilterFunc {
    //protected RecordWriter writer = null;
    protected String samfileheader = null;
    protected SAMFileHeader samfileheader_decoded = null;
    private static final int BUFFER_SIZE = 1024;


    public BamUDFRegionFilter() {
	//System.out.println("WARNING: noarg BamUDFStorer() constructor!");
        decodeSAMFileHeader();
    }

    public BamUDFRegionFilter(String samfileheaderfilename) {
	String str = "";
	this.samfileheader = "";

	try {
	    Configuration conf = UDFContext.getUDFContext().getJobConf();
	    
	    if(conf == null) {
		decodeSAMFileHeader();
		return;
	    }

            FileSystem fs;
	    
	    try {
		if(FileSystem.getDefaultUri(conf) == null
		   || FileSystem.getDefaultUri(conf).toString() == "")
		    fs = FileSystem.get(new URI("hdfs://"), conf);
		else 
		    fs = FileSystem.get(conf);
	    } catch (Exception e) {
		fs = FileSystem.get(new URI("hdfs://"), conf);
            	System.out.println("WARNING: problems with filesystem config?");
            	System.out.println("exception was: "+e.toString());
	    }
	    
	    BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(fs.getHomeDirectory(), new Path(samfileheaderfilename)))));
	    
	    while(true) {
		str = in.readLine();
		
		if(str == null) break;
		else
		    this.samfileheader += str + "\n";
	    }
	    
	    in.close();
	} catch (Exception e) {
	    System.out.println("ERROR: could not read BAM header from file "+samfileheaderfilename);
	    System.out.println("exception was: "+e.toString());
	}

	try {
	    //BASE64Encoder encode = new BASE64Encoder();
	    Base64 codec = new Base64();
	    Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
	    
	    ByteArrayOutputStream bstream = new ByteArrayOutputStream();
	    ObjectOutputStream ostream = new ObjectOutputStream(bstream);
	    ostream.writeObject(this.samfileheader);
	    ostream.close();
	    //String datastr = encode.encode(bstream.toByteArray());
	    String datastr = codec.encodeBase64String(bstream.toByteArray());
	    p.setProperty("samfileheader", datastr);
	} catch (Exception e) {
	    //throw new IOException(e);
	    System.out.println("ERROR: Unable to store SAMFileHeader in BamUDFStorer!");
	}

	this.samfileheader_decoded = getSAMFileHeader();
    }

    @Override
    public Boolean exec(Tuple input) throws IOException {
        try {

            DataType.findType(f.get(4)) == DataType.CHARARRAY

            Object values = input.get(0);
            if (values instanceof DataBag)
                return ((DataBag)values).size() == 0;
            else if (values instanceof Map)
                return ((Map)values).size() == 0;
            else {
                int errCode = 2102;
                String msg = "Cannot test a " +
                DataType.findTypeName(values) + " for emptiness.";
                throw new ExecException(msg, errCode, PigException.BUG);
            }
        } catch (ExecException ee) {
            throw ee;
        }
    }

    protected void decodeSAMFileHeader(){
	try {
            //BASE64Decoder decode = new BASE64Decoder();
            Base64 codec = new Base64();
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
            String datastr;

            datastr = p.getProperty("samfileheader");
            //byte[] buffer = decode.decodeBuffer(datastr);
            byte[] buffer = codec.decodeBase64(datastr);
            ByteArrayInputStream bstream = new ByteArrayInputStream(buffer);
            ObjectInputStream ostream = new ObjectInputStream(bstream);

            this.samfileheader = (String)ostream.readObject();
        } catch (Exception e) {
        }

        this.samfileheader_decoded = getSAMFileHeader();
    }
    
    private SAMFileHeader getSAMFileHeader() {
	final SAMTextHeaderCodec codec = new SAMTextHeaderCodec();
	codec.setValidationStringency(ValidationStringency.SILENT);
	return codec.decode(new StringLineReader(this.samfileheader), "SAMFileHeader.clone");
    }
}
