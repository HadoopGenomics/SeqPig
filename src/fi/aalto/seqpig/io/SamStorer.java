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

import org.apache.pig.StoreFunc;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.Tuple; 
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.impl.util.UDFContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMTextHeaderCodec;
import net.sf.samtools.SAMTagUtil;
import net.sf.samtools.SAMReadGroupRecord;
import net.sf.samtools.SAMProgramRecord;
import net.sf.samtools.SAMFileReader.ValidationStringency;
import net.sf.samtools.util.StringLineReader;

import fi.tkk.ics.hadoop.bam.SAMFormat;
import fi.tkk.ics.hadoop.bam.AnySAMOutputFormat;
import fi.tkk.ics.hadoop.bam.SAMRecordWriter;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import fi.tkk.ics.hadoop.bam.KeyIgnoringSAMRecordWriter;
import fi.tkk.ics.hadoop.bam.KeyIgnoringAnySAMOutputFormat;

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

public class SamStorer extends StoreFunc {
    protected RecordWriter writer = null;
    protected String samfileheader = null;
    protected SAMFileHeader samfileheader_decoded = null;
    private static final int BUFFER_SIZE = 1024;

    protected HashMap<String,Integer> selectedSAMAttributes = null;
    protected HashMap<String,Integer> allSAMFieldNames = null;

    public SamStorer() {
	System.out.println("WARNING: noarg BamStorer() constructor!");
        decodeSAMFileHeader();
    }

    public SamStorer(String samfileheaderfilename) {

	String str = "";
	this.samfileheader = "";

	try {
	    Configuration conf = UDFContext.getUDFContext().getJobConf();
	    
	    // see https://issues.apache.org/jira/browse/PIG-2576
            if(conf == null || conf.get("mapred.task.id") == null) {
                // we are running on the frontend
                decodeSAMFileHeader();
                return;
            }

	    URI uri = new URI(samfileheaderfilename);
            FileSystem fs = FileSystem.get(uri, conf);
	    
	    BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(samfileheaderfilename))));
	    
	    while(true) {
		str = in.readLine();
		
		if(str == null) break;
		else
		    this.samfileheader += str + "\n";
	    }
	    
	    in.close();
	} catch (Exception e) {
	    System.out.println("ERROR: could not read SAM header from file "+samfileheaderfilename);
	    System.out.println("exception was: "+e.toString());
	}

	try {
	    Base64 codec = new Base64();
	    Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
	    
	    ByteArrayOutputStream bstream = new ByteArrayOutputStream();
	    ObjectOutputStream ostream = new ObjectOutputStream(bstream);
	    ostream.writeObject(this.samfileheader);
	    ostream.close();
       
	    String datastr = codec.encodeBase64String(bstream.toByteArray());
	    p.setProperty("samfileheader", datastr);
	} catch (Exception e) {
	    System.out.println("ERROR: Unable to store SAMFileHeader in BamStorer!");
	}

	this.samfileheader_decoded = getSAMFileHeader();
    }

    protected void decodeSAMFileHeader(){
	try {
            Base64 codec = new Base64();
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
            String datastr;

            datastr = p.getProperty("samfileheader");
            byte[] buffer = codec.decodeBase64(datastr);
            ByteArrayInputStream bstream = new ByteArrayInputStream(buffer);
            ObjectInputStream ostream = new ObjectInputStream(bstream);

            this.samfileheader = (String)ostream.readObject();
        } catch (Exception e) {
        }

        this.samfileheader_decoded = getSAMFileHeader();
    }

    @Override
    public void putNext(Tuple f) throws IOException {

	if(selectedSAMAttributes == null || allSAMFieldNames == null) {
	    try {
		Base64 codec = new Base64();
		Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
		String datastr;
	
		datastr = p.getProperty("selectedSAMAttributes");
		byte[] buffer = codec.decodeBase64(datastr);
		ByteArrayInputStream bstream = new ByteArrayInputStream(buffer);
		ObjectInputStream ostream = new ObjectInputStream(bstream);
		
		selectedSAMAttributes =
		    (HashMap<String,Integer>)ostream.readObject();

		datastr = p.getProperty("allSAMFieldNames");
		buffer = codec.decodeBase64(datastr);
		bstream = new ByteArrayInputStream(buffer);
		ostream = new ObjectInputStream(bstream);
		
		allSAMFieldNames =
		    (HashMap<String,Integer>)ostream.readObject();
	    } catch (ClassNotFoundException e) {
		throw new IOException(e);
	    }
	}

	SAMRecordWritable samrecwrite = new SAMRecordWritable();
	SAMRecord samrec = new SAMRecord(samfileheader_decoded);

	int index = getFieldIndex("name", allSAMFieldNames);

	if(index > -1 && DataType.findType(f.get(index)) == DataType.CHARARRAY) {
	    samrec.setReadName((String)f.get(index));
	}

	index = getFieldIndex("start", allSAMFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
	    samrec.setAlignmentStart(((Integer)f.get(index)).intValue());
	}

	index = getFieldIndex("read", allSAMFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.CHARARRAY) {
	    samrec.setReadString((String)f.get(index));
	}

	index = getFieldIndex("cigar", allSAMFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.CHARARRAY) {
	    samrec.setCigarString((String)f.get(index));
	}

	index = getFieldIndex("basequal", allSAMFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.CHARARRAY) {
	    samrec.setBaseQualityString((String)f.get(index));
	}

	index = getFieldIndex("flags", allSAMFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
	    samrec.setFlags(((Integer)f.get(index)).intValue());
	}
	
	index = getFieldIndex("insertsize", allSAMFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
	    samrec.setInferredInsertSize(((Integer)f.get(index)).intValue());
	}
	
	index = getFieldIndex("mapqual", allSAMFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
	    samrec.setMappingQuality(((Integer)f.get(index)).intValue());
	}

	index = getFieldIndex("matestart", allSAMFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
	    samrec.setMateAlignmentStart(((Integer)f.get(index)).intValue());
	}

	index = getFieldIndex("materefindex", allSAMFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
	    samrec.setMateReferenceIndex(((Integer)f.get(index)).intValue());
	}

	index = getFieldIndex("refindex", allSAMFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
	    samrec.setReferenceIndex(((Integer)f.get(index)).intValue());
	}

	index = getFieldIndex("attributes", allSAMFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.MAP) {
	    Set<Map.Entry<String, Object>> set = ((HashMap<String, Object>)f.get(index)).entrySet();

	    for (Map.Entry<String, Object> pairs : set) {
		String attributeName = pairs.getKey();

		samrec.setAttribute(attributeName.toUpperCase(), pairs.getValue());
	    }
	}
	
	samrec.hashCode(); // causes eagerDecode()
	samrecwrite.set(samrec);

	try {
            writer.write(null, samrecwrite);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    private int getFieldIndex(String field, HashMap<String,Integer> fieldNames) {
	if(!fieldNames.containsKey(field)) {
	    System.err.println("Warning: field missing: "+field);
	    return -1;
	}

	return ((Integer)fieldNames.get(field)).intValue();
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {

	selectedSAMAttributes = new HashMap<String,Integer>();
	allSAMFieldNames = new HashMap<String,Integer>();
	String[] fieldNames = s.fieldNames();

	for(int i=0;i<fieldNames.length;i++) {
	    System.out.println("field: "+fieldNames[i]);
	    allSAMFieldNames.put(fieldNames[i], new Integer(i));

	    if(fieldNames[i].equalsIgnoreCase("RG")
	       || fieldNames[i].equalsIgnoreCase("LB")
	       || fieldNames[i].equalsIgnoreCase("PU")
	       || fieldNames[i].equalsIgnoreCase("PG")
	       || fieldNames[i].equalsIgnoreCase("AS")
	       || fieldNames[i].equalsIgnoreCase("SQ")
	       || fieldNames[i].equalsIgnoreCase("MQ")
	       || fieldNames[i].equalsIgnoreCase("NM")
	       || fieldNames[i].equalsIgnoreCase("H0")
	       || fieldNames[i].equalsIgnoreCase("H1")
	       || fieldNames[i].equalsIgnoreCase("H2")
	       || fieldNames[i].equalsIgnoreCase("UQ")
	       || fieldNames[i].equalsIgnoreCase("PQ")
	       || fieldNames[i].equalsIgnoreCase("NH")
	       || fieldNames[i].equalsIgnoreCase("IH")
	       || fieldNames[i].equalsIgnoreCase("HI")
	       || fieldNames[i].equalsIgnoreCase("MD")
	       || fieldNames[i].equalsIgnoreCase("CS")
	       || fieldNames[i].equalsIgnoreCase("CQ")
	       || fieldNames[i].equalsIgnoreCase("CM")
	       || fieldNames[i].equalsIgnoreCase("R2")
	       || fieldNames[i].equalsIgnoreCase("Q2")
	       || fieldNames[i].equalsIgnoreCase("S2")
	       || fieldNames[i].equalsIgnoreCase("CC")
	       || fieldNames[i].equalsIgnoreCase("CP")
	       || fieldNames[i].equalsIgnoreCase("SM")
	       || fieldNames[i].equalsIgnoreCase("AM")
	       || fieldNames[i].equalsIgnoreCase("MF")
	       || fieldNames[i].equalsIgnoreCase("E2")
	       || fieldNames[i].equalsIgnoreCase("U2")
	       || fieldNames[i].equalsIgnoreCase("OQ")) {

		System.out.println("selected attribute: "+fieldNames[i]+" i: "+i);
		selectedSAMAttributes.put(fieldNames[i], new Integer(i));
	    }
	}

	if(!( allSAMFieldNames.containsKey("name")
	      && allSAMFieldNames.containsKey("start")
	      && allSAMFieldNames.containsKey("end")
	      && allSAMFieldNames.containsKey("read")
	      && allSAMFieldNames.containsKey("cigar")
	      && allSAMFieldNames.containsKey("basequal")
	      && allSAMFieldNames.containsKey("flags")
	      && allSAMFieldNames.containsKey("insertsize")
	      && allSAMFieldNames.containsKey("mapqual")
	      && allSAMFieldNames.containsKey("matestart")
	      && allSAMFieldNames.containsKey("materefindex")
	      && allSAMFieldNames.containsKey("refindex")))
	    throw new IOException("Error: Incorrect SAM tuple-field name or compulsory field missing");

	Base64 codec = new Base64();
	Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
	String datastr;

	ByteArrayOutputStream bstream = new ByteArrayOutputStream();
	ObjectOutputStream ostream = new ObjectOutputStream(bstream);
	ostream.writeObject(selectedSAMAttributes);
	ostream.close();
	datastr = codec.encodeBase64String(bstream.toByteArray());
	p.setProperty("selectedSAMAttributes", datastr);

	bstream = new ByteArrayOutputStream();
	ostream = new ObjectOutputStream(bstream);
	ostream.writeObject(allSAMFieldNames);
	ostream.close();
	datastr = codec.encodeBase64String(bstream.toByteArray());
	p.setProperty("allSAMFieldNames", datastr);
    }

    private SAMFileHeader getSAMFileHeader() {
	final SAMTextHeaderCodec codec = new SAMTextHeaderCodec();
	codec.setValidationStringency(ValidationStringency.SILENT);
	return codec.decode(new StringLineReader(this.samfileheader), "SAMFileHeader.clone");
    }

    @Override
    public OutputFormat getOutputFormat() {
	KeyIgnoringAnySAMOutputFormat outputFormat = new KeyIgnoringAnySAMOutputFormat(SAMFormat.SAM);
        outputFormat.setSAMHeader(getSAMFileHeader());
	return outputFormat;
    }

    @Override
    public void prepareToWrite(RecordWriter writer) {
        this.writer = writer;
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        FileOutputFormat.setOutputPath(job, new Path(location));
    }
}
