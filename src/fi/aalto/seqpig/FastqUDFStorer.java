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

import fi.tkk.ics.hadoop.bam.FastqOutputFormat;
import fi.tkk.ics.hadoop.bam.FastqOutputFormat.FastqRecordWriter;
import fi.tkk.ics.hadoop.bam.SequencedFragment;

public class FastqUDFStorer extends StoreFunc {
    protected RecordWriter writer = null;
    protected HashMap<String,Integer> allFastqFieldNames = null;

    // tuple format:
    //
    //   instrument:string
    //   run_number:int
    //   flow_cell_id: string
    //   lane: int
    //   tile: int
    //   xpos: int
    //   ypos: int
    //   read: int
    //   filter: string
    //   control_number: int
    //   index_sequence: string
    //   sequence: string
    //   quality: string (note: we assume that encoding chosen on command line!!!)

    public FastqUDFStorer(){}

    @Override
    public void putNext(Tuple f) throws IOException {

	if(allFastqFieldNames == null) {
	    try {
		BASE64Decoder decode = new BASE64Decoder();
		Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
		String datastr = p.getProperty("allBAMFieldNames");
		byte[] buffer = decode.decodeBuffer(datastr);
		ByteArrayInputStream bstream = new ByteArrayInputStream(buffer);
		ObjectInputStream ostream = new ObjectInputStream(bstream);
		
		allFastqFieldNames =
		    (HashMap<String,Integer>)ostream.readObject();
	    } catch (ClassNotFoundException e) {
		throw new IOException(e);
	    }
	}

	SequencedFragment fastqrec = new SequencedFragment();
	int index;

	index = getFieldIndex("instrument", allFastqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.CHARARRAY) {
	    fastqrec.setInstrument((String)f.get(index));
	}

	index = getFieldIndex("run_number", allFastqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
	    fastqrec.setRunNumber(((Integer)f.get(index)).intValue());
	}

	index = getFieldIndex("flow_cell_id", allFastqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.CHARARRAY) {
	    fastqrec.setFlowcellId((String)f.get(index));
	}

	index = getFieldIndex("lane", allFastqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
	    fastqrec.setLane(((Integer)f.get(index)).intValue());
	}

	index = getFieldIndex("tile", allFastqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
	    fastqrec.setTile(((Integer)f.get(index)).intValue());
	}

	index = getFieldIndex("xpos", allFastqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
	    fastqrec.setXpos(((Integer)f.get(index)).intValue());
	}
	
	index = getFieldIndex("ypos", allFastqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
	    fastqrec.setYpos(((Integer)f.get(index)).intValue());
	}
	
	index = getFieldIndex("read", allFastqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
	    fastqrec.setRead(((Integer)f.get(index)).intValue());
	}

	index = getFieldIndex("filter", allFastqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.CHARARRAY) {
	    fastqrec.setFilterPassed(((String)f.get(index)).equals("N"));
	}

	index = getFieldIndex("control_number", allFastqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
	    fastqrec.setControlNumber(((Integer)f.get(index)).intValue());
	}

	index = getFieldIndex("index_sequence", allFastqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.CHARARRAY) {
	    fastqrec.setIndexSequence((String)f.get(index));
	}

	index = getFieldIndex("sequence", allFastqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.CHARARRAY) {
	    fastqrec.setSequence((String)f.get(index));
	}

	index = getFieldIndex("quality", allFastqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.CHARARRAY) {
	    fastqrec.setQuality((String)f.get(index));
	}

	try {
            writer.write(null, fastqrec);
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

	allFastqFieldNames = new HashMap<String,Integer>();
	String[] fieldNames = s.fieldNames();

	for(int i=0;i<fieldNames.length;i++) {
	    //System.out.println("field: "+fieldNames[i]);
	    allFastqFieldNames.put(fieldNames[i], new Integer(i));
	}

	if(!( allFastqFieldNames.containsKey("instrument")
	      && allFastqFieldNames.containsKey("run_number")
	      && allFastqFieldNames.containsKey("flow_cell_id")
	      && allFastqFieldNames.containsKey("lane")
	      && allFastqFieldNames.containsKey("tile")
	      && allFastqFieldNames.containsKey("xpos")
	      && allFastqFieldNames.containsKey("ypos")
	      && allFastqFieldNames.containsKey("read")
	      && allFastqFieldNames.containsKey("filter")
	      && allFastqFieldNames.containsKey("control_number")
	      && allFastqFieldNames.containsKey("index_sequence")
	      && allFastqFieldNames.containsKey("sequence")
	      && allFastqFieldNames.containsKey("quality")))
	    throw new IOException("Error: Incorrect Fastq tuple-field name or compulsory field missing");

	BASE64Encoder encode = new BASE64Encoder();
	Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
	String datastr;
	//p.setProperty("someproperty", "value");

	ByteArrayOutputStream bstream = new ByteArrayOutputStream();
	ObjectOutputStream ostream = new ObjectOutputStream(bstream);
	ostream.writeObject(allFastqFieldNames);
	ostream.close();
	datastr = encode.encode(bstream.toByteArray());
	p.setProperty("allFastqFieldNames", datastr); //new String(bstream.toByteArray(), "UTF8"));
    }

    @Override
    public OutputFormat getOutputFormat() {
	return new FastqOutputFormat();
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
