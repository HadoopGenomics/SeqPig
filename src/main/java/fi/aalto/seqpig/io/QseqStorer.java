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

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.seqdoop.hadoop_bam.QseqOutputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;

import java.io.*;
import java.util.HashMap;
import java.util.Properties;

public class QseqStorer extends StoreFunc {
    protected RecordWriter writer = null;
    protected HashMap<String,Integer> allQseqFieldNames = null;

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
    //   qc_passed (a.k.a. filter): boolean
    //   control_number: int
    //   index_sequence: string
    //   sequence: string
    //   quality: string (note: we assume that encoding chosen on command line!!!)

    public QseqStorer(){}

    @Override
    public void putNext(Tuple f) throws IOException {

	if(allQseqFieldNames == null) {
	    try {
		//BASE64Decoder decode = new BASE64Decoder();
		Base64 codec = new Base64();
		Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
		String datastr = p.getProperty("allQseqFieldNames");
		//byte[] buffer = decode.decodeBuffer(datastr);
		byte[] buffer = codec.decodeBase64(datastr);
		ByteArrayInputStream bstream = new ByteArrayInputStream(buffer);
		ObjectInputStream ostream = new ObjectInputStream(bstream);
		
		allQseqFieldNames =
		    (HashMap<String,Integer>)ostream.readObject();
	    } catch (ClassNotFoundException e) {
		throw new IOException(e);
	    }
	}

	SequencedFragment fastqrec = new SequencedFragment();
	int index;

	index = getFieldIndex("instrument", allQseqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.CHARARRAY) {
	    fastqrec.setInstrument((String)f.get(index));
	}

	index = getFieldIndex("run_number", allQseqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
	    fastqrec.setRunNumber(((Integer)f.get(index)));
	}

	index = getFieldIndex("flow_cell_id", allQseqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.CHARARRAY) {
	    fastqrec.setFlowcellId((String)f.get(index));
	}

	index = getFieldIndex("lane", allQseqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
	    fastqrec.setLane(((Integer)f.get(index)));
	}

	index = getFieldIndex("tile", allQseqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
	    fastqrec.setTile(((Integer)f.get(index)));
	}

	index = getFieldIndex("xpos", allQseqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
	    fastqrec.setXpos(((Integer)f.get(index)));
	}
	
	index = getFieldIndex("ypos", allQseqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
	    fastqrec.setYpos(((Integer)f.get(index)));
	}
	
	index = getFieldIndex("read", allQseqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
	    fastqrec.setRead(((Integer)f.get(index)));
	}

	index = getFieldIndex("qc_passed", allQseqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.BOOLEAN) {
	    fastqrec.setFilterPassed(((Boolean)f.get(index)));
	}

	index = getFieldIndex("control_number", allQseqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.INTEGER) {
	    fastqrec.setControlNumber(((Integer)f.get(index)));
	}

	index = getFieldIndex("index_sequence", allQseqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.CHARARRAY) {
	    fastqrec.setIndexSequence((String)f.get(index));
	}

	index = getFieldIndex("sequence", allQseqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.CHARARRAY) {
	    fastqrec.setSequence(new Text((String)f.get(index)));
	}

	index = getFieldIndex("quality", allQseqFieldNames);
	if(index > -1 && DataType.findType(f.get(index)) == DataType.CHARARRAY) {
	    fastqrec.setQuality(new Text((String)f.get(index)));
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

	allQseqFieldNames = new HashMap<String,Integer>();
	String[] fieldNames = s.fieldNames();

	for(int i=0;i<fieldNames.length;i++) {
	    //System.out.println("field: "+fieldNames[i]);
	    allQseqFieldNames.put(fieldNames[i], new Integer(i));
	}

	if(!( /*allQseqFieldNames.containsKey("instrument")
	      && allQseqFieldNames.containsKey("run_number")
	      && allQseqFieldNames.containsKey("flow_cell_id")
	      && allQseqFieldNames.containsKey("lane")
	      && allQseqFieldNames.containsKey("tile")
	      && allQseqFieldNames.containsKey("xpos")
	      && allQseqFieldNames.containsKey("ypos")
	      && allQseqFieldNames.containsKey("read")
	      && allQseqFieldNames.containsKey("filter")
	      && allQseqFieldNames.containsKey("control_number")
	      && allQseqFieldNames.containsKey("index_sequence")*/
	      allQseqFieldNames.containsKey("sequence")
	      && allQseqFieldNames.containsKey("quality")))
	    throw new IOException("Error: Incorrect Qseq tuple-field name or compulsory field missing");

	//BASE64Encoder encode = new BASE64Encoder();
	Base64 codec = new Base64();
	Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
	String datastr;
	//p.setProperty("someproperty", "value");

	ByteArrayOutputStream bstream = new ByteArrayOutputStream();
	ObjectOutputStream ostream = new ObjectOutputStream(bstream);
	ostream.writeObject(allQseqFieldNames);
	ostream.close();
	//datastr = encode.encode(bstream.toByteArray());
	datastr = codec.encodeBase64String(bstream.toByteArray());
	p.setProperty("allQseqFieldNames", datastr); //new String(bstream.toByteArray(), "UTF8"));
    }

    @Override
    public OutputFormat getOutputFormat() {
	return new QseqOutputFormat();
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
