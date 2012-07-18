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

import org.apache.pig.LoadFunc;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.Tuple; 
import org.apache.pig.data.DataByteArray;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.LoadMetadata;
import org.apache.pig.data.DataType;
import org.apache.pig.Expression;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.Text;

import java.io.IOException;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.ArrayList;
import java.io.StringWriter;

import fi.tkk.ics.hadoop.bam.FastqInputFormat;
import fi.tkk.ics.hadoop.bam.FastqInputFormat.FastqRecordReader;
import fi.tkk.ics.hadoop.bam.SequencedFragment;

public class FastqUDFLoader extends LoadFunc implements LoadMetadata {
    protected RecordReader in = null;
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();

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
    
    public FastqUDFLoader() {}

    @Override
    public Tuple getNext() throws IOException {
        try {
	    
	    if (mProtoTuple == null) {
		mProtoTuple = new ArrayList<Object>();
	    }
	    
            boolean notDone = in.nextKeyValue();
            if (!notDone) {
                return null;
            }

	    Text fastqrec_name = ((FastqRecordReader)in).getCurrentKey();
            SequencedFragment fastqrec = ((FastqRecordReader)in).getCurrentValue();
	   
	    //System.out.println("got: " + fastqrec.toString() + "\n"+"key: "+fastqrec_name.toString());
 
	    //mProtoTuple.add(new String(fastqrec_name.toString()));
	    
	    if(fastqrec.getInstrument() == null || fastqrec.getRunNumber() == null || fastqrec.getFlowcellId() == null ||
		fastqrec.getLane() == null || fastqrec.getTile() == null || fastqrec.getXpos() == null ||
		fastqrec.getYpos() == null || fastqrec.getRead() == null || fastqrec.getFilterPassed() == null ||
		fastqrec.getControlNumber() == null || fastqrec.getIndexSequence() == null || fastqrec.getSequence() == null ||
		fastqrec.getQuality() == null) {

		InterruptedException e = new InterruptedException();
		int errCode = 6018;
                String errMsg = "Error while reading Fastq input: check data format! (Casava 1.8?)";
                throw new ExecException(errMsg, errCode,
                                    PigException.REMOTE_ENVIRONMENT, e);

	    }

	    mProtoTuple.add(new String(fastqrec.getInstrument()));
	    mProtoTuple.add(new Integer(fastqrec.getRunNumber()));
	    mProtoTuple.add(new String(fastqrec.getFlowcellId()));
	    mProtoTuple.add(new Integer(fastqrec.getLane()));
	    mProtoTuple.add(new Integer(fastqrec.getTile()));
	    mProtoTuple.add(new Integer(fastqrec.getXpos()));
	    mProtoTuple.add(new Integer(fastqrec.getYpos()));
	    mProtoTuple.add(new Integer(fastqrec.getRead()));
	    
	    if(fastqrec.getFilterPassed().booleanValue())
		mProtoTuple.add(new String("N"));
	    else
		mProtoTuple.add(new String("?"));

	    mProtoTuple.add(new Integer(fastqrec.getControlNumber()));
	    mProtoTuple.add(new String(fastqrec.getIndexSequence()));
	    mProtoTuple.add(new String(fastqrec.getSequence().toString()));
  	    mProtoTuple.add(new String(fastqrec.getQuality().toString()));

            Tuple t =  mTupleFactory.newTupleNoCopy(mProtoTuple);
            mProtoTuple = null;
            return t;
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading Fastq input: check data format! (Casava 1.8?)";
            throw new ExecException(errMsg, errCode,
				    PigException.REMOTE_ENVIRONMENT, e);
        }

    }

    @Override
    public InputFormat getInputFormat() {
        return new FastqInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        in = reader;
    }

    @Override
    public void setLocation(String location, Job job)
	throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public ResourceSchema getSchema(String location, Job job) throws IOException {
       
	Schema s = new Schema();
	s.add(new Schema.FieldSchema("instrument", DataType.CHARARRAY));
	s.add(new Schema.FieldSchema("run_number", DataType.INTEGER));
	s.add(new Schema.FieldSchema("flow_cell_id", DataType.CHARARRAY));
	s.add(new Schema.FieldSchema("lane", DataType.INTEGER));	
	s.add(new Schema.FieldSchema("tile", DataType.INTEGER));
	s.add(new Schema.FieldSchema("xpos", DataType.INTEGER));
	s.add(new Schema.FieldSchema("ypos", DataType.INTEGER));
	s.add(new Schema.FieldSchema("read", DataType.INTEGER));
	s.add(new Schema.FieldSchema("filter", DataType.CHARARRAY));
	s.add(new Schema.FieldSchema("control_number", DataType.INTEGER));
	s.add(new Schema.FieldSchema("index_sequence", DataType.CHARARRAY));
	s.add(new Schema.FieldSchema("sequence", DataType.CHARARRAY));
	s.add(new Schema.FieldSchema("quality", DataType.CHARARRAY));

        return new ResourceSchema(s);
    }

    @Override
    public String[] getPartitionKeys(String location, Job job) throws IOException { return null; }

    @Override
    public void setPartitionFilter(Expression partitionFilter) throws IOException { }

    @Override
    public ResourceStatistics getStatistics(String location, Job job) throws IOException { return null; }
}
