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

import fi.tkk.ics.hadoop.bam.FastaInputFormat;
import fi.tkk.ics.hadoop.bam.FastaInputFormat.FastaRecordReader;
import fi.tkk.ics.hadoop.bam.ReferenceFragment;

public class FastaLoader extends LoadFunc implements LoadMetadata {
    protected RecordReader in = null;
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();

    // tuple format:
    //
    //   index_sequence: string (chromosome or contig identifier)
    //   start: int (start position of reference fragment)
    //   sequence: string
    
    public FastaLoader() {}

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

	    Text fastqrec_name = ((FastaRecordReader)in).getCurrentKey();
            ReferenceFragment fastqrec = ((FastaRecordReader)in).getCurrentValue();
	    
	    mProtoTuple.add(fastqrec.getIndexSequence());
	    mProtoTuple.add(fastqrec.getPosition());
	    mProtoTuple.add(fastqrec.getSequence().toString());

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
        return new FastaInputFormat();
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
	s.add(new Schema.FieldSchema("index_sequence", DataType.CHARARRAY));
	s.add(new Schema.FieldSchema("start", DataType.INTEGER));
	s.add(new Schema.FieldSchema("sequence", DataType.CHARARRAY));

        return new ResourceSchema(s);
    }

    @Override
    public String[] getPartitionKeys(String location, Job job) throws IOException { return null; }

    @Override
    public void setPartitionFilter(Expression partitionFilter) throws IOException { }

    @Override
    public ResourceStatistics getStatistics(String location, Job job) throws IOException { return null; }
}
