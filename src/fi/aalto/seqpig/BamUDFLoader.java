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
import org.apache.pig.LoadMetadata;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.Tuple; 
import org.apache.pig.data.DataByteArray;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.data.DataType;
import org.apache.pig.Expression;
import org.apache.pig.impl.logicalLayer.schema.Schema;
//import org.apache.pig.builtin.TOMAP;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.Text;

import fi.tkk.ics.hadoop.bam.BAMInputFormat;
import fi.tkk.ics.hadoop.bam.BAMRecordReader;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import fi.tkk.ics.hadoop.bam.FileVirtualSplit;

import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMTextHeaderCodec;
import net.sf.samtools.SAMReadGroupRecord;
import net.sf.samtools.SAMProgramRecord;
import net.sf.samtools.SAMFileReader.ValidationStringency;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.io.StringWriter;

public class BamUDFLoader extends LoadFunc implements LoadMetadata {
    protected RecordReader in = null;
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private boolean loadAttributes;

    public BamUDFLoader() {
	loadAttributes = false;
	System.out.println("BamUDFLoader: ignoring attributes");
    }
    
	public BamUDFLoader(String loadAttributesStr) {
	    if(loadAttributesStr.equals("yes"))
		loadAttributes = true;
	    else {
		loadAttributes = false;
		System.out.println("BamUDFLoader: ignoring attributes");
	    }
	}
    
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
            SAMRecord samrec = ((SAMRecordWritable)in.getCurrentValue()).get();
	    
	    mProtoTuple.add(new String(samrec.getReadName()));
	    mProtoTuple.add(new Integer(samrec.getAlignmentStart()));
	    mProtoTuple.add(new Integer(samrec.getAlignmentEnd()));
	    mProtoTuple.add(new String(samrec.getReadString()));
	    mProtoTuple.add(new String(samrec.getCigarString()));
	    mProtoTuple.add(new String(samrec.getBaseQualityString()));
	    mProtoTuple.add(new Integer(samrec.getFlags()));
	    mProtoTuple.add(new Integer(samrec.getInferredInsertSize()));
	    mProtoTuple.add(new Integer(samrec.getMappingQuality()));
	    mProtoTuple.add(new Integer(samrec.getMateAlignmentStart()));
	    // note: we cannt access this field anymore since it is not public inside samtools/picard
	    //mProtoTuple.add(new Integer(samrec.getIndexingBin()));
	    mProtoTuple.add(new Integer(samrec.getMateReferenceIndex()));
	    mProtoTuple.add(new Integer(samrec.getReferenceIndex()));
	    mProtoTuple.add(new String(samrec.getReferenceName()));
	    
	    if(loadAttributes) {
		Map attributes = new HashMap<String, Object>();
		//ArrayList<String> mapProtoTuple = new ArrayList<String>();
		
		final List<SAMRecord.SAMTagAndValue> mySAMAttributes = samrec.getAttributes();
		
		for (final SAMRecord.SAMTagAndValue tagAndValue : mySAMAttributes) {
		    
		    if(tagAndValue.value != null) {
			
			//System.out.println("found tag name: "+tagAndValue.tag);
			
			//mapProtoTuple.add((String)tagAndValue.tag);

			if(tagAndValue.value.getClass().getName().equals("java.lang.Character"))
			  //mapProtoTuple.add(tagAndValue.value.toString());
			  attributes.put(tagAndValue.tag, tagAndValue.value.toString());
			else
			   //if(tagAndValue.value.getClass().getName().equals("java.lang.String"))
			   	//mapProtoTuple.add((String)tagAndValue.value);
			   attributes.put(tagAndValue.tag, tagAndValue.value);
			   //attributes.put(tagAndValue.tag, tagAndValue.value.toString().getBytes());
		    }
		}
		
		mProtoTuple.add(attributes);
		//TOMAP tomap = new TOMAP();
		//mProtoTuple.add(tomap.exec(mTupleFactory.newTupleNoCopy(mapProtoTuple)));
	    }
	    
            Tuple t =  mTupleFactory.newTupleNoCopy(mProtoTuple);
            mProtoTuple = null;
            return t;
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode,
				    PigException.REMOTE_ENVIRONMENT, e);
        }
	
    }
    
    private boolean skipAttributeTag(String tag) {
	return (tag.equalsIgnoreCase("AM")
		|| tag.equalsIgnoreCase("NM")
		|| tag.equalsIgnoreCase("SM")
		|| tag.equalsIgnoreCase("XN")
		|| tag.equalsIgnoreCase("MQ")
		|| tag.equalsIgnoreCase("XT")
		|| tag.equalsIgnoreCase("X0")
		|| tag.equalsIgnoreCase("BQ")
		|| tag.equalsIgnoreCase("X1")
		|| tag.equalsIgnoreCase("XC"));
    }

    @Override
    public InputFormat getInputFormat() {
        return new BAMInputFormat();
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
	s.add(new Schema.FieldSchema("name", DataType.CHARARRAY));
	s.add(new Schema.FieldSchema("start", DataType.INTEGER));
	s.add(new Schema.FieldSchema("end", DataType.INTEGER));
	s.add(new Schema.FieldSchema("read", DataType.CHARARRAY));
	s.add(new Schema.FieldSchema("cigar", DataType.CHARARRAY));
	s.add(new Schema.FieldSchema("basequal", DataType.CHARARRAY));
	s.add(new Schema.FieldSchema("flags", DataType.INTEGER));
	s.add(new Schema.FieldSchema("insertsize", DataType.INTEGER));
	s.add(new Schema.FieldSchema("mapqual", DataType.INTEGER));
	s.add(new Schema.FieldSchema("matestart", DataType.INTEGER));
	//s.add(new Schema.FieldSchema("indexbin", DataType.INTEGER));
	s.add(new Schema.FieldSchema("materefindex", DataType.INTEGER));
	s.add(new Schema.FieldSchema("refindex", DataType.INTEGER));
	s.add(new Schema.FieldSchema("refname", DataType.CHARARRAY));
	s.add(new Schema.FieldSchema("attributes", DataType.MAP));
        return new ResourceSchema(s);
    }

    @Override
    public String[] getPartitionKeys(String location, Job job) throws IOException { return null; }

    @Override
    public void setPartitionFilter(Expression partitionFilter) throws IOException { }

    @Override
    public ResourceStatistics getStatistics(String location, Job job) throws IOException { return null; }
}
