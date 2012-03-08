
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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.io.Text;

import fi.tkk.ics.hadoop.bam.BAMInputFormat;
import fi.tkk.ics.hadoop.bam.BAMRecordReader;
//import fi.tkk.ics.hadoop.bam.SplittingBAMIndex;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMRecord;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMTextHeaderCodec;
import fi.tkk.ics.hadoop.bam.FileVirtualSplit;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMReadGroupRecord;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMProgramRecord;

import net.sf.samtools.SAMFileReader.ValidationStringency;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
//import net.sf.samtools.util.StringLineReader;
import java.io.StringWriter;

import it.crs4.seal.common.FastqInputFormat;
import it.crs4.seal.common.FastqInputFormat.FastqRecordReader;
import it.crs4.seal.common.SequencedFragment;

public class FastqUDFLoader extends LoadFunc {
    protected RecordReader in = null;
    //private byte fieldDel = '\t';
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
//	private boolean loadAttributes;
    //private static final int BUFFER_SIZE = 1024;

    public FastqUDFLoader() {
	//	loadAttributes = false;
	//	System.out.println("BamUDFLoader: ignoring attributes");
        Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
        p.setProperty("seal.fastq-input.base-quality-encoding", "illumina");
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
	    Text fastqrec_name = ((FastqRecordReader)in).getCurrentKey();
            SequencedFragment fastqrec = ((FastqRecordReader)in).getCurrentValue();
	    
	    //String value = samrec.toString();
            //byte[] buf = (new Text(value)).getBytes();
            //int len = value.getLength();
            //int start = 0;
	    //if(value.length() == 0)
	    //mProtoTuple.add(null);
	    //else
	    //mProtoTuple.add(new DataByteArray(buf, 0, value.length()));
	    mProtoTuple.add(new String(fastqrec_name.toString()));
	    mProtoTuple.add(new String(fastqrec.getSequence().toString()));
  	    mProtoTuple.add(new String(fastqrec.getQuality().toString()));

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
}
