
package fi.aalto.seqpig;

import org.apache.pig.LoadFunc;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.Tuple; 
import org.apache.pig.data.DataByteArray;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;

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
//import net.sf.samtools.util.StringLineReader;
import java.io.StringWriter;

public class BamUDFLoader extends LoadFunc {
    protected RecordReader in = null;
    //private byte fieldDel = '\t';
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
	private boolean loadAttributes;
    //private static final int BUFFER_SIZE = 1024;

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
	    
	    //String value = samrec.toString();
            //byte[] buf = (new Text(value)).getBytes();
            //int len = value.getLength();
            //int start = 0;
	    //if(value.length() == 0)
	    //mProtoTuple.add(null);
	    //else
	    //mProtoTuple.add(new DataByteArray(buf, 0, value.length()));
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
	    mProtoTuple.add(new Integer(samrec.getIndexingBin()));
	    mProtoTuple.add(new Integer(samrec.getMateReferenceIndex()));
	    mProtoTuple.add(new Integer(samrec.getReferenceIndex()));
	    
	    //mProtoTuple.add(new String((String)samrec.getAttribute("PG")));
	    //mProtoTuple.add(new String((String)samrec.getAttribute("RG")));
	    //mProtoTuple.add(new String((String)samrec.getAttribute("SM")));
			
			if(loadAttributes) {
				Map attributes = new HashMap<String, Object>();
				
				final List<SAMRecord.SAMTagAndValue> mySAMAttributes = samrec.getAttributes();
				
				for (final SAMRecord.SAMTagAndValue tagAndValue : mySAMAttributes) {
					//if(skipAttributeTag((String)tagAndValue.tag))
					//  continue;
					
					//System.out.println("attribute: "+tagAndValue.tag+" ("+tagAndValue.value.getClass().getName()+")");
					
					if(tagAndValue.value != null) {

						//System.out.println("found tag name: "+tagAndValue.tag);

						if(tagAndValue.value.getClass().getName().equals("java.lang.Character"))
							attributes.put(tagAndValue.tag, tagAndValue.value.toString());
						else
							attributes.put(tagAndValue.tag, tagAndValue.value);
					}
					
					/*if(tagAndValue.value.getClass().getName().equals("java.lang.String"))
					 //System.out.println("WARNING: TAG: "+((String)tagAndValue.tag)+" has non-String value!");
					 //else
					 //mProtoTuple.add(new String((String)tagAndValue.value));
					 attributes.put(tagAndValue.tag, (String)tagAndValue.value);
					 else {
					 if(tagAndValue.value.getClass().getName().equals("java.lang.Integer"))
					 //mProtoTuple.add(new Integer((Integer)tagAndValue.value));
					 attributes.put(tagAndValue.tag, (Integer)tagAndValue.value);
					 //else System.out.println("WARNING: TAG: "+((String)tagAndValue.tag)+" has non-String/Integer value!");
					 else
					 attributes.put(tagAndValue.tag, tagAndValue.value);
					 }*/
				}
				
				mProtoTuple.add(attributes);
			}
				
	    //final SAMReadGroupRecord rg = samrec.getReadGroup();
	    //mProtoTuple.add(new String(rg.getId()));

	    //System.out.println("TTT cig: "+samrec.getCigarString());	    
	    //System.out.println("TTT qual: "+samrec.getBaseQualityString());

	    // adapted from SAMFileHeader.clone()
	    /*final SAMTextHeaderCodec codec = new SAMTextHeaderCodec();
	    codec.setValidationStringency(ValidationStringency.SILENT);
	    final StringWriter stringWriter = new StringWriter();
	    codec.encode(stringWriter, samrec.getHeader());

	    mProtoTuple.add(new Integer(stringWriter.toString().length()));
	    mProtoTuple.add(new String(stringWriter.toString()));*/

	    //return codec.decode(new StringLineReader(stringWriter.toString()), "SAMFileHeader.clone");

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
}
