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

package fi.aalto.seqpig.filter;

import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.Tuple; 
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.PigException;
import org.apache.pig.EvalFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.Expression;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.ResourceStatistics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.lang.StringBuilder;
import java.util.StringTokenizer;
import java.io.IOException;

import fi.tkk.ics.hadoop.bam.FormatConstants.BaseQualityEncoding;
import fi.tkk.ics.hadoop.bam.FastqInputFormat;

public class BaseFilter extends EvalFunc<Tuple> {

    private int threshold = -1;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private BaseQualityEncoding qualityEncoding;

    // tuple input/output format:
    //
    //   sequence: string
    //   quality: string (note: we assume that encoding chosen on command line!!!)

    public BaseFilter(String threshold_s) throws Exception {
	Configuration conf = UDFContext.getUDFContext().getJobConf();	

	if(conf == null)
	    return;

	String encoding = conf.get(FastqInputFormat.CONF_BASE_QUALITY_ENCODING, FastqInputFormat.CONF_BASE_QUALITY_ENCODING_DEFAULT);
	
	if ("illumina".equals(encoding))
	    qualityEncoding = BaseQualityEncoding.Illumina;
	else if ("sanger".equals(encoding))
	    qualityEncoding = BaseQualityEncoding.Sanger;
	else
	    throw new RuntimeException("Unknown " + FastqInputFormat.CONF_BASE_QUALITY_ENCODING + " value " + encoding);
	
	threshold = Integer.parseInt(threshold_s);
    }

    @Override 
    public Tuple exec(Tuple input) throws IOException, org.apache.pig.backend.executionengine.ExecException {
        if (input == null || input.size() < 2)
            return null;

        char[] sequence = ((String)input.get(0)).toCharArray();
	StringBuilder new_sequence = new StringBuilder(sequence.length);
	char[] quality = ((String)input.get(1)).toCharArray();

	if(sequence.length != quality.length)
	    return null;
	
	Tuple tpl = TupleFactory.getInstance().newTuple(2);
	
	tpl.set(1,input.get(1)); // quality values stay the same
	
	for (int i=0; i<sequence.length; i++) {
	    int qual_val = (int)quality[i];
	    char new_base = 'N';
	    
	    if(qual_val >= threshold)
		new_base = sequence[i];
	    
	    new_sequence.append(new_base);
	}
	
	tpl.set(0, new_sequence.toString());
	return tpl;
     }

    @Override
    public Schema outputSchema(Schema input) {
        try{
            Schema tupleSchema = new Schema();

	    tupleSchema.add(new Schema.FieldSchema("sequence", DataType.CHARARRAY));
	    tupleSchema.add(new Schema.FieldSchema("quality", DataType.CHARARRAY));

	    return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), tupleSchema, DataType.TUPLE));
        } catch (Exception e){
            return null;
        }
    }
}
