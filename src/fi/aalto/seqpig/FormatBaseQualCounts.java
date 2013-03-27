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

import java.nio.LongBuffer;
import java.nio.ByteBuffer;
import java.io.IOException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.backend.executionengine.ExecException;

import java.io.IOException;
import java.util.Iterator;

/* formats the output of BaseQualCounts
*/

public class FormatBaseQualCounts extends EvalFunc<DataBag>
{
    private int read_length;

    public FormatBaseQualCounts() {
	read_length = BaseQualCounts.READ_LENGTH;
    }

    public FormatBaseQualCounts(int read_length) {
	this.read_length = read_length;
    }

    // bag output format showing the quality distribution per position:
    //   { ( position, <mean quality> <quality stddev> <basequal 0 fraction>, <basequal 1 fraction>, ..., <basequal N fraction>) }
    
   
    @Override 
    public DataBag exec(Tuple t) throws IOException, org.apache.pig.backend.executionengine.ExecException {
	    
	if (t == null || t.size() == 0)
	    return null;
 
        Tuple tmp = (Tuple)t.get(0);
	DataByteArray input = (DataByteArray)tmp.get(0);
	DataBag output = BagFactory.getInstance().newDefaultBag();
	int num_qual_values = (input.size() / 8) / read_length;
        long[] counts = new long[num_qual_values * read_length];

        ByteBuffer.wrap(input.get()).asLongBuffer().get(counts);

	for(int p = 0; p<read_length; p++) {
	    Tuple output_tpl = TupleFactory.getInstance().newTuple(num_qual_values+3);

	    // set position		
	    output_tpl.set(0, new Integer(p));
	    
            double sum = 0.0;
	    double sum_of_squares = 0.0;
	    long read_counter = 0L;
	    
	    for(int q=0;q<num_qual_values;q++) {
                long count =counts[p*num_qual_values + q];
		long base_qual = (long)BaseQualCounts.BqReader.map_int_to_qual(q);

		sum += count * base_qual;
		sum_of_squares += count * (base_qual * base_qual);
		read_counter += count;
            }

	    double avg = sum / ((double)read_counter);
	    double square_avg = sum_of_squares / ((double)read_counter);

	    output_tpl.set(1, avg);
	    output_tpl.set(2, Math.sqrt(square_avg - (avg * avg)) );
	    
	    for(int q=0;q<num_qual_values;q++)
		output_tpl.set(3+q, new Double(counts[p*num_qual_values + q] / ((double) read_counter)));
	    
	    output.add(output_tpl);
	}

	return output;
    }

    @Override
    public Schema outputSchema(Schema input) {
	try{
	    Schema bagSchema = new Schema();
	    bagSchema.add(new Schema.FieldSchema("pos", DataType.LONG));
            bagSchema.add(new Schema.FieldSchema("mean", DataType.DOUBLE));
	    bagSchema.add(new Schema.FieldSchema("stdev", DataType.DOUBLE));

	    for(int f = 0; f < BaseQualCounts.STATS_PER_POS; f++) {
	        int qscore = BaseQualCounts.BqReader.map_int_to_qual(f);
		bagSchema.add(new Schema.FieldSchema(String.format("qual%d", qscore), DataType.DOUBLE));
	    }

	    return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), bagSchema, DataType.BAG));
	}catch (Exception e){
	    return null;
	}
    }
}
