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

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
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
    public FormatBaseQualCounts() {
    }

    // tuple output format:
    //   { ( position
    //       ( <bin_1_count> ... <bin_n_count> )
    //       approx average
    //       approx stddev
    //   ) }
    
   
    @Override 
    public DataBag exec(Tuple t) throws IOException, org.apache.pig.backend.executionengine.ExecException {
	    
	if (t == null || t.size() == 0)
	    return null;

	Tuple input = (Tuple)t.get(0);

	DataBag output = BagFactory.getInstance().newDefaultBag();

	int num_base_positions = input.size();
	int num_qual_bins = BaseQualCounts.STATS_PER_POS - 1;
        //int min_base_qual = BaseQualCounts.MIN_BASE_QUAL;
	//int base_qual_bin_size = BaseQualCounts.BASE_QUAL_BINSIZE;

	for(int f = 0; f<num_base_positions; f++) {
        	Tuple input_tpl = (Tuple)input.get(f);
		Tuple output_tpl = TupleFactory.getInstance().newTuple(4);

		// set position		
		output_tpl.set(0, (Integer)input_tpl.get(0));
		
		double sum = 0;
		double sum_of_squares = 0;
		long total_count = 0;

		Tuple distribution_tpl = TupleFactory.getInstance().newTuple(num_qual_bins);

		for(int b=0;b<num_qual_bins;b++) {
			long count = (Long)input_tpl.get(b+1);
			int base_qual = BaseQualCounts.map_int_to_basequal(b);

			total_count += count;
			sum += count * base_qual;
			sum_of_squares += count * (base_qual * base_qual);
		}

		for(int b=0;b<num_qual_bins;b++) {
			distribution_tpl.set(b, (Long)input_tpl.get(b+1) / ((double)total_count));
		}

		double avg = sum / total_count;
		double square_avg = sum_of_squares / total_count;

                output_tpl.set(1, distribution_tpl);
		output_tpl.set(2, avg);
		output_tpl.set(3, square_avg - (avg * avg) );

		output.add(output_tpl);
	}

	return output;
    }

    @Override
    public Schema outputSchema(Schema input) {
	try{
	    Schema bagSchema = new Schema();
	    bagSchema.add(new Schema.FieldSchema("pos", DataType.LONG));

	    Schema columnSchema = new Schema();

	    for(int f = 1; f < BaseQualCounts.STATS_PER_POS; f++) {
	        int qscore = BaseQualCounts.map_int_to_basequal(f-1); // note: -1
		columnSchema.add(new Schema.FieldSchema(String.format("qual%d", qscore), DataType.DOUBLE));
	    }

	    bagSchema.add(new Schema.FieldSchema("dist", columnSchema, DataType.TUPLE));
            bagSchema.add(new Schema.FieldSchema("mean", DataType.DOUBLE));
	    bagSchema.add(new Schema.FieldSchema("var", DataType.DOUBLE));

	    return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), bagSchema, DataType.BAG));
	}catch (Exception e){
	    return null;
	}
    }
}
