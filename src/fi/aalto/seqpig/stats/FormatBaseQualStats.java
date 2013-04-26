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

package fi.aalto.seqpig.stats;

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

/* formats the output of BaseQualStats
*/

// idea: compute variance via the "wrong formula"
// see http://www.cs.berkeley.edu/~mhoemmen/cs194/Tutorials/variance.pdf
// 
// sigma_N(x) = sqrt(1/(N-1) (\sum_i x_i^2 - 1/n (\sum x_i)^2))

public class FormatBaseQualStats extends EvalFunc<DataBag>
{

    public FormatBaseQualStats() {
    }

    // nested tuple input format:
    //   for each read position pos
    //       (pos, <number_of_samples>, \sum x_i, \sum x_i^2)


    // tuple output format:
    //   position
    //   avg base quality (arithmetic mean)
    //   standard deviation (based on population variance)
   
    @Override 
    public DataBag exec(Tuple t) throws IOException, org.apache.pig.backend.executionengine.ExecException {
	    
	if (t == null || t.size() == 0)
	    return null;

	Tuple input = (Tuple)t.get(0);

	DataBag output = BagFactory.getInstance().newDefaultBag();

	int num_of_fields = input.size();

	for(int f = 0; f<num_of_fields; f++) {
        	Tuple input_tpl = (Tuple)input.get(f);
		Tuple output_tpl = TupleFactory.getInstance().newTuple(3);

		// sigma_N(x) = sqrt(1/(N-1) (\sum_i x_i^2 - 1/n (\sum x_i)^2))
		//	      = sqrty(1/(N-1) (tmp1 - tmp2))

		long N = (Long)input_tpl.get(1);

                // set value for position
		output_tpl.set(0, (Integer)input_tpl.get(0));

		if(N >= 1) {
		    double tmp1 = (Long)input_tpl.get(3);
		    double tmp2 = ((Long)input_tpl.get(2) / ((double)N))
			* ((Long)input_tpl.get(2));
		    
		    // set value for mean
		    output_tpl.set(1, (Long)input_tpl.get(2) / ((double)N));

		    String str = String.format("N: %d, tmp1: %f tmp2: %f tmp1-tmp2: %f", N, tmp1, tmp2, tmp1-tmp2);
		    System.out.println(str);

		    if(N >= 2)
		        // set value for stddev
		        output_tpl.set(2, Math.sqrt( (tmp1 - tmp2)/((double)N-1) ));
		}

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
	    bagSchema.add(new Schema.FieldSchema("stddev", DataType.DOUBLE));

	    return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), bagSchema, DataType.BAG));
	}catch (Exception e){
	    return null;
	}
    }
}
