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
import org.apache.pig.data.DataType;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.backend.executionengine.ExecException;

import java.io.IOException;
import java.util.Iterator;

/* Formats the output of BaseCounts
*/

public class FormatBaseCounts extends EvalFunc<DataBag>
{
    private int read_length;

    public FormatBaseCounts() {
	read_length = BaseCounts.READ_LENGTH;
    }

    public FormatBaseCounts(int read_length) {
	this.read_length = read_length;
    }

    // bag output format:
    //   { ( position, <A fraction>, <C fraction>, <G fraction>, <T fraction>, <N fraction>) }
   
    @Override 
    public DataBag exec(Tuple t) throws IOException, org.apache.pig.backend.executionengine.ExecException {
	
	if (t == null || t.size() == 0)
	    return null;
	
        Tuple tmp = (Tuple)t.get(0);
	DataByteArray input = (DataByteArray)tmp.get(0);
	DataBag output = BagFactory.getInstance().newDefaultBag();
	int num_bases = (input.size() / 8) / read_length;
        long[] counts = new long[num_bases * read_length];

        ByteBuffer.wrap(input.get()).asLongBuffer().get(counts);

	for(int p = 0; p<read_length; p++) {
	    Tuple output_tpl = TupleFactory.getInstance().newTuple(num_bases+1);

	    // set position		
	    output_tpl.set(0, new Integer(p));
	    
	    long read_counter = 0L;
	    
	    for(int b=0;b<num_bases;b++)
		read_counter += counts[p*num_bases + b];
	    
	    for(int b=0;b<num_bases;b++)
		output_tpl.set(1+b, new Double(counts[p*num_bases + b] / ((double) read_counter)));
	    
	    output.add(output_tpl);
	}

	return output;
    }

    @Override
    public Schema outputSchema(Schema input) {
	try{
	    Schema bagSchema = new Schema();
	    bagSchema.add(new Schema.FieldSchema("pos", DataType.INTEGER));

	    for(int f = 1; f <= BaseCounts.BASES.length; f++) {
		bagSchema.add(new Schema.FieldSchema(String.valueOf(BaseCounts.ReadSplitter.map_int_to_base(f-1)), DataType.DOUBLE));
	    }

	    return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), bagSchema, DataType.BAG));
	}catch (Exception e){
	    return null;
	}
    }
}
