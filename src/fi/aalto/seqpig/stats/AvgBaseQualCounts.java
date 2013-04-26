// Copyright (c) 2013 Aalto University
// Copyright (c) 2013 CRS4
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

import java.io.IOException;
import org.apache.pig.data.Tuple;
import org.apache.pig.EvalFunc;
import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import fi.tkk.ics.hadoop.bam.FormatConstants;

public class AvgBaseQualCounts extends EvalFunc<Tuple> implements Algebraic, Accumulator<Tuple>
{
	public static final int READ_LENGTH = 101;
	public static final int STATS_PER_POS = FormatConstants.SANGER_MAX + 1;
	private static final AvgBqReader abqReader = new AvgBqReader();

	private ItemCounter2D itemCounter = new ItemCounter2D(1, STATS_PER_POS, abqReader);

	//************ map abq strings to byte[] ************/
        public static class AvgBqReader implements ItemCounter2D.TupleToItem {

	    public static int map_qual_to_int(char qual) {
		int readbasequal_int = (int)qual - FormatConstants.SANGER_OFFSET;
		    
		if (readbasequal_int < 0 || readbasequal_int > FormatConstants.SANGER_MAX)
		    throw new RuntimeException("Base quality score " + qual + " is out of range");

		return readbasequal_int;
            }

	    public static int map_int_to_qual(int val) {
		return val; // + FormatConstants.SANGER_OFFSET;
	    }

	    public byte[] tupleToItem(final Tuple input) throws ExecException {
		String basequals = (String)input.get(0);
		byte[] output = new byte[1];
		
		long avg_base_qual = 0L;

		for(int pos = 0; pos < basequals.length(); ++pos)
		    avg_base_qual += map_qual_to_int(basequals.charAt(pos));
			
		output[0] = (byte)Math.round(avg_base_qual/((double)basequals.length()));
		return output;
	    }
	}

	//************** Algebraic ******************/

	public String getInitial() {
		return Initial.class.getName();
	}

	public String getIntermed() {
		return Intermediate.class.getName();
	}

	public String getFinal() {
		return Intermediate.class.getName();
	}

	@Override
	public Schema outputSchema(Schema input) {
		return itemCounter.outputSchema();
	}

	@Override
	public Tuple exec(Tuple input) throws IOException {
		return itemCounter.execAggregate(input);
	}

	public static class Initial extends EvalFunc<Tuple> {
		@Override
		public Tuple exec(Tuple input) throws IOException {
			ItemCounter2D counter = new ItemCounter2D(1, STATS_PER_POS, abqReader);
			return counter.execInitial(input);
		}
	}

	public static class Intermediate extends EvalFunc<Tuple> {
		@Override
		public Tuple exec(Tuple input) throws IOException {
			ItemCounter2D counter = new ItemCounter2D(1, STATS_PER_POS, abqReader);
			return counter.execAggregate(input);
		}
	}

	/* Accumulator interface implementation (delegates to ItemCounter2D) */

	@Override
	public void accumulate(Tuple b) throws IOException {
		itemCounter.accumulate(b);
	}        

	@Override
	public Tuple getValue() {
		return itemCounter.getValue();
	}  

	@Override
	public void cleanup() {
		itemCounter.cleanup();
	}
}
