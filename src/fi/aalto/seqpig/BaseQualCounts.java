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

package fi.aalto.seqpig;

import java.io.IOException;
import org.apache.pig.data.Tuple;
import org.apache.pig.EvalFunc;
import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import fi.tkk.ics.hadoop.bam.FormatConstants;

public class BaseQualCounts extends EvalFunc<Tuple> implements Algebraic, Accumulator<Tuple>
{
	public static final int READ_LENGTH = 101;
	protected static final int STATS_PER_POS = FormatConstants.SANGER_MAX + 1;
	private static final BqReader bqReader = new BqReader();

	private ItemCounter2D itemCounter = new ItemCounter2D(READ_LENGTH, STATS_PER_POS, bqReader);

	//************ map bq strings to byte[] ************/
	private static class BqReader implements ItemCounter2D.TupleToItem {

		public byte[] tupleToItem(final Tuple input) throws ExecException {
			String basequals = (String)input.get(0);
			byte[] output = new byte[basequals.length()];

			for(int pos = 0; pos < basequals.length(); ++pos) {
				int readbasequal_int = (int)basequals.charAt(pos) - FormatConstants.SANGER_OFFSET;

				if (readbasequal_int < 0 || readbasequal_int > FormatConstants.SANGER_MAX) {
					throw new RuntimeException("Base quality score " +
							(char)(readbasequal_int + FormatConstants.SANGER_OFFSET) +
							" is out of range");
				}
				output[pos] = (byte)readbasequal_int;
			}

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
			ItemCounter2D counter = new ItemCounter2D(READ_LENGTH, STATS_PER_POS, bqReader);
			return counter.execInitial(input);
		}
	}

	public static class Intermediate extends EvalFunc<Tuple> {
		@Override
		public Tuple exec(Tuple input) throws IOException {
			ItemCounter2D counter = new ItemCounter2D(READ_LENGTH, STATS_PER_POS, bqReader);
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
