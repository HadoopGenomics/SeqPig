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
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.EvalFunc;
import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class BaseCounts extends EvalFunc<Tuple> implements Algebraic, Accumulator<Tuple>
{
	protected static final char[] BASES = new char[]{ 'A', 'C', 'G', 'T', 'N' };
	public static final int READ_LENGTH = 101;
	private static final ReadSplitter readSplitter = new ReadSplitter();

	private ItemCounter2D itemCounter = new ItemCounter2D(READ_LENGTH, BASES.length, readSplitter);

	//************ map sequences to byte[] ************/
	private static class ReadSplitter implements ItemCounter2D.TupleToItem {

		protected static byte map_base_to_int(char c) {
			switch(c) {
				case 'A':
					return (byte)0;
				case 'C':
					return (byte)1;
				case 'G':
					return (byte)2;
				case 'T':
					return (byte)3;
				case 'N':
					return (byte)4;
				default:
					throw new RuntimeException("invalid base character " + c);
			}
		}

		public byte[] tupleToItem(final Tuple input) throws ExecException {
			String bases = (String)input.get(0);
			byte[] output = new byte[bases.length()];

			for(int pos = 0; pos < bases.length(); ++pos) {
				output[pos] = map_base_to_int(bases.charAt(pos));
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
			ItemCounter2D counter = new ItemCounter2D(READ_LENGTH, BASES.length, readSplitter);
			return counter.execInitial(input);
		}
	}

	public static class Intermediate extends EvalFunc<Tuple> {
		@Override
		public Tuple exec(Tuple input) throws IOException {
			ItemCounter2D counter = new ItemCounter2D(READ_LENGTH, BASES.length, readSplitter);
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
