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

import java.util.Iterator;

import java.io.IOException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;

import fi.tkk.ics.hadoop.bam.FormatConstants;

public class BaseQualCounts extends EvalFunc<Tuple> implements Algebraic, Accumulator<Tuple>
{
	protected final static int READ_LENGTH = 100;
	// number of buckets:  position + one per valid quality score (i.e. the max score +1 for 0)
	protected final static int STATS_PER_POS = 1 + FormatConstants.SANGER_MAX + 1;

	private static TupleFactory mTupleFactory = TupleFactory.getInstance();

	protected static void initTuple(Tuple tpl) throws Exception {
		for (int i = 0; i < tpl.size(); ++i) {
			Tuple column = mTupleFactory.newTuple(STATS_PER_POS);
			column.set(0, i); // position
			for (int j = 1; j < STATS_PER_POS; ++j)
				column.set(j, 0L); // counters
			tpl.set(i, column);
		}
	}

	// accumulating_tpl is tuple that follows output convention, new_tpl is (basequals)
	protected static void processTuple(Tuple accumulating_tpl, Tuple new_tpl) throws Exception {
		String basequals = (String)new_tpl.get(0);

		assert(new_tpl.size() == READ_LENGTH);
		assert(basequals.length() == READ_LENGTH);

		for(int pos = 0; pos < basequals.length(); ++pos) {
			int readbasequal_int = (int)basequals.charAt(pos) - FormatConstants.SANGER_OFFSET;

			if (readbasequal_int < 0 || readbasequal_int > FormatConstants.SANGER_MAX) {
				throw new RuntimeException("Base quality score " +
						(char)(readbasequal_int + FormatConstants.SANGER_OFFSET) +
						" is out of range");
			}

			// update base frequencies
			Tuple column = (Tuple)accumulating_tpl.get(pos);
			column.set(1+readbasequal_int, 1L + (Long)column.get(1+readbasequal_int));
		}
	}

	protected static Tuple sumColumnTuples(Tuple accum, Tuple addValue) throws ExecException {
		// sum all elements except the first, which is the position within the sequence
		assert(accum.size() == addValue.size());
		for (int i = 1; i < accum.size(); ++i) {
			accum.set(i, (Long)accum.get(i) + (Long)addValue.get(i));
		}
		return accum;
	}

	// values is bag of tuples that follow output convention
	static protected Tuple combineTuples(DataBag values) throws Exception {
		Tuple output_tpl = mTupleFactory.newTuple(READ_LENGTH);
		initTuple(output_tpl);

		for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
			Tuple partial = it.next();

			for(int pos = 0; pos < partial.size(); ++pos) {
				output_tpl.set(pos, sumColumnTuples((Tuple)output_tpl.get(pos), (Tuple)partial.get(pos)));
			}
		}

		return output_tpl;
	}

	@Override
	public Tuple exec(Tuple input) throws IOException, ExecException {
		try {
			DataBag bag = (DataBag)input.get(0);
			long number_of_reads = bag.size();
			Iterator it = bag.iterator();

			Tuple output_tpl = TupleFactory.getInstance().newTuple(READ_LENGTH); 
			initTuple(output_tpl);

			while (it.hasNext()){
				Tuple t = (Tuple)it.next();
				processTuple(output_tpl, t);
			}

			return output_tpl;
		} catch(Exception e) {
			e.printStackTrace();
			throw new IOException("problem in exec");
		}
	}

	public String getInitial() {
		return Initial.class.getName();
	}

	public String getIntermed() {
		return Final.class.getName();
	}

	public String getFinal() {
		return Final.class.getName();
	}

	static public class Initial extends EvalFunc<Tuple> {
		@Override
		public Tuple exec(Tuple input) {
			try {
				Tuple output_tpl = mTupleFactory.newTuple(READ_LENGTH);
				initTuple(output_tpl);

				DataBag bg = (DataBag)input.get(0);

				if(bg == null) return output_tpl;

				if(bg.iterator().hasNext()) {
					Tuple t = bg.iterator().next();
					processTuple(output_tpl, t); // note: we normalize later, so pretend there is only one read
				}

				return output_tpl;
			} catch(Exception e) {
				e.printStackTrace();
				return null;
			}
		}
	}

	static public class Final extends EvalFunc<Tuple> {
		@Override
		public Tuple exec(Tuple input) throws IOException {
			try {
				DataBag b = (DataBag)input.get(0);
				return combineTuples(b);
			} catch (ExecException ee) {
				ee.printStackTrace();
				throw ee;
			} catch (Exception e) {
				e.printStackTrace();
				int errCode = 2106;
				String msg = "Error while computing average in " + this.getClass().getSimpleName();
				throw new ExecException(msg, errCode, PigException.BUG, e);           

			}
		}
	}

	@Override
	public Schema outputSchema(Schema input) {
		Schema columnSchema = new Schema();
		Schema tupleSchema = new Schema();

		try {
			columnSchema.add(new Schema.FieldSchema("pos", DataType.INTEGER));
			for (int qscore = 0; qscore <= FormatConstants.SANGER_MAX; ++qscore)
				columnSchema.add(new Schema.FieldSchema(String.format("%d", qscore), DataType.LONG));

			for(int i = 0; i < READ_LENGTH; i++) {
				tupleSchema.add(new Schema.FieldSchema(String.format("pos_%04d", i), columnSchema, DataType.TUPLE));
			}

			return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), tupleSchema, DataType.TUPLE));
		} catch(Exception e) { e.printStackTrace(); return null; }
	}

	/* Accumulator interface implementation */

	private Tuple accumulatingTuple = null;

	@Override
	public void accumulate(Tuple b) throws IOException {
		try {
			if(accumulatingTuple == null) {
				accumulatingTuple = mTupleFactory.newTuple(READ_LENGTH);
				initTuple(accumulatingTuple);
			}

			processTuple(accumulatingTuple, b); // note: we normalize later, so pretend there is only one read
		} catch (ExecException ee) {
			ee.printStackTrace();
			throw ee;
		} catch (Exception e) {
			e.printStackTrace();
			int errCode = 2106;
			String msg = "Error while computing average in " + this.getClass().getSimpleName();
			throw new ExecException(msg, errCode, PigException.BUG, e);           
		}
	}        

	@Override
	public void cleanup() {
		accumulatingTuple = null;
	}

	@Override
	public Tuple getValue() {
		return accumulatingTuple;
	}    
}

