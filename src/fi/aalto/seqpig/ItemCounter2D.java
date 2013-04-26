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

import java.util.Arrays;
import java.nio.LongBuffer;
import java.nio.ByteBuffer;
import java.io.IOException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class ItemCounter2D implements Accumulator<Tuple>
{
	public interface TupleToItem {
		public byte[] tupleToItem(final Tuple input) throws ExecException;
	};

	private static TupleFactory mTupleFactory = TupleFactory.getInstance();

	private int itemSize;
	private int dictionarySize;
	private TupleToItem itemConverter;
	private long[] counts = null; // accumulates item counts for Accumulator implementation

	public ItemCounter2D(int itemSize, int dictionarySize, final TupleToItem itemConverter) {
		if (itemSize <= 0)
			throw new IllegalArgumentException("itemSize must be greater than 0 (got " + itemSize + ")");
		if (dictionarySize <= 0)
			throw new IllegalArgumentException("dictionarySize must be greater than 0 (got " + dictionarySize + ")");
		if (dictionarySize > Byte.MAX_VALUE)
			throw new IllegalArgumentException("for now we only support dictionaries with size <= " + Byte.MAX_VALUE);
		if (itemConverter == null)
			throw new IllegalArgumentException("NULL item converter!");

		this.itemSize = itemSize;
		this.dictionarySize = dictionarySize;
		this.itemConverter = itemConverter;
	}

	public int getItemSize() { return itemSize; }
	public int getDictionarySize() { return dictionarySize; }

	public Schema outputSchema() {
		Schema tupleSchema = new Schema();
		tupleSchema.add(
				new Schema.FieldSchema(
					String.format("long_array_%dx%d", dictionarySize, itemSize),
					DataType.BYTEARRAY));

		String schemaName = String.format("%s_%d_%d", this.getClass().getName().toLowerCase(), dictionarySize, itemSize);
		try {
			return new Schema(new Schema.FieldSchema(schemaName, tupleSchema, DataType.TUPLE));
		} catch (org.apache.pig.impl.logicalLayer.FrontendException e) {
			e.printStackTrace();
			return null;
		}
	}

	protected ByteBuffer newZeroedCountsBuffer() {
		ByteBuffer bytes = ByteBuffer.allocate(itemSize * dictionarySize * Long.SIZE / 8);

		assert(Long.SIZE == 8*8);
		Arrays.fill(bytes.array(), (byte)0);
		return bytes;
	}

	public long[] newZeroedCountsArray() {
		long[] retval =	new long[itemSize * dictionarySize];
		Arrays.fill(retval, 0L);
		return retval;
	}

	public long[] accumulateItemIn(long[] counts, final Tuple newTuple) throws IOException {
		return accumulateItemIn(counts, itemConverter.tupleToItem(newTuple));
	}

	public long[] accumulateItemIn(long[] counts, final byte[] newColumnItem) {
		if (newColumnItem.length > itemSize) {
			throw new IllegalArgumentException(
					"accumulated items must have a length that is less than or equal to the specified itemSize.\n" +
					"(ItemCounter2D itemSize " + itemSize + " but newColumnItem has length " + newColumnItem.length + ")");
		}

		for (int i = 0; i < newColumnItem.length; ++i) {
			if (newColumnItem[i] < 0 || newColumnItem[i] >= dictionarySize)
				throw new IllegalArgumentException("newColumnItem must only contain values in the range [0, dictionarySize) -- got " + newColumnItem[i]);
			int idx = dictionarySize*i + newColumnItem[i];
			counts[idx] += 1;
		}

		return counts;
	}

	protected ByteBuffer aggregateDataBag(DataBag values) throws IOException {
		ByteBuffer output = newZeroedCountsBuffer();

		long[] histogram = newZeroedCountsArray();
		long[] buffer = new long[histogram.length];

		for (Tuple partial: values) {
			DataByteArray data = (DataByteArray)partial.get(0);
			if (partial.size() == 1) { // this is a partial histogram
				// read data into long[] buffer, then aggregate
				ByteBuffer.wrap(data.get()).asLongBuffer().get(buffer);
				for (int i = 0; i < histogram.length; ++i)
					histogram[i] += buffer[i];
			}
			else if (partial.size() == 2) { // these are initial values
				byte[] newColumnItem = data.get();
				accumulateItemIn(histogram, newColumnItem);
			}
			else
				throw new RuntimeException("unexpected partial tuple size of " + partial.size());
		} 
		
		// copy histogram array into output
		output.asLongBuffer().put(histogram, 0, histogram.length);

		return output;
	} 

	/*
	 * Initial:  map quality strings to a Tuple(byte[], "q").
	 * The byte[] contains the quality values.
	 */
	public Tuple execInitial(Tuple input) throws IOException {
		DataBag bg = (DataBag)input.get(0);

		if(bg != null && bg.iterator().hasNext()) {
			Tuple t = bg.iterator().next();

			Tuple output_tpl = mTupleFactory.newTuple(2); 
			output_tpl.set(0, new DataByteArray(itemConverter.tupleToItem(t)));
			output_tpl.set(1, "q"); // sentinel value
			return output_tpl;
		}
		else
			return null;
	}

	/*
	 * Intermed
	 * Generates a Tuple with a count[qvalue, pos] array.
	 * The array is encoded in one dimension, with rows of length dictionarySize
	 * (so, each row corresponds to a read position; within each row, the position
	 * corresponds to a specific value).
	 *
	 * Given a Tuple from Initial, it will count the values per pos
	 * and sum them to the running count.
	 *
	 * Given another Tuple output from Intermed, it aggregates the two count arrays.
	 */
	public Tuple execAggregate(Tuple input) throws IOException {
		DataBag bag = (DataBag)input.get(0);
		ByteBuffer counts = aggregateDataBag(bag);
		Tuple output_tpl = mTupleFactory.newTuple(1); 
		output_tpl.set(0, new DataByteArray(counts.array()));
		return output_tpl;
	}


	/* Accumulator interface implementation */

	@Override
	public void accumulate(Tuple b) throws IOException {
		try {
			if(counts == null)
				counts = newZeroedCountsArray();

			accumulateItemIn(counts, itemConverter.tupleToItem(b));
		}
	 	catch (Exception e) {
			e.printStackTrace();
			int errCode = 2106;
			String msg = "Error while counting in " + this.getClass().getSimpleName();
			throw new ExecException(msg, errCode, PigException.BUG, e);           
		}
	}        

	@Override
	public Tuple getValue() {
		try {
			Tuple output_tpl = mTupleFactory.newTuple(1); 
			// Copy our counts long[] array to a byte[], via a ByteBuffer.
			// We then wrap the byte[] in a DataByteArray and attach it to the tuple.
			ByteBuffer buffer = newZeroedCountsBuffer();
			buffer.asLongBuffer().put(counts);
			output_tpl.set(0, new DataByteArray(buffer.array()));
			return output_tpl;
		} catch (ExecException e) {
			e.printStackTrace();
			return null;
		}
	}  

	@Override
	public void cleanup() {
		counts = null;
	}
}
