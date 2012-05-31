package udf_refcoord;

import java.io.IOException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;

import it.crs4.seal.common.AlignOp;
import it.crs4.seal.common.WritableMapping;

import java.util.ArrayList;


public class RefPositions extends EvalFunc<DataBag>
{
	private WritableMapping mapping = new WritableMapping();
	private ArrayList<Integer> refPositions = new ArrayList(150);
	private TupleFactory mTupleFactory = TupleFactory.getInstance();
	private BagFactory mBagFactory = BagFactory.getInstance();

	// tuple format:
	//   sequence
	//   flag
	//   chr
	//   position
	//   cigar
	private void loadTuple(Tuple tpl) throws org.apache.pig.backend.executionengine.ExecException
	{
		mapping.clear();

		mapping.setSequence((String)tpl.get(0));
		mapping.setFlag(((Integer)tpl.get(1)).intValue());

		// now get the rest of the fields
		if (mapping.isMapped())
		{
			mapping.setContig((String)tpl.get(2));
			mapping.set5Position(((Integer)tpl.get(3)).intValue());
			mapping.setAlignment(AlignOp.scanCigar((String)tpl.get(4)));
		}
	}


	public DataBag exec(Tuple input) throws IOException, org.apache.pig.backend.executionengine.ExecException {
		if (input == null || input.size() == 0)
			return null;
		try
		{
			DataBag output = mBagFactory.newDefaultBag();
			loadTuple(input);
			mapping.calculateReferenceCoordinates(refPositions);
			String seq = mapping.getSequenceString();
			for (int i = 0; i < seq.length(); ++i)
			{
				int pos = refPositions.get(i);
				if (pos >= 0)
				{
					Tuple tpl = TupleFactory.getInstance().newTuple(3);
					tpl.set(0, mapping.getContig());
					tpl.set(1, pos);
					tpl.set(2, seq.substring(i, i+1));
					output.add(tpl);
				}
			}

			return output;

		} catch(Exception e) {
			throw WrappedIOException.wrap("Caught exception processing input row ", e);
		}
	}

	public Schema outputSchema(Schema input) {
		try{
			Schema bagSchema = new Schema();
			bagSchema.add(new Schema.FieldSchema("chr", DataType.CHARARRAY));
			bagSchema.add(new Schema.FieldSchema("pos", DataType.INTEGER));
			bagSchema.add(new Schema.FieldSchema("base", DataType.CHARARRAY));

			return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), bagSchema, DataType.BAG));
		}catch (Exception e){
			return null;
		}
	}
}
