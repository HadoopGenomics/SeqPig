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

import org.apache.hadoop.conf.Configuration;

import org.apache.pig.data.NonSpillableDataBag;
//import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
//import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.SchemaTuple;
import org.apache.pig.data.SchemaTupleFactory;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.data.SchemaTupleFrontend;
import org.apache.pig.data.SchemaTupleClassGenerator.GenContext;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.PigContext;

import it.crs4.seal.common.AlignOp;
import it.crs4.seal.common.MdOp;
import it.crs4.seal.common.WritableMapping;
import it.crs4.seal.common.AbstractTaggedMapping;
import it.crs4.seal.common.FormatException;

import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

/* UDF UnalignedReadSplit
   
 * takes an (unaligned) single read, for example from a Fastq or Qseq input source,
 * and produces a set of (name, position, base, base_quality) values for each position in the read
*/

public class UnalignedReadSplit extends EvalFunc<DataBag>
{
    private String name;
    private String sequence;
    private String basequal;

    private SchemaTupleFactory mSchemaTupleFactory;
    //private TupleFactory mTupleFactory = TupleFactory.getInstance();
    //private BagFactory mBagFactory = BagFactory.getInstance();

    public UnalignedReadSplit() throws org.apache.pig.parser.ParserException, java.io.IOException {
        Configuration conf = UDFContext.getUDFContext().getJobConf();

        //Schema udfSchema = Utils.getSchemaFromString("(pos:int, readbase:int, basequal:int)");
        Schema bagSchema = new Schema();
            bagSchema.add(new Schema.FieldSchema("pos", DataType.INTEGER));
            bagSchema.add(new Schema.FieldSchema("readbase", DataType.INTEGER));
            bagSchema.add(new Schema.FieldSchema("basequal", DataType.INTEGER));

            Schema udfSchema = new Schema(new Schema.FieldSchema("UnalignedReadSplit", bagSchema, DataType.BAG));

        if(conf != null) {
            SchemaTupleBackend.initialize(conf, new PigContext()); 
            mSchemaTupleFactory = SchemaTupleFactory.getInstance(udfSchema, false);
        } //else {
            GenContext context = GenContext.UDF;
            SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, false, context);
        //}
    }

    // tuple input format: (subset of FastqUDFLoader output format + MD tag)
    //   sequence
    //   base qualities

    // tuple output format:
    //   base position (inside read)
    //   base
    //   base quality (if applicable else null)    

    private int getBaseQuality(int baseindex) {
    	return (int)(basequal.substring(baseindex, baseindex+1).charAt(0))-33;
    }

    @Override 
    public DataBag exec(Tuple input) throws IOException, org.apache.pig.backend.executionengine.ExecException, org.apache.pig.parser.ParserException {
	if (input == null || input.size() == 0)
	    return null;

	sequence = (String)input.get(0);
	basequal = (String)input.get(1);

        if(sequence.length() != basequal.length()) {
            throw new IOException("Sequence and base quality strings have different length!! sequence: " + sequence + " qualitis: " + basequal);
	}

	// DataBag output = mBagFactory.newDefaultBag();
	NonSpillableDataBag output = new NonSpillableDataBag(sequence.length());
	String readbase = "";

        for(int i=0;i<sequence.length();i++) {
		readbase = sequence.substring(i,i+1);
                int readbase_int = (int)readbase.charAt(0);
		int cur_basequal = getBaseQuality(i);

		SchemaTuple tpl = mSchemaTupleFactory.newTuple(3);

		tpl.set(0, i);
		tpl.set(1, readbase_int);
		tpl.set(2, cur_basequal);

		output.add(tpl);
	}

	return output;
    }

    @Override
	public Schema outputSchema(Schema input) {
	try{
	    Schema bagSchema = new Schema();
	    bagSchema.add(new Schema.FieldSchema("pos", DataType.INTEGER));
	    bagSchema.add(new Schema.FieldSchema("readbase", DataType.INTEGER));
	    bagSchema.add(new Schema.FieldSchema("basequal", DataType.INTEGER));

	    return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), bagSchema, DataType.BAG));
	}catch (Exception e){
	    return null;
	}
    }
}
