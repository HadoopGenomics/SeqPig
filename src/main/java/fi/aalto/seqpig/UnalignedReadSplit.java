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

import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;

/* UDF UnalignedReadSplit
   
 * takes an (unaligned) single read, for example from a Fastq or Qseq input source,
 * and produces a set of (name, position, base, base_quality) values for each position in the read
 */

public class UnalignedReadSplit extends EvalFunc<DataBag>
{
    private String name;
    private String sequence;
    private String basequal;

    private TupleFactory mTupleFactory = TupleFactory.getInstance();

    public UnalignedReadSplit() throws org.apache.pig.parser.ParserException, java.io.IOException {}

    // tuple input format: (subset of FastqLoader output format + MD tag)
    //   sequence
    //   base qualities

    // tuple output format:
    //   base position (inside read)
    //   base
    //   base quality    

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

	NonSpillableDataBag output = new NonSpillableDataBag(sequence.length());
	String readbase = "";

        for(int i=0;i<sequence.length();i++) {
	    int cur_basequal = getBaseQuality(i);
	    Tuple tpl = mTupleFactory.newTuple(3);

	    readbase = sequence.substring(i,i+1);

	    tpl.set(0, i);
	    tpl.set(1, readbase);
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
	    bagSchema.add(new Schema.FieldSchema("readbase", DataType.CHARARRAY));
	    bagSchema.add(new Schema.FieldSchema("basequal", DataType.INTEGER));

	    return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), bagSchema, DataType.BAG));
	}catch (Exception e){
	    return null;
	}
    }
}
