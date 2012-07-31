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
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;

//import it.crs4.seal.common.AlignOp;
//import it.crs4.seal.common.WritableMapping;

//import java.util.ArrayList;


public class PileupOutputFormatting extends EvalFunc<Tuple> implements Accumulator<Tuple>
{
	// tuple format:
	//   pileup string
	//   base qualities
	//
	
   private String bases = null;
   private String basequals = null;

   @Override
   public Tuple exec(Tuple input) throws IOException {
      try {
      String mbases = "";
      String mbasequals = "";
      DataBag bag = (DataBag)input.get(0);
      Iterator it = bag.iterator();

      while (it.hasNext()){
           Tuple t = (Tuple)it.next();
           if (t != null && t.size() > 1 && t.get(0) != null && t.get(1) != null) {
           	mbases = mbases +(String)t.get(0);
                mbasequals = mbasequals + (String)t.get(1);
           }
      }

      Tuple tpl = TupleFactory.getInstance().newTuple(2);

      tpl.set(0, mbases);
      tpl.set(1, mbasequals);

      return tpl;
      } catch (Exception e) {
            int errCode = 2106;
            String msg = "Error while computing pairwise string concatenation in " + this.getClass().getSimpleName();
            System.err.println(e.toString());
            throw new ExecException(msg, errCode, PigException.BUG, e);
      } 
   }

   @Override
   public void accumulate(Tuple input) throws IOException {

      try {
      DataBag bag = (DataBag)input.get(0);
      Iterator it = bag.iterator();

      while (it.hasNext()){
           Tuple t = (Tuple)it.next();
           if (t != null && t.size() > 1 && t.get(0) != null && t.get(1) != null) {
		    if(bases != null && basequals != null) {
          		bases = bases +(String)t.get(0);
          		basequals = basequals + (String)t.get(1);
          	    } else {
          		bases = (String)t.get(0);
          		basequals = (String)t.get(1);
          	    }
           }
      }
      } catch (Exception e) {
            int errCode = 2106;
            String msg = "Error while computing pairwise string concatenation in " + this.getClass().getSimpleName();
	    System.err.println(e.toString());
            throw new ExecException(msg, errCode, PigException.BUG, e);           
      }
    }

    @Override
    public void cleanup() {
        bases = null;
	basequals = null;
    }

    @Override
    public Tuple getValue() {
        System.out.println("getValue!");
	try {
		Tuple tpl = TupleFactory.getInstance().newTuple(2);

		tpl.set(0, bases);
        	tpl.set(1, basequals);

        	return tpl;
	} catch (Exception e) {
		return null;
	}
    }

    public Schema outputSchema(Schema input) {
	try{
            Schema tupleSchema = new Schema();
            tupleSchema.add(new Schema.FieldSchema("pileup", DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema("basequals", DataType.CHARARRAY));
            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),tupleSchema, DataType.TUPLE));
        }catch (Exception e){
                return null;
        }
    }
}
