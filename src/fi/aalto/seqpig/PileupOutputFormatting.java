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
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;

//import it.crs4.seal.common.AlignOp;
//import it.crs4.seal.common.WritableMapping;

//import java.util.ArrayList;


public class PileupOutputFormatting extends EvalFunc<Tuple> implements Accumulator<Tuple>
{
	// tuple format:
	//   refbase
	//   pileup string
	//   base qualities
	// for debugging additionally
	//   read
	//   start
	//   cigar
	//   MD tag
	//   pos  
	//


	// WARNINGS:
	//   we use the folling Pig UDF warnings:
	//   PigWarning.UDF_WARNING_5 :      incomplete input
	//   PigWarning.UDF_WARNING_6 :      other problem

   private final static int DEBUG_PILEUP_OUTPUT_FORMATTING = 0;

   private String refbase = null;	
   private String bases = null;
   private String basequals = null;
   private int counter = 0;
   private int last_pos = -1;

   @Override
   public Tuple exec(Tuple input) throws IOException {
      //try {
      String mbases = "";
      String mbasequals = "";

      if(input.size() < 2 || input.get(0) == null || input.get(1) == null) {
	warn("Pileup output formatter received incomplete tuple!", PigWarning.UDF_WARNING_5);
	return null;
      }

      DataBag bag = (DataBag)input.get(0);
      Iterator it = bag.iterator();
      int pos = 0;
      boolean new_pos = false;

      pos = ((Integer)input.get(1)).intValue();

      if (last_pos == -1 || pos != last_pos) {
	new_pos = true;
	counter = 0;
      }

      while (it.hasNext()){
           Tuple t = (Tuple)it.next();
           if (t != null && t.size() > 2 && t.get(1) != null) {
           	mbases = mbases +(String)t.get(1);

		// note: if we are just getting a deletion it will have
		// a null quality since it actually refers to the previous position!
		if(t.get(2) != null) {
                	mbasequals = mbasequals + (String)t.get(2);
			counter++;
		}
	   
                if(t.get(0) != null) {
		    if(refbase == null || new_pos)
			refbase = (String)t.get(0);
		   else if(!refbase.equals((String)t.get(0))) {
			if( DEBUG_PILEUP_OUTPUT_FORMATTING > 0)
				throw new IOException("PileupOutputFormatting: found refbase mismatch: "+refbase+" vs "+(String)t.get(0)+" read: "+(String)t.get(3)+" start: "+((Integer)t.get(4)).intValue()+" cigar: "+(String)t.get(5)+" MD: "+(String)t.get(6)+" pos: "+pos+" counter: "+counter+" mbases: "+mbases+" mbasequals: "+mbasequals);
	         	else {
				warn("PileupOutputFormatting: found refbase mismatch: "+refbase+" vs "+(String)t.get(0)+" (enable DEBUG for more information)", PigWarning.UDF_WARNING_6);
				return null;
			}
			}
		   }
	   }
      }

      Tuple tpl = TupleFactory.getInstance().newTuple(4);

      tpl.set(0, refbase);
      tpl.set(1, new Integer(counter));
      tpl.set(2, mbases);
      tpl.set(3, mbasequals);

      last_pos = pos;

      return tpl;
   }

   @Override
   public void accumulate(Tuple input) throws IOException {

      if(input.size() < 2 || input.get(0) == null || input.get(1) == null) {
        warn("Pileup output formatter received incomplete tuple!", PigWarning.UDF_WARNING_2);
        return;
      }

      DataBag bag = (DataBag)input.get(0);
      Iterator it = bag.iterator();
      int pos = 0;
      boolean new_pos = false;

      pos = ((Integer)input.get(1)).intValue();
      
      if (last_pos == -1 || pos != last_pos) {
                new_pos = true;
		bases = null;
		basequals = null;
		counter = 0;
      }

      while (it.hasNext()){
           Tuple t = (Tuple)it.next();
           if (t != null && t.size() > 2 && t.get(1) != null) {
		    if(bases != null && basequals != null) { // note: it should
		    // not be the case that we see the match first, then the deletion
		    // that starts there!!!
          		bases = bases +(String)t.get(1);

			if(t.get(2) != null) {
          			basequals = basequals + (String)t.get(2);
				counter++;
			}
          	    } else {
          		bases = (String)t.get(1);
          		basequals = (String)t.get(2); // note: it should not be the case that basequals is null here!!!!	
			counter = 1;
          	    }
           }
	   if(t != null && t.size() > 0 && t.get(0) != null) {
                if(refbase == null || new_pos)
                        refbase = (String)t.get(0);
                else if(!refbase.equals((String)t.get(0))) {
			if( DEBUG_PILEUP_OUTPUT_FORMATTING > 0)
				throw new IOException("PileupOutputFormatting: found refbase mismatch: "+refbase+" vs "+(String)t.get(0)+" read: "+(String)t.get(3)+" start: "+((Integer)t.get(4)).intValue()+" cigar: "+(String)t.get(5)+" MD: "+(String)t.get(6)+" pos: "+pos+" counter: "+counter+" bases: "+bases+" basequals: "+basequals);
			else {
				warn("PileupOutputFormatting: found refbase mismatch: "+refbase+" vs "+(String)t.get(0)+" (enable DEBUG for more information)", PigWarning.UDF_WARNING_6);
                                return;
			}
           	     }
	    }
      }

      last_pos = pos;
    }

    @Override
    public void cleanup() {
	refbase = null;
        bases = null;
	basequals = null;
	counter = 0;
    }

    @Override
    public Tuple getValue() {
        System.out.println("getValue!");
	try {
		Tuple tpl = TupleFactory.getInstance().newTuple(4);

		tpl.set(0, refbase);
		tpl.set(1, new Integer(counter));
		tpl.set(2, bases);
        	tpl.set(3, basequals);

        	return tpl;
	} catch (Exception e) {
		return null;
	}
    }

    public Schema outputSchema(Schema input) {
	try{
            Schema tupleSchema = new Schema();
	    tupleSchema.add(new Schema.FieldSchema("refbase", DataType.CHARARRAY));
	    tupleSchema.add(new Schema.FieldSchema("count", DataType.INTEGER));
            tupleSchema.add(new Schema.FieldSchema("pileup", DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema("basequals", DataType.CHARARRAY));
            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),tupleSchema, DataType.TUPLE));
        }catch (Exception e){
                return null;
        }
    }
}
