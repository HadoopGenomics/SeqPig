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

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.backend.executionengine.ExecException;

import it.crs4.seal.common.AlignOp;
import it.crs4.seal.common.MdOp;
import it.crs4.seal.common.WritableMapping;
import it.crs4.seal.common.AbstractTaggedMapping;
import it.crs4.seal.common.FormatException;

import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

/* UDF ReadPileup 
   
 * takes a single read (created via input format from SAMRecord) and produces pileup data
    for each reference position
*/

public class BinReadPileup extends EvalFunc<DataBag>
{
    private ArrayList<ReadPileupEntry> readPileups = new ArrayList();
   

    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private BagFactory mBagFactory = BagFactory.getInstance();

    private int qual_threshold = 0; // base quality threshold:
	// in any base has base quality smaller this value the
	// read is discarded for the purpose of pileup (as samtools
	// does it)

    private int left_readindex = 0;
    private int right_readindex = 0;
    private int left_pos = 0;
    private int end_pos = 0;

    private String chrom;

    class ReadPileupEntry implements Comparable<ReadPileupEntry> {
	//public DataBag pileup;
	public ArrayList<Tuple> pileup; 
	public int start_pos=-1;
	public int cur_index=-1; // used for iterating through reads in bucket
	public int size=0;

	public ReadPileupEntry(Tuple read) throws ExecException, IOException {
	    start_pos = ((Integer)read.get(3)).intValue();
	    cur_index = 0;

	    ReadPileup readPileup = new ReadPileup(qual_threshold);
	    DataBag pileup_bag = readPileup.exec(read);

	    if(pileup_bag == null)
		pileup = null;
	    else {
		Iterator it = pileup_bag.iterator();
		pileup = new ArrayList<Tuple>();

		while(it.hasNext()) {

	            Tuple piledup = (Tuple)it.next();
		    ArrayList<Object> tuple = new ArrayList<Object>();

                    // refbase, pileup, qual
             	    tuple.add((String)piledup.get(2));
                    tuple.add((String)piledup.get(3));
                    tuple.add((String)piledup.get(4));
		    
   		    Tuple t =  mTupleFactory.newTupleNoCopy(tuple);
		    pileup.add(t);

		    size++;
		}
	    }
	}

	public int compareTo( ReadPileupEntry other) {
	    if (start_pos < other.start_pos) return -1;
	    else if (start_pos == other.start_pos)
		return 0;
	    else return 1;
	}
    }

    public BinReadPileup() {
        qual_threshold = 0;
    }

    public BinReadPileup(String min_quality) {
	qual_threshold = Integer.parseInt(min_quality);	
    }

    // tuple input format:
    //   sequence
    //   flag
    //   chr
    //   position
    //   cigar
    //   base qualities
    //   MD tag
    //   mapping quality

    // tuple output format:
    //   chr
    //   position
    //   ref base (if known, else null value)
    //   pileup string (according to samtools)
    //   base quality/ies (if applicable else null)
   
    // WARNINGS:
    // we use the folling Pig UDF warnings:
    // 	PigWarning.UDF_WARNING_2 :	problems with parsing CIGAR string
    // 	PigWarning.UDF_WARNING_3 :	problem with parsing MD tag
    // 	PigWarning.UDF_WARNING_4 :	other problem

    @Override 
    public DataBag exec(Tuple input) throws IOException, org.apache.pig.backend.executionengine.ExecException {
	if (input == null || input.size() == 0)
	    return null;
	//try {
	    // first load the mapping and do some error checks
	    
	    DataBag bag = (DataBag)input.get(0);
	    Iterator it = bag.iterator();
	    DataBag output = mBagFactory.newDefaultBag();

	    left_readindex = 0;
	    right_readindex = 0;
	    //left_pos = ((Integer)input.get(1)).intValue();
	    left_pos = -1;
	    end_pos = ((Integer)input.get(1)).intValue();

            readPileups.clear();

	    boolean first_read = true;

	    while (it.hasNext()) {
	        Tuple t = (Tuple)it.next();

		if(first_read) {
			first_read = false;
			chrom = (String)t.get(2);
	  	}

		ReadPileupEntry entry = new ReadPileupEntry(t);

		if(entry.pileup != null)
		    readPileups.add(entry);
	    }

	    // sort by start position
	    Collections.sort(readPileups);

	    Iterator<ReadPileupEntry> readit = readPileups.iterator();
	    
	    while(readit.hasNext()) {
		ReadPileupEntry cur_entry = readit.next();

		if(left_pos == -1) {// i.e., if this is the first read of the bin
		    left_pos = cur_entry.start_pos;
		} 

		boolean final_read = !(readit.hasNext());
		    
		produce_pileup(output, final_read);
		
		right_readindex++;
	    }

	    return output;

	//} catch(Exception e) {
	  //  throw new IOException("Caught exception processing input row " + e.toString());
	//}
    }

    void produce_pileup(DataBag output, boolean final_read) throws IOException {
	
	int right_pos;

	// note: we may assume that if a bin is non-empty it contains at least 2 reads!!

	if(!final_read) {
	    ReadPileupEntry right_entry = readPileups.get(right_readindex);
	    right_pos = right_entry.start_pos;

	    ReadPileupEntry left_entry =  readPileups.get(left_readindex);
	    left_pos = left_entry.start_pos + left_entry.cur_index;
	} else {
	    ReadPileupEntry right_entry = readPileups.get(right_readindex);
	    right_pos = right_entry.start_pos + right_entry.size;
	}

	if(right_pos > end_pos)
		right_pos = end_pos;

	// idea: for all but last read, iterate over all reads whose pileup can now be determined
	// note: for the last read we still need to iterate over the positions that fall into the bin

	while(left_pos < right_pos) {
	    
	    DataBag this_output = mBagFactory.newDefaultBag();
	    int min_non_empty_read = right_readindex;
	    
	    for(int i=left_readindex; i < right_readindex; i++) {
		ReadPileupEntry cur_entry =  readPileups.get(i);

		if(cur_entry.cur_index < cur_entry.size) {
		    this_output.add(cur_entry.pileup.get(cur_entry.cur_index));
		    cur_entry.cur_index++;

		    if(min_non_empty_read > i && cur_entry.cur_index < cur_entry.size) {
			min_non_empty_read = i;
		    }
		}
	    }

	    left_readindex = min_non_empty_read;

	    if(final_read) { // that means we are processing the last read of this bin
		ReadPileupEntry cur_entry =  readPileups.get(right_readindex);

		if(cur_entry.cur_index < cur_entry.size) {
		    this_output.add(cur_entry.pileup.get(cur_entry.cur_index));
		    cur_entry.cur_index++;
		} else
		    return;
	    }

	    PileupOutputFormatting pileupOutputFormatting = new PileupOutputFormatting();
	    ArrayList<Object> tuple = new ArrayList<Object>();

	    // refbase, pileup, qual, start, (flags/16)%2, name

	    tuple.add(this_output);
	    tuple.add(new Integer(left_pos));
	    Tuple tin =  mTupleFactory.newTupleNoCopy(tuple);
	    Tuple tout = pileupOutputFormatting.exec(tin);

	    if(((Integer)tout.get(1)).intValue() > 0) {
	    	ArrayList<Object> tuple_output = new ArrayList<Object>();
	    	tuple_output.add(chrom);
	    	tuple_output.add(left_pos);
	    	tuple_output.add(tout.get(0));
	    	tuple_output.add(tout.get(1));
	    	tuple_output.add(tout.get(2));
	    	tuple_output.add(tout.get(3));
	    
	    	output.add(mTupleFactory.newTupleNoCopy(tuple_output));

	    	// TODO: add call to format output!!!!
	    	left_pos++;
	     } else {
		left_pos = right_pos;
		return;
	     }
	}
    }

    @Override
    public Schema outputSchema(Schema input) {
	try{
	    Schema bagSchema = new Schema();
	    bagSchema.add(new Schema.FieldSchema("chr", DataType.CHARARRAY));
	    bagSchema.add(new Schema.FieldSchema("pos", DataType.INTEGER));
            bagSchema.add(new Schema.FieldSchema("refbase", DataType.CHARARRAY));
            bagSchema.add(new Schema.FieldSchema("count", DataType.INTEGER));
            bagSchema.add(new Schema.FieldSchema("pileup", DataType.CHARARRAY));
            bagSchema.add(new Schema.FieldSchema("basequals", DataType.CHARARRAY));

	    return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), bagSchema, DataType.BAG));
	}catch (Exception e){
	    return null;
	}
    }
}
