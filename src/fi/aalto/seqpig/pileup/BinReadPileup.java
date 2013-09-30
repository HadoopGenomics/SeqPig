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

package fi.aalto.seqpig.pileup;

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

    private static final boolean debug = false;

    private ArrayList<ReadPileupEntry> readPileups = new ArrayList();
   
    private ReadPileup readPileup = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private BagFactory mBagFactory = BagFactory.getInstance();

    private int qual_threshold = 0; // base quality threshold:
    // in any base has base quality smaller this value the
    // read is discarded for the purpose of pileup (as samtools
    // does it)
  

    private int reads_cutoff = -1; // max number of read coverage to be taken into account
    // trying to solve memory issues for large inputs
    
    private int left_readindex = 0; // the index of the left-most read in the bucked that still needs to be processed; forms the left end of the processing frame
    private int right_readindex = 0; // the index of the current read; forms the right end of the processing frame
    private int left_pos = 0; // the ref-position that is currently processed
    private int right_end_pos = 0; // the right end of the bin
    private int left_end_pos = 0; // the left end of the bin

    private int max_read_rightpos = 0; // the maximum end position of any read in the current bin

    private String chrom;

    class ReadPileupEntry implements Comparable<ReadPileupEntry> {
	//public DataBag pileup;
	public ArrayList<Tuple> pileup; 
	public ArrayList<Tuple> coordinates = null;
	public int start_pos=-1;
	public int cur_index=-1; // used for iterating through reads in bucket
	public int size=0;
        public String name;
	public boolean reverse_strand;
	public int flags, mapqual;

        public String getPileupString(Tuple ct, Tuple pt) throws ExecException {
	    String retval = "chrom: ";

	    if(ct.get(0) == null)
		retval += "?";
	    else retval += (String)ct.get(0);

	    retval += " pos: ";

	    if(ct.get(1) == null)
		retval += "?";
	    else {
		int val =  ((Integer)ct.get(1)).intValue();
		retval += val;
	    }

	    retval += " refbase: ";

	    if(pt.get(0) == null)
		retval += "?";
	    else
		retval += (String)pt.get(0);
		
	    retval += " pileup: ";

	    if(pt.get(1) == null)
		retval += "?";
	    else
		retval += (String)pt.get(1);

	    retval += " qual: ";

	    if(pt.get(2) == null)
		retval += "?";
	    else
		retval += (String)pt.get(2);

	    return retval;
        }

	public ReadPileupEntry(Tuple read) throws ExecException, IOException {
	    start_pos = ((Integer)read.get(3)).intValue();
	    flags = ((Integer)read.get(1)).intValue();
	    mapqual = ((Integer)read.get(7)).intValue(); 
	    name = (String)read.get(8);
	    reverse_strand = (((((Integer)read.get(1)).intValue()) & 16) == 16);
	    size = 0;

	    if(start_pos < left_end_pos)
	    	cur_index = left_end_pos - start_pos;
	    else
	 	cur_index = 0;

	    //ReadPileup readPileup = new ReadPileup(qual_threshold);
	    DataBag pileup_bag = readPileup.exec(read);

	    if(pileup_bag == null)
		pileup = null;
	    else {
		Iterator it = pileup_bag.iterator();
		pileup = new ArrayList<Tuple>((int)pileup_bag.size());

		if(debug) coordinates = new ArrayList<Tuple>();

		while(it.hasNext()) {

	            Tuple piledup = (Tuple)it.next();
		    //ArrayList<Object> pileup_tuple = new ArrayList<Object>();
		    Tuple pileup_tuple = mTupleFactory.newTuple(3);

		    if(debug) {
			ArrayList<Object> coordinates_tuple = new ArrayList<Object>();

			// chrom, pos
			coordinates_tuple.add(piledup.get(0));
			coordinates_tuple.add(piledup.get(1));

			Tuple ct =  mTupleFactory.newTupleNoCopy(coordinates_tuple);
			coordinates.add(ct);
		    }

		    // refbase, pileup, qual
                    pileup_tuple.set(0, piledup.get(2));
                    pileup_tuple.set(1, piledup.get(3));
                    pileup_tuple.set(2, piledup.get(4));

                    //Tuple pt =  mTupleFactory.newTupleNoCopy(pileup_tuple);
                    //pileup.add(pt);
                    pileup.add(pileup_tuple);

		    if(size==0 && start_pos != ((Integer)piledup.get(1)).intValue()) {
			start_pos = ((Integer)piledup.get(1)).intValue();
			// note: there should be some warning probably
			// the condition should only hold if the CIGAR starts with a clipping
		    }

		    if(debug) {
			if(((Integer)piledup.get(1)).intValue() - start_pos != size) {

			    if(size > 0)
				throw new IOException("two records for the same position!? "+(((Integer)piledup.get(1)).intValue()-start_pos) +" vs "+size
						      + " " + getPileupString(coordinates.get(size-1), pileup.get(size-1))
						      + " " + getPileupString(coordinates.get(size), pileup.get(size)));
			    else
				throw new IOException("start position of first pileup entry does not match read start position!!");

			}
		    }

		    size++;
		}
	    }
	}

	// NOTE: comparison code mostly taken over from Samtools/Picard SAMRecordCoordinateComparator
	// notable difference: no tie-breaking based on read mate available currently, which may lead to
	// a different sorting order, depending on bam file
	private int compareInts(int i1, int i2) {
	    if (i1 < i2) return -1;
	    else if (i1 > i2) return 1;
	    else return 0;
	}

	// Note1: we assume that refname (chrom) are already the same!!!! (this is a local sort after all
	// and both reads are expected to fall into the same bin)
	public int compareTo( ReadPileupEntry other) {
	    if (start_pos == other.start_pos) {
		if(reverse_strand == other.reverse_strand) {
		    int cmp = name.compareTo(other.name);
		    if(cmp != 0) return cmp;
		    cmp = compareInts(flags, other.flags);
		    if(cmp != 0) return cmp;
		    cmp = compareInts(mapqual, other.mapqual);
		    if(cmp != 0) return cmp;
		    // note: here should be also broken ties by: MateReferenceIndex, MateAlignmentStart and InferredInsertSize (currently not passed over to UDF)		 
		    return 0;
		}
		else return (reverse_strand? 1: -1);
	    } else return compareInts(start_pos, other.start_pos);
	}
    }

    public BinReadPileup() {
        qual_threshold = 0;
	readPileup = new ReadPileup(qual_threshold);
    }

    public BinReadPileup(String min_quality, String read_cutoff_s) {
	qual_threshold = Integer.parseInt(min_quality);	
	reads_cutoff = Integer.parseInt(read_cutoff_s);
	readPileup = new ReadPileup(qual_threshold);
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
    //   sequence name


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
	left_end_pos = ((Integer)input.get(1)).intValue();
	left_pos = -1;
	right_end_pos = ((Integer)input.get(2)).intValue();

	max_read_rightpos = 0;

        int read_counter = 0;

	readPileups.clear();

	boolean first_read = true;

	while (it.hasNext() && (reads_cutoff < 0 || read_counter < reads_cutoff)) {
	    Tuple t = (Tuple)it.next();

	    if(first_read) {
		first_read = false;
		chrom = (String)t.get(2);
	    }

	    ReadPileupEntry entry = new ReadPileupEntry(t);

	    if(entry.pileup != null) {
		readPileups.add(entry);
		read_counter++;
	    }
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

	    if(cur_entry.start_pos + cur_entry.size > max_read_rightpos)
		max_read_rightpos = cur_entry.start_pos + cur_entry.size; 
		    
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

        ReadPileupEntry left_entry =  readPileups.get(left_readindex);
        left_pos = left_entry.start_pos + left_entry.cur_index;

        ReadPileupEntry right_entry = readPileups.get(right_readindex);
        right_pos = right_entry.start_pos;

        if(final_read) // that means we are processing the last read of this bin
            right_pos = max_read_rightpos;

	if(right_pos > right_end_pos)
	    right_pos = right_end_pos;

	if(left_pos < left_end_pos)
	    left_pos = left_end_pos;

	// idea: for all but last read, iterate over all reads whose pileup can now be determined
	// note: for the last read we still need to iterate over the positions that fall into the bin

	while(left_pos < right_pos) {
	    
	    DataBag this_output = mBagFactory.newDefaultBag();
	    int min_non_empty_read = right_readindex;
	    boolean first_in_bag = true;
	    String refbase = null;
	    
	    for(int i=left_readindex; i < right_readindex; i++) {
		ReadPileupEntry cur_entry =  readPileups.get(i);

		if(cur_entry.cur_index < cur_entry.size) {
		    Tuple t =  cur_entry.pileup.get(cur_entry.cur_index);
		    
		    if(first_in_bag && t.get(0) != null) {
			refbase = (String)t.get(0);
			first_in_bag = false;
		    }
		   
		    if(debug) { 
			if(t.get(0) != null && !refbase.equals((String)t.get(0)))
			    throw new IOException("mismatching rebases: "+refbase+ " and "+(String)t.get(0)+" for "+i+" in ["+left_readindex+","+right_readindex+"] "+" pos: "+left_pos+" read pos: "+cur_entry.cur_index+"/"+cur_entry.size+" read: "+cur_entry.name);
		    }

		    this_output.add(t);
		    cur_entry.cur_index++;

		    if(min_non_empty_read > i && cur_entry.cur_index < cur_entry.size) {
			min_non_empty_read = i;
		    }
		}
	    }

	    left_readindex = min_non_empty_read;

	    if(left_pos >= right_entry.start_pos && right_entry.cur_index < right_entry.size) {
		// note that left_pos >= right_entry.start_pos implies final_read is true!!

		Tuple t =  right_entry.pileup.get(right_entry.cur_index);

		if(debug) {
		    if(t.get(0) != null && refbase != null)
			if(!refbase.equals((String)t.get(0)))
			    throw new IOException("mismatching rebases (final read): "+refbase+ " and "+(String)t.get(0)+" for "+right_readindex+" in ["+left_readindex+","+right_readindex+"] "+" pos: "+left_pos+" read pos: "+right_entry.cur_index+"/"+right_entry.size+" read: "+right_entry.name);
		}

		this_output.add(t);
		right_entry.cur_index++;
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
	    } else {
		left_pos = right_entry.start_pos-1; // -1 because directly afterwards it is incremented!
	    }

	    left_pos++;
	}

	//if(final_read && left_readindex != right_readindex)
        //	throw new IOException("final read but read indices don't match!: left: "+left_readindex+" right: "+right_readindex);
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
