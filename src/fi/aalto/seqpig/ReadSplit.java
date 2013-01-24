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

import it.crs4.seal.common.AlignOp;
import it.crs4.seal.common.MdOp;
import it.crs4.seal.common.WritableMapping;
import it.crs4.seal.common.AbstractTaggedMapping;
import it.crs4.seal.common.FormatException;

import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

/* UDF ReadSplit
   
 * takes a single read (created via input format from SAMRecord) and produces base data
   (read base, reference base, quality, etc.) for each reference position
*/

public class ReadSplit extends EvalFunc<DataBag>
{
    private WritableMapping mapping = new WritableMapping();
    private List<AlignOp> alignment;
    private List<MdOp> mdOps;

    private String chrom; // or reference sequence id

    private String name;
    private int start;
    private String sequence;
    private String basequal;
    private String chromosome;
    private int flags;
    private int mapqual;

    private int last_unclipped_base; // 1-based position of last unclipped base (starting from (potentially) clipped ones)
    private int read_length; // length of (potentially clipped) read
    private int read_clip_offset; // number of clipped bases at the front of the read

    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private BagFactory mBagFactory = BagFactory.getInstance();

    public ReadSplit() {
    }

    // tuple input format: (subset of BamUDFLoader output format + MD tag)
    //   name
    //   start
    //   sequence
    //   cigar
    //   base qualities
    //   flags
    //   map quality
    //   refindex
    //   refname
    //   MD tag

    // tuple output format:
    //   chromosome/reference id
    //   reference position
    //   reference base (if known, else null value)
    //   read name
    //   read length (clipped)
    //   mapping quality
    //   flags
    //   base position (inside read)
    //   base quality (if applicable else null)
   
    // WARNINGS:
    // we use the folling Pig UDF warnings:
    // 	PigWarning.UDF_WARNING_2 :	problems with parsing CIGAR string
    // 	PigWarning.UDF_WARNING_3 :	problem with parsing MD tag
    // 	PigWarning.UDF_WARNING_4 :	other problem

    // compares the length indicated by CIGAR and MD strings and CIGAR and read sequence length; returns true if these match and
    // false otherwise
    private boolean compareAndSetLengthCigarMd() {
	int insert_length = 0, clip_length = 0, match_length = 0, md_length = 0;

	Iterator<AlignOp> alignOpIt = alignment.iterator();

	last_unclipped_base = 0;
	read_clip_offset = 0;

	while(alignOpIt.hasNext()) {
	    AlignOp alignOp = alignOpIt.next();	

	    if (alignOp.getType() == AlignOp.Type.Match)
		match_length += alignOp.getLen();
	    else {
		if(alignOp.getType() == AlignOp.Type.Insert)
		    insert_length += alignOp.getLen();

		else if(alignOp.getType() == AlignOp.Type.SoftClip) {
		    if(!alignOpIt.hasNext()) // this is the final clipping!
			last_unclipped_base = match_length + insert_length + clip_length;
		    else // this is the first clipping!
			read_clip_offset = alignOp.getLen();

		    clip_length += alignOp.getLen();
		}
	    }
   	}

	for (MdOp mdOp: mdOps) {
	    if (mdOp.getType() == MdOp.Type.Match || mdOp.getType() == MdOp.Type.Mismatch) {
		md_length += mdOp.getLen();
	    }

	}

	if(last_unclipped_base == 0)
	    last_unclipped_base = match_length + insert_length + clip_length;

	read_length = match_length + insert_length;

	return ((match_length == md_length) && (match_length + insert_length + clip_length == sequence.length()));
    }

    private void setFields(Tuple tpl, int refpos, String refbase, int basepos, String readbase, int cur_basequal) 
	throws org.apache.pig.backend.executionengine.ExecException {
        tpl.set(0, chromosome);
	tpl.set(1, refpos);
        tpl.set(2, refbase);
	tpl.set(3, name);
	tpl.set(4, read_length);
	tpl.set(5, mapqual);
	tpl.set(6, flags);
	tpl.set(7, basepos);
	tpl.set(8, readbase);
	tpl.set(9, cur_basequal);
    }

    private int getBaseQuality(int baseindex) {
    	return (int)(basequal.substring(baseindex, baseindex+1).charAt(0))-33;
    }


    @Override 
	public DataBag exec(Tuple input) throws IOException, org.apache.pig.backend.executionengine.ExecException {
	if (input == null || input.size() == 0)
	    return null;
	    
	//   name
	//   start
	//   sequence
	//   cigar
	//   base qualities
	//   flags
	//   map quality
	//   refindex
	//   refname
	//   MD tag

	name = (String)input.get(0);
	start = ((Integer)input.get(1)).intValue();
	sequence = (String)input.get(2);
	basequal = (String)input.get(4);
	flags = ((Integer)input.get(5)).intValue();
	mapqual = ((Integer)input.get(6)).intValue();
	chromosome = (String)input.get(8);

	mapping.clear();
	mapping.setSequence(sequence);
	mapping.setFlag(flags);

	if (!mapping.isMapped() || input.get(8)==null) {
	    warn("encountered unmapped read or empty chromosome/contig id!", PigWarning.UDF_WARNING_4);
	    return null;
	}

	mapping.set5Position(((Integer)input.get(1)).intValue());
	mapping.setContig((String)input.get(8));

	try {
	    mapping.setAlignment(AlignOp.scanCigar((String)input.get(3)));
	    alignment = mapping.getAlignment();
	} catch(FormatException e) {
	    warn("errors parsing CIGAR string (input: " + (String)input.get(3) + "), silently IGNORING read, exception: "+e.toString(), PigWarning.UDF_WARNING_2);
	    return null;
	}

	try {
	    mapping.setTag("MD", AbstractTaggedMapping.TagDataType.String, ((String)input.get(9)).toUpperCase());
	    mdOps = MdOp.scanMdTag(((String)input.get(9)).toUpperCase());
	} catch(FormatException e) {
	    warn("errors parsing MD string (input: " + (String)input.get(9) + "), silently IGNORING read, exception: "+e.toString(), PigWarning.UDF_WARNING_3);
	    return null;
	}

	DataBag output = mBagFactory.newDefaultBag();

	// NOTE: code based on copy&paste from Seal AbstractTaggedMapping::calculateReferenceMatches

	if (mdOps.isEmpty()) {
	    warn("no MD operators extracted from tag! (tag: " + (String)input.get(9) + "), silently IGNORING read", PigWarning.UDF_WARNING_3);
	    return null;
	}

	if (!compareAndSetLengthCigarMd()) {
	    warn("CIGAR, MD and sequence lengths do not match! ignoring read! (CIGAR: " + (String)input.get(3) + " MD: " + (String)input.get(9) + " seq: " + sequence + ")", PigWarning.UDF_WARNING_1);
	    return null;
	}
		
	Iterator<MdOp> mdIt = mdOps.iterator();

	MdOp mdOp = mdIt.next();
	int mdOpConsumed = 0; // the number of positions within the current mdOp that have been consumed.
	int seqpos = 0;
	int refpos = start;

	Tuple prev_tpl = null; // used for insertions and deletions to join their records; always set to last created tuple!
		
	for (AlignOp alignOp: alignment) {
	    if (alignOp.getType() == AlignOp.Type.Match) {

		int positionsToCover = alignOp.getLen();

		while (positionsToCover > 0 && mdOp != null) {
		    if (mdOp.getType() == MdOp.Type.Delete) {

			throw new IOException("BUG or bad data?? found MD deletion while parsing CIGAR match! CIGAR: " + AlignOp.cigarStr(alignment) + "; MD: " + (String)input.get(9) + "; read: " + sequence + "; seqpos: "+seqpos+"; mdOpConsumed: "+mdOpConsumed+"; positionsToCover: "+positionsToCover);

		    } else {

			if(prev_tpl != null) {
			    output.add(prev_tpl);
			    prev_tpl = null;
			}

			// must be a match or a mismatch
			boolean match = mdOp.getType() == MdOp.Type.Match;
			int consumed = Math.min(mdOp.getLen() - mdOpConsumed, positionsToCover);

			for (int i = 0; i < consumed; i++) {

			    String refbase, readbase;
			    int cur_basequal;

			    Tuple tpl = TupleFactory.getInstance().newTuple(10);

			    if(!mapping.isOnReverse())
				readbase = sequence.substring(seqpos, seqpos+1);
			    else
				readbase = sequence.substring(seqpos, seqpos+1).toLowerCase();

			    if(match) // reference and read have matching bases
				refbase = sequence.substring(seqpos, seqpos+1);
			    else
				refbase = mdOp.getSeq().substring(i, i+1);

			    cur_basequal = getBaseQuality(seqpos);

			    setFields(tpl, refpos++, refbase, seqpos-read_clip_offset, readbase, cur_basequal);
				
			    if(i == consumed-1)
				prev_tpl = tpl;

			    output.add(tpl);
			    seqpos++;
			}

			positionsToCover -= consumed;
			mdOpConsumed += consumed;
			if (mdOpConsumed >= mdOp.getLen()) { // operator consumed.  Advance to next
			    mdOpConsumed = 0;
			    if (mdIt.hasNext())
				mdOp = mdIt.next();
			    else
				mdOp = null;
			}
		    }
		}

		if (positionsToCover > 0 && mdOp == null)
		    throw new IOException("BUG or bad data?? Found more read positions than was covered by the MD tag. CIGAR: " + AlignOp.cigarStr(alignment) + "; MD: " + (String)input.get(9) + "; read: " + sequence);
	    }

	    else if(alignOp.getType() == AlignOp.Type.Insert) {

		// NOTE!!! for now we ignore leading insertions!!!
		if(seqpos < read_clip_offset) {
		    seqpos += alignOp.getLen();
		    continue;
		}

		String refbase, readbases;

		if(prev_tpl != null)
		    refbase = (String)prev_tpl.get(2);
		else
		    refbase = null;

		prev_tpl = null;

		if(mapping.isOnReverse())
		    readbases = sequence.substring(seqpos,seqpos+alignOp.getLen()).toLowerCase();
		else
		    readbases = sequence.substring(seqpos,seqpos+alignOp.getLen());

		for(int i=0;i<alignOp.getLen();i++) {
		    String readbase = readbases.substring(i,i+1);
		    int cur_basequal = getBaseQuality(seqpos);

		    Tuple tpl = TupleFactory.getInstance().newTuple(10);

		    setFields(tpl, refpos, refbase, seqpos-read_clip_offset, readbase, cur_basequal);	
		    seqpos++;

		    output.add(tpl);
		}

	    }  else if(alignOp.getType() == AlignOp.Type.Delete) {

		if (mdIt.hasNext())
		    mdOp = mdIt.next();
		else
		    mdOp = null;

	    } else if(alignOp.getType() == AlignOp.Type.SoftClip) {

		seqpos += alignOp.getLen();

	    }
	}

	return output;
    }

    @Override
	public Schema outputSchema(Schema input) {
	try{
	    Schema bagSchema = new Schema();
	    bagSchema.add(new Schema.FieldSchema("chr", DataType.CHARARRAY));
	    bagSchema.add(new Schema.FieldSchema("pos", DataType.INTEGER));
	    bagSchema.add(new Schema.FieldSchema("refbase", DataType.CHARARRAY));
	    bagSchema.add(new Schema.FieldSchema("readname", DataType.CHARARRAY));
	    bagSchema.add(new Schema.FieldSchema("readlength", DataType.INTEGER));
	    bagSchema.add(new Schema.FieldSchema("mapqual", DataType.INTEGER));
	    bagSchema.add(new Schema.FieldSchema("flags", DataType.INTEGER));
	    bagSchema.add(new Schema.FieldSchema("basepos", DataType.INTEGER));
	    bagSchema.add(new Schema.FieldSchema("readbase", DataType.CHARARRAY));
	    bagSchema.add(new Schema.FieldSchema("basequal", DataType.INTEGER));

	    return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), bagSchema, DataType.BAG));
	}catch (Exception e){
	    return null;
	}
    }
}
