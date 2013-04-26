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

package fi.aalto.seqpig.filter;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class SAMFlagsFilter  extends FilterFunc {

    public enum FlagTypes {
	    HasMultipleSegments,
	    IsProperlyAligned,
	    HasSegmentUnmapped,
	    NextSegmentUnmapped,
	    IsReverseComplemented,
	    NextSegmentReversed,
	    IsFirstSegment,
	    IsLastSegment,
	    HasSecondaryAlignment, 
	    FailsQC,
	    IsDuplicate 
    }

    private FlagTypes my_type;

    /* From the SAM/BAM format specification:
        0x1 template having multiple segments in sequencing
	0x2 each segment properly aligned according to the aligner
	0x4 segment unmapped
	0x8 next segment in the template unmapped
	0x10 SEQ being reverse complemented
	0x20 SEQ of the next segment in the template being reversed
	0x40 the first segment in the template
	0x80 the last segment in the template
	0x100 secondary alignment
	0x200 not passing quality controls
	0x400 PCR or optical duplicate
    */

    public SAMFlagsFilter(String filter) throws Exception {
	my_type = FlagTypes.valueOf(filter);
    }

    
    public static Boolean hasMultipleSegments(int value) {
	return new Boolean((value &  0x1) != 0);
    }
    
    public  static Boolean isProperlyAligned(int value) {
	return new Boolean((value &  0x2) != 0);
    }

    public static Boolean hasSegmentUnmapped(int value) {
	return new Boolean((value &  0x4) != 0);
    }

    public static Boolean nextSegmentUnmapped(int value) {
	return new Boolean((value &  0x8) != 0);
    }
    
    public static Boolean isReverseComplemented(int value) {
	return new Boolean((value &  0x10) != 0);
    }

    public static Boolean nextSegmentReversed(int value) {
	return new Boolean((value &  0x20) != 0);
    }
    
    public static Boolean isFirstSegment(int value) {
	return new Boolean((value &  0x40) != 0);
    }
    
    public static Boolean isLastSegment(int value) {
	return new Boolean((value &  0x80) != 0);
    }
    
    public static Boolean hasSecondaryAlignment(int value) {
	return new Boolean((value &  0x100) != 0);
    }
    
    public static Boolean failsQC(int value) {
	return new Boolean((value &  0x200) != 0);
    }
    
    public static Boolean isDuplicate(int value) {
	return new Boolean((value &  0x400) != 0);
    }

    @Override
     public Boolean exec(Tuple input) throws IOException {
	    int flags = ((Integer)input.get(0)).intValue();

	    switch (my_type) {
	    case HasMultipleSegments:
		return hasMultipleSegments(flags);
	    case IsProperlyAligned:
		return isProperlyAligned(flags);
	    case HasSegmentUnmapped:
		return hasSegmentUnmapped(flags);
	    case NextSegmentUnmapped:
		return nextSegmentUnmapped(flags);
	    case  IsReverseComplemented:
		return isReverseComplemented(flags);
	    case NextSegmentReversed:
		return nextSegmentReversed(flags);
	    case IsFirstSegment:
		return isFirstSegment(flags);
	    case IsLastSegment:
		return isLastSegment(flags);
	    case HasSecondaryAlignment:
		return hasSecondaryAlignment(flags);
	    case FailsQC:
		return failsQC(flags);
	    case IsDuplicate:
		return isDuplicate(flags);
	    }

	    return null;
    }
}
