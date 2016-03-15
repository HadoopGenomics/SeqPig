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

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;

// computes the reverse complement of the given base sequence
public class ReverseComplement extends EvalFunc<String>
{
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try{

	    String sequence = (String)input.get(0);
            char[] reverseStringArray = new char[sequence.length()];

            for (int i = sequence.length() - 1, j = 0; i != -1; i--, j++) {
                 char c = sequence.charAt(i);

                 switch(c) {
			case 'A':
				reverseStringArray[j] = 'T';
				break;
			case 'T':
				reverseStringArray[j] = 'A';
				break;
			case 'G':
				reverseStringArray[j] = 'C';
				break;
			case 'C':
				reverseStringArray[j] = 'G';
				break;
			default:
				reverseStringArray[j] = c;
		 }
            }
            
            return new String(reverseStringArray);
        } catch(Exception e){
            throw WrappedIOException.wrap("Caught exception processing input row ", e);
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
	try{
	    Schema tupleSchema = new Schema();
	    tupleSchema.add(new Schema.FieldSchema("reversed_read", DataType.CHARARRAY));
	    
	    return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), tupleSchema, DataType.TUPLE));
	} catch (Exception e){
	    return null;
	}
    }
}
