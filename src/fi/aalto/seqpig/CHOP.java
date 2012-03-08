
package fi.aalto.seqpig;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

public class CHOP extends EvalFunc<String>
{
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try{

	    String sequence = (String)input.get(0);
	    int choplength = ((Integer)input.get(1)).intValue();
	    String chopseq = sequence.substring(0,sequence.length()-choplength);

            return new String(chopseq);

        }catch(Exception e){
            throw WrappedIOException.wrap("Caught exception processing input row ", e);
        }
    }
}
