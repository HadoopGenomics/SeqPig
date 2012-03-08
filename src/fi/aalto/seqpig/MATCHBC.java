
package fi.aalto.seqpig;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

public class MATCHBC extends EvalFunc<Integer>
{
    String[] barcodes = {"GCCAAT", "CAGATC", "ACAGTG"};

    //private ArrayList<Object> mProtoTuple = null;
    //private TupleFactory mTupleFactory = TupleFactory.getInstance();

    public Integer exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try{

	    /*if (mProtoTuple == null) {
                mProtoTuple = new ArrayList<Object>();
            }*/

            String name = (String)input.get(0);
	    String sequence = (String)input.get(1);
	    String barcode = sequence.substring(sequence.length()-7,sequence.length()-1);

	    int mindist = 1000;
	    int minindex = -1;

	    for(int i=0;i<barcodes.length;i++) {
		if(barcodes[i].equals(barcode)) {
			minindex = i;
			mindist = 0;
		}
	    }

	    //mProtoTuple.add(new String());

	    /*Tuple t =  mTupleFactory.newTupleNoCopy(mProtoTuple);
            mProtoTuple = null;
            return t;*/
            return new Integer(minindex);

        }catch(Exception e){
            throw WrappedIOException.wrap("Caught exception processing input row ", e);
        }
    }
}
