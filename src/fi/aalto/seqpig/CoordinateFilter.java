
package fi.aalto.seqpig;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.FilterFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;

import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMTextHeaderCodec;
import net.sf.samtools.SAMTagUtil;
import net.sf.samtools.SAMReadGroupRecord;
import net.sf.samtools.SAMProgramRecord;
import net.sf.samtools.SAMFileReader.ValidationStringency;
import net.sf.samtools.util.StringLineReader;

// tuple input format:
    //   chr
    //   position
    //   flag
    //   mapping quality

public class CoordinateFilter extends FilterFunc {

    protected String samfileheader = null;
    protected SAMFileHeader samfileheader_decoded = null;

    public CoordinateFilter() {
	decodeSAMFileHeader();
    }

    public CoordinateFilter(String samfileheaderfilename) {
	String str = "";
	this.samfileheader = "";

	try {
	    Configuration conf = UDFContext.getUDFContext().getJobConf();
	    
	    if(conf == null) {
		decodeSAMFileHeader();
		return;
	    }

            FileSystem fs;
	    
	    try {
		if(FileSystem.getDefaultUri(conf) == null
		   || FileSystem.getDefaultUri(conf).toString() == "")
		    fs = FileSystem.get(new URI("hdfs://"), conf);
		else 
		    fs = FileSystem.get(conf);
	    } catch (Exception e) {
		fs = FileSystem.get(new URI("hdfs://"), conf);
            	System.out.println("WARNING: problems with filesystem config?");
            	System.out.println("exception was: "+e.toString());
	    }
	    
	    BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(fs.getHomeDirectory(), new Path(samfileheaderfilename)))));
	    
	    while(true) {
		str = in.readLine();
		
		if(str == null) break;
		else
		    this.samfileheader += str + "\n";
	    }
	    
	    in.close();
	} catch (Exception e) {
	    System.out.println("ERROR: could not read BAM header from file "+samfileheaderfilename);
	    System.out.println("exception was: "+e.toString());
	}

	try {
	    Base64 codec = new Base64();
	    Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
	    
	    ByteArrayOutputStream bstream = new ByteArrayOutputStream();
	    ObjectOutputStream ostream = new ObjectOutputStream(bstream);
	    ostream.writeObject(this.samfileheader);
	    ostream.close();
	    String datastr = codec.encodeBase64String(bstream.toByteArray());
	    p.setProperty("samfileheader", datastr);
	} catch (Exception e) {
	    System.out.println("ERROR: Unable to store SAMFileHeader in CoordinateFilter!");
	}

	this.samfileheader_decoded = getSAMFileHeader();
    }

    protected void decodeSAMFileHeader(){
	try {
            Base64 codec = new Base64();
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
            String datastr;

            datastr = p.getProperty("samfileheader");
            byte[] buffer = codec.decodeBase64(datastr);
            ByteArrayInputStream bstream = new ByteArrayInputStream(buffer);
            ObjectInputStream ostream = new ObjectInputStream(bstream);

            this.samfileheader = (String)ostream.readObject();
        } catch (Exception e) {
        }

        this.samfileheader_decoded = getSAMFileHeader();
    }

    private SAMFileHeader getSAMFileHeader() {
	final SAMTextHeaderCodec codec = new SAMTextHeaderCodec();
	codec.setValidationStringency(ValidationStringency.SILENT);
	return codec.decode(new StringLineReader(this.samfileheader), "SAMFileHeader.clone");
    }

    @Override
    public Boolean exec(Tuple input) throws IOException {
        try {
            /*Object values = input.get(0);
            if (values instanceof DataBag)
                return ((DataBag)values).size() >= csize;
            else if (values instanceof Map)
                return ((Map)values).size() >= csize;
            else {
                int errCode = 2102;
                String msg = "Cannot test a " +
                DataType.findTypeName(values) + " for emptiness.";
                throw new ExecException(msg, errCode, PigException.BUG);
		}*/

	    Integer chrom = (Integer)input.get(0);
	    Integer pos = (Integer)input.get(1);

	    return ( chrom.intValue() == 0 && pos.intValue() < 10000 );

        } catch (ExecException ee) {
            throw ee;
        }
    }
} 
