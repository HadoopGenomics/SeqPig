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
import java.util.Map;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Set;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.pig.FilterFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.ResourceSchema;
import org.apache.pig.impl.util.UDFContext;

import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMTextHeaderCodec;
import net.sf.samtools.SAMTagUtil;
import net.sf.samtools.SAMReadGroupRecord;
import net.sf.samtools.SAMProgramRecord;
import net.sf.samtools.SAMSequenceRecord;
import net.sf.samtools.SAMFileReader.ValidationStringency;
import net.sf.samtools.util.StringLineReader;

import org.apache.commons.codec.binary.Base64;

// tuple input format:
    //   chr
    //   position
    //   flag
    //   mapping quality

public class MappabilityFilter extends FilterFunc {

    class RegionEntry {
	public int index=-1;
	public int start=-1;
	public int end=-1;

	public RegionEntry(int i, int s, int e) {
		index = i;
		start = s;
		end = e;
	}

	public String toString() {
		String ret = "region: ("+index+","+start+","+end+")";
		return ret;
	}
    }

    protected String samfileheader = null;
    protected SAMFileHeader samfileheader_decoded = null;
    protected TreeMap<RegionEntry,Boolean> regions = null;

    public MappabilityFilter() {
	decodeSAMFileHeader();
	decodeRegions();
    }

    public MappabilityFilter(String samfileheaderfilename, String regionfilename, double mappability_threshold) throws  ExecException {

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
	    System.out.println("MappabilityFilter: ERROR: problems with filesystem config?");
	    System.out.println("exception was: "+e.toString());
	}
	    
	if(samfileheader == null) {

	    this.samfileheader = "";

	    try {
		BufferedReader headerin = new BufferedReader(new InputStreamReader(fs.open(new Path(fs.getHomeDirectory(), new Path(samfileheaderfilename)))));
		
		while(true) {
		    str = headerin.readLine();
		    
		    if(str == null) break;
		    else
			this.samfileheader += str + "\n";
		}
		
		headerin.close();
		
	    } catch (Exception e) {
		System.out.println("MappabilityFilter: ERROR: could not read BAM header from file "+samfileheaderfilename);
		System.out.println("exception was: "+e.toString());
	    }
	}

	if(regions == null) {

	    regions = new TreeMap<RegionEntry,Boolean>();

	    try {
		BufferedReader regionin = new BufferedReader(new InputStreamReader(fs.open(new Path(fs.getHomeDirectory(), new Path(regionfilename)))));
		
		regionin.readLine(); // throw away first line that describes colums 

		while(true) {
		    str = regionin.readLine();
		    
		    if(str == null) break;
		    else {
			String[] region_data = str.split("\t");
			
			if(region_data[0] == null || region_data[1] == null || region_data[2] == null || region_data[3] == null) {
			    int errCode = 0;
			    String errMsg = "MappabilityFilter: Error while reading region file input";
			    
			    throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT, e);
			}
			
			if(Double.parseDouble(region_data[3]) >= mappability_threshold) {
			    
			    SAMSequenceRecord ref = samfileheader_decoded.getSequence(region_data[0]);
			    
			    if (ref == null)
				try {
				    ref = samfileheader_decoded.getSequence(Integer.parseInt(refStr));
				} catch (NumberFormatException e) {}
			    
			    if (ref == null) {
				int errCode = 0;
				String errMsg = "MappabilityFilter: Unable find sequence record for region: "+str;
				
				throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT, e);
			    }
			    
			    RegionEntry e = new RegionEntry(ref.getSequenceIndex(), parseCoordinate(region_data[1]), parseCoordinate(region_data[2]));
			    regions.put(e, new Boolean(true));
			}
		    }
		}
		
		regionin.close();
		
	    } catch (Exception e) {
		System.out.println("ERROR: could not read BAM header from file "+samfileheaderfilename);
		System.out.println("exception was: "+e.toString());
	    }
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

	    bstream = new ByteArrayOutputStream();
	    ostream = new ObjectOutputStream(bstream);
	    ostream.writeObject(this.regions);
	    ostream.close();
	    datastr = codec.encodeBase64String(bstream.toByteArray());
	    p.setProperty("regiontree", datastr);
	} catch (Exception e) {
	    System.out.println("ERROR: Unable to store SAMFileHeader or RegionTree in MappabilityFilter!");
	}
	
	this.samfileheader_decoded = getSAMFileHeader();
    }

    protected void decodeSAMFileHeader() throws Exception {
	Base64 codec = new Base64();
	Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
	String datastr;
	
	datastr = p.getProperty("samfileheader");
	byte[] buffer = codec.decodeBase64(datastr);
	ByteArrayInputStream bstream = new ByteArrayInputStream(buffer);
	ObjectInputStream ostream = new ObjectInputStream(bstream);
	
	this.samfileheader = (String)ostream.readObject();

        this.samfileheader_decoded = getSAMFileHeader();
    }

    protected void decodeRegions() throws Exception {
	Base64 codec = new Base64();
	Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
	String datastr;
	
	datastr = p.getProperty("regiontree");
	byte[] buffer = codec.decodeBase64(datastr);
	ByteArrayInputStream bstream = new ByteArrayInputStream(buffer);
	ObjectInputStream ostream = new ObjectInputStream(bstream);
	
	this.regions = (TreeMap)ostream.readObject();
    }

    private SAMFileHeader getSAMFileHeader() {
	final SAMTextHeaderCodec codec = new SAMTextHeaderCodec();
	codec.setValidationStringency(ValidationStringency.SILENT);
	return codec.decode(new StringLineReader(this.samfileheader), "SAMFileHeader.clone");
    }

    private int parseCoordinate(String s) {
	int c;
	try {
	    c = Integer.parseInt(s);
	} catch (NumberFormatException e) {
	    c = -1;
	}
	if (c < 0)
	    System.err.printf("MappabilityFilter: Not a valid coordinate: '%s'\n", s);
	return c;
    }    

    // tuple input format:
    //   chrom index
    //   start position
    //   end postition

    @Override
    public Boolean exec(Tuple input) throws IOException {
        try {
	    int chrom = ((Integer)input.get(0)).intValue();
	    int start_pos = ((Integer)input.get(1)).intValue();
	    int end_pos = ((Integer)input.get(2)).intValue();
	    
	    for(RegionEntry entry : regions) {
		if(entry.index == chrom && entry.start <= start_pos && entry.end >= end_pos)
			return true;
	    }

	    return false;

        } catch (ExecException ee) {
            throw ee;
        }
    }
} 
