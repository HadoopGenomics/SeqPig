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
import java.util.HashMap;
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

public class CoordinateFilter extends FilterFunc {

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

    protected List<RegionEntry> regions = null;
    protected String regions_str = null;

    public CoordinateFilter() {
	decodeSAMFileHeader();
	decodeRegions();
    }

    public CoordinateFilter(String samfileheaderfilename, String regions_str) {
	String str = "";
	this.samfileheader = "";

        this.regions_str = regions_str;

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
	    p.setProperty("regionsstr", regions_str);
	} catch (Exception e) {
	    System.out.println("ERROR: Unable to store SAMFileHeader in CoordinateFilter!");
	}

	this.samfileheader_decoded = getSAMFileHeader();
	populateRegions();
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

    protected void decodeRegions(){
        try {
            Base64 codec = new Base64();
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
	    this.regions_str = p.getProperty("regionsstr");

	    //System.err.println("decoded regions!");
        } catch (Exception e) {
	    //System.err.println("error: "+e.toString());
        }

        populateRegions();
    }

    private SAMFileHeader getSAMFileHeader() {
	final SAMTextHeaderCodec codec = new SAMTextHeaderCodec();
	codec.setValidationStringency(ValidationStringency.SILENT);
	return codec.decode(new StringLineReader(this.samfileheader), "SAMFileHeader.clone");
    }

    // note: most of this is shamelessly copied from hadoop-bam
    private void populateRegions() {
	StringTokenizer rst = new StringTokenizer(regions_str, ",");
	boolean errors = false;
	
	regions = new ArrayList();
	int ctr = 0;

	while(rst.hasMoreTokens()) {
		String region = rst.nextToken();

	        final StringTokenizer st = new StringTokenizer(region, ":-");
                final String refStr = st.nextToken();
                final int beg, end;

                if (st.hasMoreTokens()) {
                	beg = parseCoordinate(st.nextToken());
                      	end = st.hasMoreTokens() ? parseCoordinate(st.nextToken()) : -1;

                      	if (beg < 0 || end < 0 || end < beg) {
                      		errors = true;
                                continue;
                        }
                } else
              		beg = end = 0;

                SAMSequenceRecord ref = samfileheader_decoded.getSequence(refStr);
                
		if (ref == null) try {
                	ref = samfileheader_decoded.getSequence(Integer.parseInt(refStr));
                } catch (NumberFormatException e) {}

                if (ref == null) {
                                //System.err.printf(
                                 //       "view :: Not a valid sequence name or index: '%s'\n", refStr);
                	errors = true;
                        continue;
               	}

		RegionEntry e = new RegionEntry(ref.getSequenceIndex(), beg, end);
		regions.add(e);
		//System.err.println(e.toString());
		ctr++;
    	}

	//System.err.println("decoded "+ctr+" regions!");
    }

    private int parseCoordinate(String s) {
                int c;
                try {
                        c = Integer.parseInt(s);
                } catch (NumberFormatException e) {
                        c = -1;
                }
                if (c < 0)
                        System.err.printf("CoordinateFilter :: Not a valid coordinate: '%s'\n", s);
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
