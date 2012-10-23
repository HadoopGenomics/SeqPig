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
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.pig.FilterFunc;
import org.apache.pig.PigWarning;
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

    class RegionEntry implements Comparable<RegionEntry> {
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

	public int compareTo( RegionEntry other) {
	    if(this.index == other.index) {
		if(this.start == other.start)
		    return (this.end - other.end);
		else
		    return (this.start - other.start);
	    } else
		return (this.index - other.index);
	}
    }

    protected String samfileheader = null;
    protected SAMFileHeader samfileheader_decoded = null;
    protected TreeMap<RegionEntry,Boolean> regions = null;

    protected int region_size = -1;

    /*public MappabilityFilter() throws Exception {
	decodeSAMFileHeader();
	decodeRegions();
    }*/

    public MappabilityFilter(String mappability_threshold_s) throws  Exception {

        double mappability_threshold = Double.parseDouble(mappability_threshold_s);

	Configuration conf = UDFContext.getUDFContext().getJobConf();
	    
	if(conf == null) {
	    //decodeSAMFileHeader();
	    return;
	}

	/*try {
	    if(FileSystem.getDefaultUri(conf) == null
	       || FileSystem.getDefaultUri(conf).toString() == "")
		fs = FileSystem.get(new URI("hdfs://"), conf);
	    else 
		fs = FileSystem.get(conf);
	} catch (Exception e) {
	    fs = FileSystem.get(new URI("hdfs://"), conf);
	    System.out.println("MappabilityFilter: ERROR: problems with filesystem config?");
	    System.out.println("exception was: "+e.toString());
	    }*/
	    
	if(samfileheader == null) {

	    this.samfileheader = "";

	    try {
		//BufferedReader headerin = new BufferedReader(new InputStreamReader(fs.open(new Path(fs.getHomeDirectory(), new Path(samfileheaderfilename)))));

	        FileSystem fs = FileSystem.getLocal(conf);
		
		/*Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf); 
		
		for (Path cachePath : cacheFiles) {
		    String msg = "found cache file: "+cachePath.getName();
                    warn(msg, PigWarning.UDF_WARNING_1);


		    if (cachePath.getName().equals("input_asciiheader")) {
			BufferedReader headerin = new BufferedReader(new InputStreamReader(fs.open( cachePath )));*/

		        BufferedReader headerin = new BufferedReader(new InputStreamReader(fs.open( new Path("input_asciiheader"))));
			
			while(true) {
			    String str = headerin.readLine();

			    //System.out.println("read: "+str);
			    
			    if(str == null) break;
			    else
				this.samfileheader += str + "\n";
			}
			
			headerin.close();
		    //}
		//} 

		if(this.samfileheader.equals("")) {
			int errCode = 0;
                        String errMsg = "MappabilityFilter: unable to read samfileheader from distributed cache!";

                        throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT);
                        //warn(errMsg, PigWarning.UDF_WARNING_1);
		} else {
			String msg = "successfully read samfileheader";
                        warn(msg, PigWarning.UDF_WARNING_1);

			this.samfileheader_decoded = getSAMFileHeader();
		}

	    } catch (Exception e) {
		//warn(new String("MappabilityFilter: ERROR: could not read BAM header: "+e.toString()), PigWarning.UDF_WARNING_1); // from file "+samfileheaderfilename);
		String errMsg = "MappabilityFilter: ERROR: could not read BAM header: "+e.toString();
		int errCode = 0;

		throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT);
	    }
	}

	if(regions == null) {

	    regions = new TreeMap<RegionEntry,Boolean>();

	    try {

		FileSystem fs = FileSystem.getLocal(conf);

                /*Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);

                for (Path cachePath : cacheFiles) {
		    String msg = "found cache file: "+cachePath.getName();
		    warn(msg, PigWarning.UDF_WARNING_1);

                    if (cachePath.getName().equals("input_regionfile")) {
                        BufferedReader regionin = new BufferedReader(new InputStreamReader(fs.open( cachePath )));*/


			//BufferedReader regionin = new BufferedReader(new InputStreamReader(fs.open(new Path(fs.getHomeDirectory(), new Path(regionfilename)))));'


			BufferedReader regionin = new BufferedReader(new InputStreamReader(fs.open( new Path("./input_regionfile"))));
		
			regionin.readLine(); // throw away first line that describes colums 

			while(true) {
		    		String str = regionin.readLine();
				//System.out.println("read: "+str);
		    
		    		if(str == null) break;
		    		else {
					String[] region_data = str.split("\t");

					if(region_data[0] == null || region_data[1] == null || region_data[2] == null || region_data[3] == null) {
			    			int errCode = 0;
			    			String errMsg = "MappabilityFilter: Error while reading region file input";
			    
			    			throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT);
					}
			
					if(Double.parseDouble(region_data[3]) >= mappability_threshold) {
			    
			    			int start = parseCoordinate(region_data[1]);
			    			int end = parseCoordinate(region_data[2]);

			    			if(end < start) {
                            				int errCode = 0;
                            				String errMsg = "MappabilityFilter: Error while reading region file input";

                            				throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT);
                            			}

			    			if(region_size < 0) {
							region_size = end - start + 1;
							warn("setting region size to "+Integer.toString(region_size), PigWarning.UDF_WARNING_1);
						}

			    			SAMSequenceRecord ref = samfileheader_decoded.getSequence(region_data[0]);
			    
			    			if (ref == null)
						try {
				    			ref = samfileheader_decoded.getSequence(Integer.parseInt(region_data[0]));
						} catch (NumberFormatException e) {
							warn(new String("unable to parse region entry!"), PigWarning.UDF_WARNING_1);
						}
			    
			    			if (ref == null) {
							int errCode = 0;
							String errMsg = "MappabilityFilter: Unable find sequence record for region: "+str;
				
							throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT);
			    			}
			    
			    			RegionEntry e = new RegionEntry(ref.getSequenceIndex(), start, end);
			    			regions.put(e, new Boolean(true));
					}
		    		}
			}
		
			regionin.close();

			String msg = "successfully read region file with "+regions.size()+" regions";
			warn(msg, PigWarning.UDF_WARNING_1);
		//}
		
	      //}
	   } catch (Exception e) {
		String errMsg = "MappabilityFilter: ERROR: could not region file: "+e.toString();
                int errCode = 0;

                throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT);
	   }
	}
	    
	/*try {
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
	
	this.samfileheader_decoded = getSAMFileHeader();*/
    }

    /*protected void decodeSAMFileHeader() throws Exception {
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

	RegionEntry firstk = regions.firstKey();

	if(region_size < 0)
	    region_size = firstk.end - firstk.start;
    }*/

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
	    
	    int start_region_index = (int)Math.floor((double)start_pos/((double)region_size));
	    int end_region_index = (int)Math.floor((double)end_pos/((double)region_size));

	    System.out.println("#regions: "+regions.size());
	    System.out.println("current: "+chrom+" "+start_region_index*region_size+" "+end_region_index*region_size);

	    if(start_region_index == end_region_index) {
		RegionEntry entry = new RegionEntry(chrom, (start_region_index*region_size)+1, (start_region_index+1)*region_size);

		return (regions.get(entry) != null);
	    } else {
		// now start and end fall into different regions

		RegionEntry start_entry = new RegionEntry(chrom, (start_region_index*region_size)+1, (start_region_index+1)*region_size);
		RegionEntry end_entry = new RegionEntry(chrom, (end_region_index*region_size)+1, (end_region_index+1)*region_size);

		return ((regions.get(start_entry) != null) && (regions.get(end_entry) != null));
	    }

        } catch (ExecException ee) {
            throw ee;
        }
    }
} 
