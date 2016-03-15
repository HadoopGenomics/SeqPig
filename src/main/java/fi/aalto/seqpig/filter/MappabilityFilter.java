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

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.util.StringLineReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.FilterFunc;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.TreeMap;

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

    public MappabilityFilter(String mappability_threshold_s) throws  Exception {
        double mappability_threshold = Double.parseDouble(mappability_threshold_s);
	Configuration conf = UDFContext.getUDFContext().getJobConf();
	   
        // see https://issues.apache.org/jira/browse/PIG-2576
        if(conf == null || conf.get("mapred.task.id") == null) {
       	    // we are running on the frontend
            //decodeSAMFileHeader();
            return;
        }
 
	if(samfileheader == null) {
	    this.samfileheader = "";
	    try {
	        FileSystem fs = FileSystem.getLocal(conf);
		BufferedReader headerin = new BufferedReader(new InputStreamReader(fs.open( new Path("input_asciiheader"))));
			
		while(true) {
		    String str = headerin.readLine();
		    if(str == null) break;
		    else
			this.samfileheader += str + "\n";
		}
		headerin.close(); 

		if(this.samfileheader.equals("")) {
		    int errCode = 0;
		    String errMsg = "MappabilityFilter: unable to read samfileheader from distributed cache!";
		    throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT);
		} else {
		    String msg = "successfully read samfileheader";
		    warn(msg, PigWarning.UDF_WARNING_1);
		    this.samfileheader_decoded = getSAMFileHeader();
		}
	    } catch (Exception e) {
		String errMsg = "MappabilityFilter: ERROR: could not read BAM header: "+e.toString();
		int errCode = 0;
		throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT);
	    }
	}

	if(regions == null) {
	    regions = new TreeMap<RegionEntry,Boolean>();
	    try {
		FileSystem fs = FileSystem.getLocal(conf);
		// this was required before creating symlinks to files in the distributed cache
                /*Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
		  for (Path cachePath : cacheFiles) {
		  String msg = "found cache file: "+cachePath.getName();
		  warn(msg, PigWarning.UDF_WARNING_1);
		  if (cachePath.getName().equals("input_regionfile")) {
		  BufferedReader regionin = new BufferedReader(new InputStreamReader(fs.open( cachePath )));
		  BufferedReader regionin = new BufferedReader(new InputStreamReader(fs.open(new Path(fs.getHomeDirectory(), new Path(regionfilename)))));'*/
		BufferedReader regionin = new BufferedReader(new InputStreamReader(fs.open( new Path("./input_regionfile"))));	
		regionin.readLine(); // throw away first line that describes colums 

		while(true) {
		    String str = regionin.readLine();
		    
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
	    } catch (Exception e) {
		String errMsg = "MappabilityFilter: ERROR: could not region file: "+e.toString();
                int errCode = 0;

                throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT);
	    }
	}
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
