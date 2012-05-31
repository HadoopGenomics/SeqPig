#!/usr/bin/env perl

### NOTE: this is work in progress
#
# Usage:
#
# grunt> A = load 'input.bam' using fi.aalto.seqpig.BamUDFLoader('yes') AS (name:chararray, start:int, end:int, read:chararray, cigar:chararray, basequal:chararray, flags:int, insertsize:int, mapqual:int, matestart:int, indexbin:int, materefindex:int, refindex:int, attributes:map []);
# grunt> B = LIMIT A 20;
# grunt> DEFINE CMD `Pileup.pl` ship('perl/Pileup.pl');
# grunt> OP = stream B through CMD;

sub computePileup {
  my ($i, $reads) = @_;

  return scalar(@$reads);
}

@reads = ();
$prev_index = -1;
$counter = 0;
$start_index = -1;

# note: we now assume first start index of bucket then end index of bucket, then number of reads in bucketi, then read itself, then reference base then attributes are given

while (<>) {
	chomp;
	s/,/\t/g;
	my @tmpar = split /\[/;
	my @samrec = split /\t/,$tmpar[0];
	my @attributes = split /\t/, join('\t', splice(@tmpar, 1));
	my $md_val = "";
	#my @outBag = ();
	
	my $reference_bases = $samrec[16]; # reference bases that are "covered" by this read
	my $start_index = $samrec[0]; #number of reads in bucket to be processed by this UDF
	my $end_index = $samrec[1];
	my $bucket_size = $samrec[2];

	@samrec = splice(@samrec, -1*(scalar(@samrec)-3)); # cut off extra fields

        my $start_this_read = $samrec[1];

        if($prev_index == -1) {
           if($start_this_read > $start_index) {
              $prev_index = $start_this_read;
           } else {
              $prev_index = $start_index;
	   }
        }


        if(scalar(@reads) > 0) {
           # first, get rid of all reads that end before this one starts (there cannot be
           # any more reads starting before this one since they arrive in the order of
           # start coordindate by assumption)
           while(scalar(@reads) > 0 && $reads[0][2] <  $start_this_read) {
              $end_cur_read = $reads[0][2];

              foreach my $i ($prev_index..($end_cur_read+1)) {
                 my @tup=($i, computePileup($i, \@reads));
                 #push @outBag, \@tup;
                 print join(",",@tup);
	      }

              @reads = splice(@reads, -1*(scalar(@reads)-1)); # remove first read
              $prev_index = $end_cur_read + 1;
	   }

           # note: the number of "active" reads has not changed since prev_index and start_this_read,
           # so we produce the same output for this range of coordinates
           foreach my $i (prev_index..$start_this_read) {
              my @tup=($i, computePileup($i, \@reads));
              #push @outBag, \@tup;
              print join(",",@tup);
	   }

           # now place this read in list at a position according to its end coordinate
           # if it is not alreay in the list (maybe both start and end coordinate lie in
           # our bucket)
           # NOTE: we now assume that ther are NO DUPLICATES (i.e., DISTINCT has been called)
           #if samrec not in reads:
           #if not any(r for r in reads if r[0] == samrec[0] and r[1] == samrec[1] and r[2] == samrec[2]):
           #threshl = [$samrec[2]]*len(reads)
	   my @smaller_reads = ();
	   my @larger_reads = ();

	   foreach $read (@reads) {
		push @smaller_reads, $read if($$read[2] <= $samrec[2]);
		push @larger_reads, $read if($$read[2] > $samrec[2]);
	   }

           #smaller_reads = map(filterFuncSmall, reads, threshl)
           #larger_reads = map(filterFuncLarge, reads, threshl)
           #
           push @smaller_reads, \@samrec;
	   @reads = @smaller_reads;
	   push @reads, @larger_reads;

           #smaller_reads.append(samrec)
           #smaller_reads.extend(larger_reads)
           #reads = filter(None, smaller_reads)
        } else {
           push @reads, \@samrec;
	}

        #counter = counter + 1
        $counter++;

        if($start_this_read > $prev_index) {
           $prev_index = $start_this_read;
	}

        # if this is the last read of the bucket, clear the reads list in order of end
        # coordinate and produce output
        if($counter == $bucket_size) {
           while(scalar(@reads) > 0) {
              $end_cur_read = $reads[0][2];

              if($end_cur_read >= $end_index) {
                 $end_cur_read = $end_index - 1;
	      }

	      foreach my $i ($prev_index..($end_cur_read+1)) {
                 @tup=($i, computePileup($i, \@reads));
		 print join(",",@tup);
                 #outBag.append(tup)
              }

              #reads = reads[1:]
	      @reads = splice(@reads, -1*(scalar(@reads)-1)); # remove first read
              $prev_index = $end_cur_read + 1;

              if($prev_index >= $end_index) {
                 break;
	      }
           }
        }
}
