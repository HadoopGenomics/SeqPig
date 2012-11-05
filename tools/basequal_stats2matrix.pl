#!/usr/bin/perl -w

use strict;

# converts the output of basequal_stats.pig to a matrix that
# can then be plotted via plot_basequal_stats.R

if(@ARGV != 2) {
	print "usage: $0 <input_basequal_data> <output_matrix_file>\n";
} else {
	my @DATA;

	my $max_read_length = 0;
	my $min_qual_value = 10000;
	my $max_qual_value = -1;

	open(INPUTF, $ARGV[0]);
	open(OUTPUTF, ">$ARGV[1]");

	while(<INPUTF>) {
		chomp($_);
		s/[\{\}]//g;

		my @line_record;
		my $base_counter = 0;

		while(/\,{0,1}(\(\d+\,\d+\,\d+\))/g) {
        		my $record = $1;
			$record =~ s/[\(\)]//g;
			print "record: $record\n";
			my @fields = split(',', $record);

			$max_read_length = $fields[0]
				if($fields[0] > $max_read_length);

			$max_qual_value = $fields[1]
				if($fields[1] > $max_qual_value);			
			$min_qual_value = $fields[1]
                                if($fields[1] < $min_qual_value);

			$base_counter += $fields[2];
	
			push @line_record, [$fields[1], $fields[2]];	
    		}

		print "found $base_counter bases\n";

		push @DATA, \@line_record;
	}

	for(my $i=0;$i<=$max_read_length;$i++) {
		my @line_record = @{$DATA[$i]};

		my $j=0;
		my $cur_qual_value = $min_qual_value;

		while($cur_qual_value <= $max_qual_value) {
			if($j<@line_record+0) {
				my @cur_entry = @{$line_record[$j]};
			
				while($cur_qual_value < $cur_entry[0]) {
					print OUTPUTF "0 ";
					$cur_qual_value++;
				}

				print OUTPUTF "$cur_entry[1]\n" if($cur_qual_value == $max_qual_value);
				print OUTPUTF "$cur_entry[1] " if($cur_qual_value < $max_qual_value);
				$j++;
				$cur_qual_value++;
			} else {
				print OUTPUTF "0\n" if($cur_qual_value == $max_qual_value);
                                print OUTPUTF "0 " if($cur_qual_value < $max_qual_value);
                                $cur_qual_value++;
			}
		}
	}

	print "read: ".($max_read_length+1)." x ".($max_qual_value - $min_qual_value + 1)." values\n";
}
