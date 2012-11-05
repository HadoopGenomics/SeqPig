#!/usr/bin/perl -w

# simple script to convert a reference sequence in fasta format into a file
# with three columns:
#
# <reference_name>	<position>	<reference bases>
#
# here reference name can be a chromosome or contig name. Each line corresponds to a single line
# in the fasta file. Note that this file has the advantage of being splittable.

my $curpos = 1;
my $linectr = 1;
my $curchrom_length = -1;
my $curchrom_name = "";

while($_ = <STDIN>) {
        chomp($_);
# >1 dna:chromosome chromosome:GRCh37:1:1:249250621:1
        if (/^\>([\w\.]+)\s.*\:(\d+)\:1$/) {  # found new chromosome
		if($curchrom_length > 0 && $curpos != $curchrom_length) {
			print STDERR "warning: number of bases does not match chromosome length!\n$curpos found vs $curchrom_length announced\n"
		}
		print STDERR "found beginning of chromosome/contig $1 of length: $2\n";
		$curpos = 1;
		$curchrom_length = $2+1;
		$curchrom_name = $1;
	} else {
	
		if($curchrom_length < 0) {
			print STDERR "warning: did not find beginning of chromosome!";
			$curchrom_length = 0;
		}
	
		if(/^[A-Z]{1,80}$/) {
			print "$curchrom_name\t$curpos\t$_\n";
                } else {
			print STDERR "warning: line: $linectr, pos: $curpos could not parse input: $_\n";
		}
		$curpos += length($_);
	}
	$linectr++;
}
