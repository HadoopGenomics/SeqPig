#!/usr/bin/env perl

### NOTE: this is work in progress
#
# Usage:
#
# grunt> A = load 'input.bam' using fi.aalto.seqpig.BamUDFLoader('yes') AS (name:chararray, start:int, end:int, read:chararray, cigar:chararray, basequal:chararray, flags:int, insertsize:int, mapqual:int, matestart:int, indexbin:int, materefindex:int, refindex:int, attributes:map []);
# grunt> B = LIMIT A 20;
# grunt> DEFINE CMD `Pileup.pl` ship('perl/Pileup.pl');
# grunt> OP = stream B through CMD;


# adapted from Bio::DB::Bam::AlignWrapper
sub getRefBases {
    my ($qseq, $cigar_str, $md) = @_;

    my @cigar;
    my @cigart = split /(\d+[IDCHM])/, $cigar_str;
    map { my $temp = $_; $temp =~ /(\d+[IDCHM])/; if (defined($1)) { push @cigar, $1; } } @cigart;

    if ($md ne "") {  # try to use MD string

        #preprocess qseq using cigar array
        my $seq   = '';
        foreach (@cigar) {
	    m/(\d+)([IDCHM])/;
            my ($operation,$count) = ($2, $1);
            
	    if ($operation eq 'M') {
                $seq .= substr($qseq,0,$count,''); # include these residues
            } elsif ($operation eq 'S' or $operation eq 'I') {
                substr($qseq,0,$count,'');         # skip soft clipped and inserted residues
            }
        }

        my $start = 0;
        my $result;
        while ($md =~ /(\d+)|\^([gatcn]+)|([gatcn]+)/ig) {
            if (defined $1) {
                $result .= substr($seq,$start,$1);
                $start  += $1;
            } elsif ($2) {
                $result .= $2;
            } elsif ($3) {
                $result .= $3;
                $start  += length $3;
            }
        }
        return $result;
    }
}

# example input line:
# SRR096689.6740187,115327100,115327149,GAGGAGAATAACTACACTTTAATTTTTTTTTAATTACAGTCACAAGTGAC,50M,:;::=;>?>=@<A>=B=AAA?@><<AAA;<A<!!2.5!!/3=!!129!!S,1024,0,96,0,11720,-1,10,[AS#2000,RG#0,HI#1,CS#T12202220330123111200303000000003000311112110021131,OQ#L````````````````````_^TS```ST`_!!9;T!!9O\!!@N[!!(,XA#3,NH#4,XE#---------------------------------0----1----0----3-,NM#0,CQ#!BBBBBBBBBABABBABAB??A5@+?BA=->=9%*&,?1%+;8@%23?5(,PG#bfast,IH#1,CM#4,MD#50]


while (<>) {
	chomp;
	s/,/\t/g;
	my @tmpar = split /\[/;
	my @samrec = split /\t/,$tmpar[0];
	my @attributes = split /\t/, join('\t', splice(@tmpar, 1));
	my $md_val = "";
	my $name = $samrec[0];
	my $cigar = $samrec[4];

	foreach (@attributes) {
		if(/^MD\#/) {
			s/\]//g;
			$md_val = substr $_,3;
			break;	
		}
	}

	my $ref_bases = getRefBases($samrec[3],$cigar,$md_val);

	print join(",",@samrec).",".$ref_bases.",".join(",",@attributes)."\n";
}
