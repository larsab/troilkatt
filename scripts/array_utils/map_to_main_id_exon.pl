#!/usr/bin/perl
# Maps the second column of a pcl file to using a 2 column map file
#
use strict;
if (scalar @ARGV<3){
    print "Usage ./translate_to_main_id.pl <map_file> <update_flag(0/1)> pcl_files\n";
    print "Set update_flag to 1 if existing files should be overwritten, 0 otherwise\n";
    exit;
}
my $map_file=shift(@ARGV);
my $update=shift(@ARGV);
if ($update ne "0" and $update ne "1"){
    print "pleas set the update_flag to 0 or 1\n";
exit
}

my @array;
my %map;
my %hgcn;

open (MAP, "$map_file") or die "can't open $map_file: $!\n";
print "reading map\n";
while (<MAP>){
	chomp;
	@array=split(/\t/, $_);
	$array[1]=~s/\*//;
	$array[0]=~s/^[^:]*://; #get rid of platform info
	#print "$array[0]\n" if $.<100;
	push(@{$map{$array[0]}},$array[1]) if $array[1] ne "";
}


$"="\t";


my $file;
my $probe;
my $exon;
open (LOG, ">not_found");
my $nf_count=0;
foreach $file (@ARGV){
    $.=0; 
   $nf_count=0;
    if (! -e $file.".map" or $update){
	print "mapping $file\n";
	open (DATA, "$file") or die "can't open data";
	open (OUT, ">$file.map");
	$.=0;
      LOOP: while (<DATA>){
	    if ($.>1 and !/EWEIGHT/){
		chomp;
		@array=split(/\t/, $_);
		$probe=shift(@array);  
		shift(@array);
		shift(@array);
		$probe=~s/^[^:]*://; #get rid of platform info
		#print "$array[0]\n" if $.<100;	
		if (defined $map{$probe}){
		    foreach $exon (@{$map{$probe}}){
			print OUT "$exon\t$exon";
			print OUT "\t" if scalar(@array)>0;
			print OUT "@array\n";	    	
		    }    
	    }
		else{
		    if ($nf_count>100000){
			last LOOP;
		    }
		    $nf_count++;
		    print LOG "$file\t$probe\n";
		}
	    }
	    else{
		print OUT "\t$_";
	    }  
    }
	if ($nf_count>100000){
	    print LOG "no map for $file\n";
	    #system "rm $file.map";
	}
	close DATA;
	close OUT;
    }
}

