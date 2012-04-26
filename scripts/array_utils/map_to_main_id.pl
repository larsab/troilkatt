#!/usr/bin/perl
# Maps the second column of a pcl file to using a 2 column map file
#
use strict;
if (scalar @ARGV<3){
    print "Usage ./translate_to_main_id.pl <map_file>  <update_flag(0/1)> pcl_files\n";
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
	$map{$array[0]}=$array[1] if $array[1] ne "";
}

$"="\t";


my $file;
my $gene;
open (LOG, ">not_found");
foreach $file (@ARGV){
    if (! -e $file.".map" or $update){
	print "mapping $file\n";
	open (DATA, "$file") or die "can't open data";
	open (OUT, ">$file.map");
	$.=0;
	while (<DATA>){
	    if ($.>1 and !/EWEIGHT/){
		chomp;
		@array=split(/\t/, $_);
		shift(@array);  
		$gene=shift(@array);  
		if (defined $map{$gene}){
		    print OUT "$map{$gene}\t$map{$gene}";
		    print OUT "\t" if scalar(@array)>0;
		    print OUT "@array\n";	    	    
	    }
		else{
		    print LOG "$file\t$gene\n";
		}
	    }
	    else{
		print OUT $_;
	    }  
    }
	close DATA;
	close OUT;
    }
}

