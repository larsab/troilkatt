#!/usr/bin/perl
# usage ./half2weights.pl [mi_table] [list_of_data] [output_file]

use strict;

open (D, "$ARGV[0]");
open (OUT, ">$ARGV[2]");

my @array;
my %values;
my @dataset;
my $data;
my ($i, $j);
my %in_file;
while (<D>){
    chomp;
    @array=split(/\t/, $_);
    if ($.==1){
	shift(@array);
	@dataset=@array;
    }
    else{
	$data=shift(@array);
	$in_file{$data}=1;
	for ($i=0; $i<scalar(@array); $i++){
	    if ($array[$i] ne ""){	   
		$values{"$data"."_$dataset[$i]"}=$array[$i];
		$values{"$dataset[$i]_$data"}=$array[$i];
	    }
	   #print"$data"."_$dataset[$i]\t$array[$i]\n";
	   #print"$dataset[$i]_$data\t$array[$i]\n";
	}
    }
}

my %include;
if ($ARGV[1] ne ""){
    open (H, "$ARGV[1]");
    while (<H>){
	chomp;
	$include{$_}=1;
	if (! defined $in_file{$_}){
	    print STDERR "Warning, $_ is not included in $ARGV[0] and will not be processed\n";
	}
    }

    my @tmp;
    foreach (@dataset){
	push (@tmp, $_) if defined $include{$_};
    }
    
    @dataset=@tmp;
}


my $sum;
my $self;
for ($i=0; $i<scalar(@dataset); $i++){
    $self=$values{"$dataset[$i]_$dataset[$i]"};
    #print "$dataset[$i]\t$self\n";
    $sum=0;
    for ($j=0; $j<scalar(@dataset); $j++){
	$sum+=$values{"$dataset[$i]_$dataset[$j]"} if $i!=$j;
    }
    $dataset[$i]=~s/\.dab//;
   printf OUT "$dataset[$i]\t%4.12e\n", 2**($sum/$self)-1;
}
