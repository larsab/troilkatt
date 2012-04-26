#!/usr/bin/perl

use strict;
use FileHandle;
my $i;
open (DATA, "$ARGV[0]") or die "log_transform.pl: Can't open data file $ARGV[0]\n";
open (OUT, ">$ARGV[1]");
print "$ARGV[0]\n";
if ($ARGV[2] eq "" or $ARGV[3] eq ""){
    die "Usage: PCL rowsToSkip columnToSkip\n";
}
my $sRow=$ARGV[2];
my $sColumn=$ARGV[3];
my $row_header;
my $std;
my $median;
my @array;
while (<DATA>){
    if ($.<=$sRow){
	print OUT $_;
    }
    else{
	chomp;
	@array=split(/\t/, $_);
	$row_header=shift(@array);
	for ($i=0; $i<$sColumn; $i++){
	    $row_header.="\t";
	    $row_header.=shift(@array);
	}

	$std=std(\@array);
	$median=median(\@array);
	print OUT $row_header;
	for ($i=0; $i<scalar(@array); $i++){
	    if ($array[$i] eq ""){
		print OUT "\t";
	    }
	    else{
		printf OUT "\t%4.4f", log($array[$i]);
	    }
	}
	print OUT "\n";
    }
}

sub std
{
    my $array_ref = shift;
    my $n = scalar(@{$array_ref});
    my $result = 0;
    my $item;
    my $sum = 0;
    my $sum_sq = 0;
    my $n = scalar @{$array_ref};
    foreach $item (@{$array_ref})
    {
        $sum += $item;
        $sum_sq += $item*$item;
    }
    if ($n > 1)
    {
        $result = (($n * $sum_sq) - $sum**2)/($n*$n);
    }

    return sqrt($result);
}


sub average
{
    my $array_ref = shift;
    my $sum = 0;
    my $result = 0;
    my $n = scalar @{$array_ref};
    my $item;
    foreach $item (@{$array_ref})
    {
        $sum += $item;
    }
    if ($n > 0)
    {
        $result = $sum / $n;
    }
    return $result;
}

sub median{
    my $array_ref=shift;
    my @sorted;   
    @sorted= sort {$a<=>$b} @{$array_ref};
    return $sorted[int((scalar @sorted)/2)];
}
