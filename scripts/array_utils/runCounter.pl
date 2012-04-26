#!/usr/bin/perl

if ( scalar @ARGV < 3 ) {
    print "Usage ./runCounter.pl [input_dir] [tmp_dir] [output_dir] [data_dir]\n";
    exit;
}

# Directory with *.dab files
my $input_dir = "$ARGV[0]";
# Directory where temporary files such as the datasets and file list are stored
my $tmp_dir = "$ARGV[1]";
# Directory where output files are stored
my $output_dir = "$ARGV[2]";
# Directory that has varius data files used by the scripts
my $data_dir = "$ARGV[3]";

# In addition we assume that the scripts programs to be run are in ./scripts/array_utils

print "Directories:\n";
print "\tInput:  $input_dir\n";
print "\tOutput: $output_dir\n";
print "\tTmp:    $tmp_dir\n";
print "\tData:   $data_dir\n";

my @list=`ls $input_dir/\*dab`;
my $count=1;
open (OUT, ">$tmp_dir/datasets.txt") or die "can't open datasets\n";
open (OUT2, ">$tmp_dir/file_list");
my %hash;
foreach (@list){
    /\/([^\/]*)\.dab/;
    print OUT "$count\t$+\n";
    print OUT2 "$input_dir/$+.dab\n";
    $count++;
    $hash{$+}=1;
}
system "perl ./scripts/array_utils/half2weights.pl $output_dir/mitable.txt $tmp_dir/file_list $output_dir/yeast_alphas";

print "STAGE 1\n";
system "./scripts/array_utils/Counter -w $data_dir/yeast_standard.dab -d $input_dir -o $output_dir -m";

print "STAGE 2\n";
system "./scripts/array_utils/Counter -k $output_dir -o $output_dir/bayesnet.bin -s $tmp_dir/datasets.txt -b $output_dir/global.txt -p 2 -a $output_dir/yeast_alphas";

print "STAGE 3\n";
system "./scripts/array_utils/Counter -n $output_dir/bayesnet.bin -o $output_dir -d $input_dir -s $tmp_dir/datasets.txt -e $data_dir/genes.txt -m";
