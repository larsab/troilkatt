# 
# Arguments:
# $1 is the directory containing PCL files from you.  
# $2 is an output location.  
# $3 is just a prefix (like 2011-june). 
#

mkdir -p $2

#MAKE WORM
mkdir -p $2/worm
for x in $1/worm/cel-final-pcl/*.final; do echo "$x `head -n1 $x | wc -w`"; done > $2/worm/datasets.txt
mkdir -p $2/worm/cel-final-pcl/
python parsers/process_datasets.py --data-desc=$2/worm/datasets.txt --skip=2 --min-arrays=6 --out-dir=$2/worm/cel-final-pcl/ -n Normalizer -c Combiner
Combiner -v 0 -k 2 -o $2/worm/$3-worm.pcl $2/worm/cel-final-pcl/*.final

#MAKE YEAST
mkdir -p $2/yeast
for x in $1/yeast/gds-final-pcl/*.final; do echo "$x `head -n1 $x | wc -w`"; done > $2/yeast/datasets.txt
mkdir -p $2/yeast/gds-final-pcl/
python parsers/process_datasets.py --data-desc=$2/yeast/datasets.txt --skip=2 --min-arrays=6 --out-dir=$2/yeast/gds-final-pcl/ -n Normalizer -c Combiner
Combiner -v 0 -k 2 -o $2/yeast/$3-yeast.pcl $2/yeast/gds-final-pcl/*.final

#MAKE RAT
mkdir -p $2/rat
for x in $1/rat/cel-final-pcl/*.final; do echo "$x `head -n1 $x | wc -w`"; done > $2/rat/datasets.txt
mkdir -p $2/rat/cel-final-pcl/
python parsers/process_datasets.py --data-desc=$2/rat/datasets.txt --skip=2 --min-arrays=6 --out-dir=$2/rat/cel-final-pcl/ -n Normalizer -c Combiner
Combiner -v 0 -k 2 -o $2/rat/$3-rat.pcl $2/rat/cel-final-pcl/*.final

#MAKE ARABIDOPSIS
mkdir -p $2/arabidopsis
for x in $1/arabidopsis/cel-final-pcl/*.final; do echo "$x `head -n1 $x | wc -w`"; done > $2/arabidopsis/datasets.txt
mkdir -p $2/arabidopsis/cel-final-pcl/
python parsers/process_datasets.py --data-desc=$2/arabidopsis/datasets.txt --skip=2 --min-arrays=6 --out-dir=$2/arabidopsis/cel-final-pcl/ -n Normalizer -c Combiner
Combiner -v 0 -k 2 -o $2/arabidopsis/$3-arabidopsis.pcl $2/arabidopsis/cel-final-pcl/*.final

#MAKE MOUSE
mkdir -p $2/mouse
for x in $1/mouse/cel-final-pcl/*.final; do echo "$x `head -n1 $x | wc -w`"; done > $2/mouse/datasets.txt
mkdir -p $2/mouse/cel-final-pcl/
python parsers/process_datasets.py --data-desc=$2/mouse/datasets.txt --skip=2 --min-arrays=6 --out-dir=$2/mouse/cel-final-pcl/ -n Normalizer -c Combiner
Combiner -v 0 -k 2 -o $2/mouse/$3-mouse.pcl $2/mouse/cel-final-pcl/*.final

#MAKE HUMAN
mkdir -p $2/human
for x in $1/human/cel-final-pcl/*.final; do echo "$x `head -n1 $x | wc -w`"; done > $2/human/datasets.txt
mkdir -p $2/human/cel-final-pcl/
python parsers/process_datasets.py --data-desc=$2/human/datasets.txt --skip=2 --min-arrays=6 --out-dir=$2/human/cel-final-pcl/ -n Normalizer -c Combiner
Combiner -v 0 -k 2 -o $2/human/$3-human.pcl $2/human/cel-final-pcl/*.final

