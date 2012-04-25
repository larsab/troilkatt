import os
from optparse import OptionParser
parser = OptionParser()
parser.add_option("-d", "--data-desc", type = "string", dest = "datadesc")
parser.add_option('-k', '--skip', type = "int", dest = "skip")
parser.add_option('-m', '--min-arrays', type = "int", dest = "min")
parser.add_option("-o", "--out-dir", type = "string", dest = "output")
parser.add_option("-n", "--normalize-binary", type = "string", dest = "normalize")
parser.add_option("-c", "--combiner-binary", type = "string", dest = "combiner")
(options, args) = parser.parse_args()

under_min = []
total_arrays = 0
datasets = open(options.datadesc)
for line in datasets:
    (full_name, cols) = line.strip().split()
    num_arrays = int(cols)-(options.skip+1)
    total_arrays += num_arrays
    if (num_arrays < options.min ):
        under_min.append(full_name)
    else:
        file_name = full_name.split('/').pop()
        #print(options.normalize + ' -s ' + str(options.skip) + ' -T rowz -i ' + full_name + ' -o ' + options.output + file_name)
        os.system(options.normalize + ' -s ' + str(options.skip) + ' -t pcl -T rowz -i ' + full_name + ' -o ' + options.output + file_name)

if under_min:
    os.system(options.combiner + ' -v 0 -k ' + str(options.skip) + ' -o ' + options.output + 'TMP__Combiner_UNDERMIN.pcl ' + ' '.join(under_min))
    os.system(options.normalize + ' -s ' + str(options.skip) + ' -t pcl -T rowz -i ' + options.output + 'TMP__Combiner_UNDERMIN.pcl -o ' + options.output + 'Combined_UNDERMIN.final')
    os.system('rm ' + options.output + 'TMP__Combiner_UNDERMIN.pcl')

print(options.output + '\t' + str(total_arrays))

