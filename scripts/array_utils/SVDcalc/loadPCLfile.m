function [all_data, geneNames] = loadPCLfile (input_file),
% Loads in the data from a .pcl formatted microarray data file
% **Requires that DataParser.java be in the javapath (use javaaddpath)
%

parser = PCLParser;
parser.readData(input_file);

all_data = parser.data;
geneNames = parser.idx2gene;

