function writeOutPCLData (data_matrix, orfNames, outfilename),
%
%

numConds = size(data_matrix,2);

fout = fopen(outfilename,'w');
fprintf(fout,'YORF\tNAME\tGWEIGHT');
for i=1:numConds,
    fprintf(fout,'\t%d',i);
end
fprintf(fout,'\nEWEIGHT\t');
for i=1:numConds,
    fprintf(fout,'\t1');
end

for i=1:size(data_matrix,1),
    fprintf(fout,'\n%s\t%s\t1',char(orfNames(i)),char(orfNames(i)));
    for j=1:size(data_matrix,2),
        fprintf(fout,'\t%f',data_matrix(i,j));
    end
end

fclose(fout);
