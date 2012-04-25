%Script requirements:
% dataset_file_list = file containing paths to .pcl files to perform SVD on
%

%Load the locations of the datasets to use
fin = fopen(dataset_file_list);
dsets = textscan(fin,'%s');
dsets = dsets{1};
fclose(fin);

%Load in each dataset in succession
for i=1:length(dsets),
    [data, names] = loadPCLfile(dsets{i});
    %Perform SVD on the dataset
    [U,S,V] = svd(data,0);
    %Write out the results to file
    writeOutPCLData(U,names,strcat(dsets{i},'.svd_u'));
end
