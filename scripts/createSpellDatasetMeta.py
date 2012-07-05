# The biopython library is required for this script
from Bio import Entrez
from Bio import Medline

"""
Tool used to add citation a file with metadata about the datasets in a Spell compendia by querying
PubMed and combining the results with the metadata output by tge preprocessing pipeline.

@param inputFilename: "info" input file produced by the Spell data preprocessing pipeline. It is a tab delminated file
with the following columns per line:
0. File
1. DatasetID (GEO ID)
2. Organism
3. Platform
4. ValueType
5. #channels
6. Title (datset name)
7. Description
8. PubMedID
9. #features (#genes)
10. #samples (#conditions)
11. date
12. Min
13. Max
14. Mean
15. #Neg
16. #Pos
17. #Zero
18. #MV
19. #Total
20. #Channels (duplicate of field 5)
21. logged
22. zerosAreMVs
23. MVcutoff

@param outputFilename: output file. It is a tab delimated file with the following columns per line:
0: PubMedID (from input file)
1: Filename (from input file)
2: GeoID (from input file)
3: PlatformID (from input file)
4: channelCount (from input file)
5: DatasetName (from input file)
6: Description (from input file)
7: NumConditions (from input file)
8: NumGenes (from input file)
9: FirstAuthor (from input file)from Pubmed)
10: AllAuthors (from input file)from Pubmed)
11: Title (from Pubmed)
12: Journal (from input file)from Pubmed)
13: PubYear (from input file)from Pubmed)
14: ConditionDescriptions (from input file)
15: Tags (from input file)not set and not sure what it is used for)
16: ValueType  (from input file)
17: Min (from input file)
18. Max (from input file)
19. Mean (from input file)
20. NumNeg (from input file)
21. NumPos (from input file)
22. NumZero (from input file)
23. NumMV (from input file)
24. NumTotal (from input file)
25. logged (from input file)
26. zerosAreMVs (from input file)
27. MVcutoff (from input file)
"""
def createMetadataFile(inputFilename, outputFilename):
    Entrez.email = 'lbongo@princeton.edu'
    
    inputFile = open(inputFilename, "r")
    outputFile = open(outputFilename, "w")
        
    # 
    # 0. Write header line (one line written in two parts
    #
    outputFile.write("PubMedID\tFilename\tGeoID\tPlatformID\tchannelCount\tDatasetName\tDescription\tNumConditions\tNumGenes\tFirstAuthor\tAllAuthors\tTitle\tJournal\tPubYear\tConditionDescriptions\tTags");
    outputFile.write("\tValueType\tMin\tMax\tMean\tNumNeg\NumPos\tNumZero\tnumMV\NumTotal\tlogged\tzerosAreMVs\tMVcutoff")    
        
    # Skip header line
    inputFile.readline()
        
    datasets = {} # Filename is key
    pubmedIDs = [] # list of PubMedID's
    while 1:
        line = inputFile.readline()
        
        if line == "":
            break
    
        #
        # 1. Parse input file (line)
        #    
        line = line.strip()
        parts = line.split("\t")
        if (len(parts) != 24):                
            raise "Invalid line in alias file: " + line
        
        datasetFilename = parts[0]
        pubmedID = parts[8]
        if datasets.has_key(datasetFilename):
            raise "Multiple records with same dataset filename in input file: " + datasetFilename
        
        datasets[datasetFilename] = {"Filename": datasetFilename,
                                     "GeoID": parts[1],
                                    "Organism": parts[2],
                                    "PlatformID": parts[3],
                                    "ValueType": parts[4],
                                    "channelCount": parts[5],
                                    "DatasetName": parts[6],
                                    "Description": parts[7],
                                    "PubMedID": pubmedID,
                                    "NumGenes": parts[9],
                                    "NumConditions": parts[10],
                                    "Min": parts[12],
                                    "Max": parts[13],
                                    "Mean": parts[14],
                                    "NumNeg": parts[15],
                                    "NumPos": parts[16],
                                    "NumZero": parts[17],
                                    "NumMV": parts[18],
                                    "NumTotal": parts[19],
                                    "logged": parts[21],
                                    "zerosAreMVs": parts[22],
                                    "MVcutoff": parts[23]}
        pubmedIDs.append(pubmedID)
        
        #
        # Note! must be read from dataset file
        #
        datasets[datasetFilename]["ConditionDescriptions"] = "not set"
        
        #
        # Note! not sure what this value is
        #
        datasets[datasetFilename]["Tags"] = "default"
        
    #
    # 2. Retrieve citation information from PubMed
    #
    handle = Entrez.efetch(db="pubmed", id=pubmedIDs, rettype="medline", retmode="text")
    records = Medline.parse(handle)
    records = list(records) # Convert to a list
    citedDatasets = [] # List with keys of already cited datasets
    for r in records:
        pubmedID = r['PMID']
        authorList = r['AU']
        firstAuthor = authorList[0]
        allAuthors = ""
        for a in authorList:
            allAuthors = allAuthors + a
        title = r["TI"]
        journal = r["JT"]
        pubYear = r["DP"].split()[0]
    
        # Find matching datasets and update records
        isFound = 0
        for k in datasets.keys():
            if k in citedDatasets:
                continue
            
            d = datasets[k]
            if d["PubMedID"] == pubmedID:
                d["FirstAuthor"] = firstAuthor
                d["AllAuthors"] = allAuthors
                d["Title"] = title
                d["Journal"] = journal
                d["PubYear"] = pubYear
                isFound = 1
                citedDatasets.append(k)
        if not isFound:
            raise "PubmedID" + pubmedID + " not found in any dataset"
     
    #
    # 3. Fill in values for datasets where citations could not be found
    #
    for k in datasets.keys():
        if k not in citedDatasets:
            print "Warning: no citations found for dataset: " + k
            
            d = datasets[k]
            d["FirstAuthor"] = "Unknown"
            d["AllAuthors"] = "Unknown"
            d["Title"] = "Unknown (GEO ID %s)" + d["GeoID"]
            d["Journal"] = "Unknown"
            d["PubYear"] = "Unknown"
                
    #
    # 4. Write output to outputfile
    #
    for k in datasets.keys():
        d = datasets[k]
        outputFile.write(d["PubMedID"] + "\t")
        outputFile.write(d["Filename"] + "\t")
        outputFile.write(d["GeoID"] + "\t")
        outputFile.write(d["PlatformID"] + "\t")
        outputFile.write(d["channelCount"] + "\t")
        outputFile.write(d["DatasetName"] + "\t")
        outputFile.write(d["Description"] + "\t")
        outputFile.write(d["NumConditions"] + "\t")
        outputFile.write(d["NumGenes"] + "\t")
        outputFile.write(d["FirstAuthor"] + "\t")
        outputFile.write(d["AllAuthors"] + "\t")
        outputFile.write(d["Title"] + "\t")
        outputFile.write(d["Journal"] + "\t")
        outputFile.write(d["PubYear"] + "\t")
        outputFile.write(d["ConditionDescriptions"] + "\t")
        outputFile.write(d["Tags"] + "\t")
        outputFile.write(d["ValueType"] + "\t")
        outputFile.write(d["Min"] + "\t")
        outputFile.write(d["Max"] + "\t")
        outputFile.write(d["Mean"] + "\t")
        outputFile.write(d["NumNeg"] + "\t")
        outputFile.write(d["NumPos"] + "\t")
        outputFile.write(d["NumZero"] + "\t")
        outputFile.write(d["NumMV"] + "\t")
        outputFile.write(d["NumTotal"] + "\t")
        outputFile.write(d["logged"] + "\t")
        outputFile.write(d["zerosAreMVs"] + "\t")
        outputFile.write(d["MVcutoff"] + "\n")
        
    inputFile.close()
    outputFile.close()
    
"""
Command line arguments:
0 - input file: .info file produced by the troilkatt Spell preprocessing pipeline
1 - output file
""" 
if __name__ == '__main__':
    import sys
    
    assert(len(sys.argv) == 3), "Usage: %s inputFile outputFile" % (sys.argv[0])
    createMetadataFile(sys.argv[1], sys.argv[2])
    
        