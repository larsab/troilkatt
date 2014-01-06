import os, sys

"""
Fix a bug in the recent version of KNNImputer where PCL header files are corrupted
        
Command line arguments: fixKNNImputer.py KNNImputer args ...
"""
if __name__ == '__main__':
    import sys
    
    # Find input and output filename
    inputFilename = None
    outputFilename = None
    tmpFilename = None
    for i in range(len(sys.argv)):
        if sys.argv[i] == "-i" or sys.argv[i] == "--input":
            if len(sys.argv) >= i + 2:
                inputFilename = sys.argv[i+1]
        if sys.argv[i] == "-o" or sys.argv[i] == "--output":
            if len(sys.argv) >= i + 2:
                outputFilename = sys.argv[i+1]
                tmpFilename = outputFilename + ".tmp"
                sys.argv[i+1] = tmpFilename
                
    if inputFilename == None:
        print "Input file argument not found"
        sys.exit(-1)
    if inputFilename == None:
        print "Output file argument not found"
        sys.exit(-1) 
    
    # 0 is fixKNNImputer.py
    execCmd = sys.argv[1]
    for i in range(2, len(sys.argv)):
        execCmd = "%s %s" % (execCmd, sys.argv[i])
        
    os.system(execCmd)
    
    fp = open(inputFilename)
    inputHeader = fp.readline()
    inputWeight = fp.readline()
    fp.close()

    fpi = open(tmpFilename)
    fpo = open(outputFilename, "w")
    outputHeader = fpi.readline()
    outputWeight = fpi.readline()
    
    if inputHeader == outputHeader and inputWeight == outputWeight:
        os.rename(tmpFilename, outputFilename)
        fpi.close()
        fpo.close()
    else:
        print "KNNImputer has corrupted headers. Replacing these with input file headers"

        # Headers are corrupted, so we use the headers from the input file
        fpo.write(inputHeader)
        fpo.write(inputWeight )
        if outputWeight.lower().find("eweight") == -1: 
            # no EWEIGHT row
            fpo.write(outputWeight)
        
        # write remaining rows
        while 1:
            r = fpi.readline()
            if r == "":
                break
            fpo.write(r)
    
        fpi.close()
        fpo.close()

        os.remove(tmpFilename)
