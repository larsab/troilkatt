import os
from troilkatt_script import TroilkattScript

"""
Convert a soft file to a pcl file (per directory version)

This class parses a SOFT file to identify:
1. Probe ID
2. Gene names
3. Sample ID
4. Expression values
5. Wether to treat zeros as missing values

The above are used as arguments in a script that does the conversion.
"""
class Soft2Pcl(TroilkattScript):
    """
    arguments: file with gene-names
        
    @param: see description for super-class
    """
    def __init__(self):
        TroilkattScript.__init__(self)
        
        # Note that args anf self.args may differ since TroilkattScript.__init__() sets the TROILKATT.*
        # substrings        
        #self.geneSet = self.initGeneSet(self.args)
        
        # Keys in various dictionaries
        self. queries = ['pp', 'gn', 'sp', 'ev']
        
        # These keywords are used to identify the various columns of interest in the .soft file.
        # The columns of interest are the column numbers used as arguments in the script that 
        # converts the .soft file to a .pcl file
        self. keywords = {# List of keywords used to identify the platform probe column number
                          'pp': ['ID'],
                          # List of keywords used to detect the gene names column number (in prioritized order)                
                          #'ngn': ['ORF', 'GB_ACC', 'GENOME_ACC', 'RANGE_GB', 'GB_LIST', 'Gene_ID', 'CLONE_ID', 'Genename', 'GENE_NAME', 'Common Name', 'Gene Symbol', 'GENE_SYMBOL'],
                          # Conservative list
                          'gn': ['ORF', 'GB_ACC', 'GENOME_ACC', 'RANGE_GB', 'GB_LIST', 'Gene Symbol', 'GENE_SYMBOL'],
                          # List of keywords used to detect the sample probe ID    
                          'sp': ['ID_REF'],    
                          # List of keywords used to detect the expression values    
                          'ev': ['VALUE']}    
        
        # Key to more meaningful name mapping
        self.labels = {'pp': 'Platform probe ID:',
                       'gn': 'Gene names:',
                       'sp': 'Sample probe ID:',
                       'ev': 'Expression values:',
                       'zamv': 'Zero to missing value conversion'}
       
        # During zeros as missing values estimation, zeros with fewer than this number desimals
        # are considered to be true values (and hence not missing values)
        self.valueDesimals = 5
        
        # This command is used to converts the .soft file to a .pcl file
        # The TROILKATT substrings are replaced before running the scipt (as in execute_per_file.py)
        self.cmd = 'ruby TROILKATT.UTILS/seriesFamilyParser.rb TROILKATT.INPUT_DIR/TROILKATT.FILE < TROILKATT.LOG_DIR/TROILKATT.FILE.args > TROILKATT.OUTPUT_DIR/TROILKATT.FILE_NOEXT.pcl 2> TROILKATT.LOG_DIR/TROILKATT.FILE.error'

    """
    Initialize frozenset with gene names.
    
    @param genesFile: file with gene names.
    
    @return: frozenset with gene names. The frozen set provides O(1) search operation.
    """
    def initGeneSet(self, genesFile):
        #
        # Initialize yeast gene hash table by reading in yeast names from 
        # a file
        #
        # Note that the frozenset is implemented in Python as a hash table so it
        # supports O(1) lookups
        #
        self.logger.info('Read set of gene names from: %s' % (genesFile))
        
        # Read in gene names into a list that is later used to initialize the frozenset
        geneList = []
        fp = open(genesFile)
        while 1:
            line = fp.readline()
            if line == '':
                break
            
            parts = line.split()
            for p in parts:
                gene = p.strip()
                
                if gene not in geneList:
                    geneList.append(gene)                        
        fp.close()    
        
        # Use list to initialize hash table
        return frozenset(geneList)    
    
    """
    Script specific run function. A sub-class should implement this function.
    
    @param inputFiles: list of absolute filenames in the input directory
    @param inputObjects: dictionary with objects indexed by a script specific key (can be None).
    
    @return: dictionary with objects that should be saved in Hbase using the given keys (can be None).
    """
    def run(self, inputFiles, inputObjects):
        self.logger.info('Process %d files' % (len(inputFiles)))                
        
        # Statistics of the number of columns found and number of duplicate columns
        nFiles = 0
        nFilesExact = 0          # Files with exactly one column per question 
        nFilesMissing = 0        # Files with missing columns
        nFilesInconsistent = 0   # Files with multiple sample tables of different format
        nZero2MV = 0             # Files where zeros should be converted to missing values
        
        # More statistics
        nInconsistent = {}
        nMissing = {}    
        # Files where list of filenames are written
        missingFPs = {}
        inconsistentFPs = {}
        for q in self.queries:
            nInconsistent[q] = 0
            nMissing[q] = 0
            # List of files with missing columns
            missingFPs[q] = open(os.path.join(self.logDir, 'missing_%s.txt' % (q)), 'w')
            # List of files with inconsistent columns
            inconsistentFPs[q] = open(os.path.join(self.logDir, 'inconsistent_%s.txt' % (q)), 'w')
        # List of files where zeros should be converted to missing values 
        zero2MVFP = open(os.path.join(self.logDir, 'zero_to_missing_value.txt'), 'w')
        # Detailed logging information (summary is stored in summary.log)                
        detailsFP = open(os.path.join(self.logDir, 'details.log'), 'w')
        # Files for which at least one column could not be found
        notConvertedFP = open(os.path.join(self.logDir, 'failures.log'), 'w')
        
        # Replace TROILKATT directory commands        
        convertCmd = self.setTroilkattVariables(self.cmd)
        
        #
        # Convert all .soft files to .pcl files
        #
        for f in inputFiles:
            if not self.endsWith(f, '.soft'):
                # Not a SOFT file
                self.logger.debug('%s ignored: not a .soft file' % (f))
                continue
            
            #
            # Parse file to find arguments ti be used by the soft->pcl conversion script
            #
            rd, messages, md = self.parseSOFT(f)
            
            # 
            # Update statistics and logfiles
            #
            nFiles += 1
            hasMissing = 0        
            if md['pp'] == 1 and md['gn'] == 1 and md['sp'] == 1 and md['ev'] == 1:
                nFilesExact += 1
            elif md['pp'] == 0 or md['gn'] == 0 or md['sp'] == 0 or md['ev'] == 0:
                nFilesMissing += 1
                hasMissing = 1
            elif md['sp'] > 1 or md['ev'] > 1:
                nInconsistent += 1
    
            # Write statistics to log files
            for k in self.queries:
                if md[k] > 1:
                    nInconsistent[k] += 1
                    inconsistentFPs[k].write('%s\n' % (f))
                elif md[k] == 0:
                    nMissing[k] += 1
                    missingFPs[k].write('%s\n' % (f))
                                
            if rd['zamv']:
                zero2MVFP.write('%s\n' % (f))
                nZero2MV += 1
                
            detailsFP.write('Parsing file: %s\n' % (f))
            for q in self.queries + ['zamv']:
                detailsFP.write('%s:\n' % (self.labels[q]))
                for m in messages[q]:
                    detailsFP.write('\t%s\n' % (m))
            detailsFP.write('\n\n')
            
            #
            # Create arguments file
            #
            if not hasMissing:
                argsFilename = os.path.join(self.logDir, '%s.args' % (os.path.basename(f)))
                argsFile = open(argsFilename, 'w')
                argsFile.write('%d\n%d\n%d\n%d\n%d\n' % (rd['pp'],
                                                         rd['gn'],
                                                         rd['sp'],
                                                         rd['ev'],
                                                         rd['zamv']))
                argsFile.close()
            else:
                notConvertedFP.write('%s\n' % (f))
            
            
            # 
            # Convert SOFT file to PCL file
            #    
            if not hasMissing:        
                fileCmd = self.setTroilkattFilename(convertCmd, f)
                self.logger.debug('Execute: %s' % (fileCmd))
                if os.system(fileCmd) != 0:
                    self.logger.error('Command failed: %s' % (fileCmd))
            
    
        # Close log files
        for q in self.queries:
            inconsistentFPs[q].close()
            missingFPs[q].close()
        detailsFP.close()
        notConvertedFP.close()
            
        # Write summary statistics and create a log-file with all log messages
        summaryFP = open(os.path.join(self.logDir, 'summary.log'), 'w')
        if nFiles > 0:                            
            summaryFP.write('Exact match:          %d (%2.2f%%)\n' % (nFilesExact, float(100 * nFilesExact) / nFiles))
            summaryFP.write('Missing columns:      %d (%2.2f%%)\n' % (nFilesMissing, float(100 * nFilesMissing) / nFiles))
            summaryFP.write('Inconsistent columns: %d (%2.2f%%)\n' % (nFilesInconsistent, float(100 * nFilesInconsistent) / nFiles))
            summaryFP.write('Total files:      %d\n\n' % (nFiles))        
            summaryFP.write('Convert zero to missing value: %d\n\n' % (nZero2MV))                
            for k in self.queries:
                summaryFP.write('%s\n' % (self.labels[k]))
                summaryFP.write('\tMissing:      %d (%2.2f%%)\n' % (nMissing[k], float(100 * nMissing[k]) / nFiles))
                summaryFP.write('\tInconsistent: %d (%2.2f%%)\n' % (nInconsistent[k], float(100 * nInconsistent[k]) / nFiles))                                                         
        else:
            summaryFP.write('No new nor updated files.\n')
        summaryFP.close()        
      
    """
    Identify columns in a SOFT file that contain information used later in the processing 
    pipeline.
    
    @param filename: file to parse
    
    @return (results, messages, matches, zero2MissingValue)
        results: dict with self.queries as keys, and column numbers as values
        messages: list with message strings
        matches: dict with self.queries as keys, and the number of matching columns found
         per query (zero if not match and more than 1 if there are duplicates)
    """    
    def parseSOFT(self, filename):
        self.logger.info('Parse SOFT file: %s' % (filename))
         
        # Query to column number mapping
        results = {}
        
        # Log information about each query type. These are saved in a list such that all information
        # about a query can be printed together
        messages = {}
        for q in self.queries:
            messages[q] = []
        messages['zamv'] = []
        
        # Count of matches found
        matches = {}
        for q in self.queries:
            matches[q] = 0
        
        # Number of zero expression values with at least N desimals, and no desimals. 
        # These are used to determine whether to treat zero's as missing values
        zeroAsValue = 0
        zeroAsMissing = 0
        # Set to true when parsing sample table rows
        countZeros = 0       
        # Expression values column in sample table
        expressionValueCol = None
        # Column header for expression values column
        expressionValueHeader = None 
        # Information about type of zero expression values found in file
        missingExpressionValueCol = 0
        nullExpressionValue = 0
        intExpressionValue = 0
        emptyExpressionValue = 0
        
        #
        # Search for columns with platform probe number and gene names
        #                
        fp = open(filename)
        while 1:
            line = fp.readline()
            if line == '': # EOF
                break
            
            if line.find('!platform_table_begin') == 0:
                # The next line contains the platform header columns
                line = fp.readline()
                try:
                    parts = line.split('\t')
                except Exception, e:
                    self.logger.error('Could not parse platform table column headers in file: %s' % (filename))
                    self.logger.error(e)
                    raise Exception('Could not parse file: %s' % (filename))
                
                for i in range(len(parts)):
                    for q in ['pp', 'gn']:
                        header = parts[i].strip()
                        if header in self.keywords[q]:    
                            if q not in results:                    
                                results[q] = i
                                msg = 'Matching column: %d: %s' % (i, header)
                                messages[q].append(msg)
                            else:
                                self.logger.error('Multiple %s in file: %s' % (self.labels[q], filename))
                                msg = 'WARNING: Multiple %s in file: %s' % (self.labels[q], filename)
                                messages[q].append(msg)
                            
                            matches[q] += 1
            elif line.find('!sample_table_begin') == 0:                
                countZeros = 1
                
                # The next line contains the sample header columns
                line = fp.readline()
                try:
                    parts = line.split('\t')
                except Exception, e:
                    self.logger.error('Could not parse sample table column headers in file: %s' % (filename))
                    self.logger.error(e)
                    raise Exception('Could not parse file: %s' % (filename))
                
                for i in range(len(parts)):
                    for q in ['sp', 'ev']:
                        header = parts[i].strip()
                        if header in self.keywords[q]:    
                            if q not in results:                    
                                results[q] = i
                                msg = 'Matching column: %d: %s' % (i, header)
                                messages[q].append(msg)
                                matches[q] += 1
                            else:
                                if i != results[q]:
                                    msg = 'Inconsistent column %s: %d was %d' % (header, i, results[q])
                                    messages[q].append(msg)
                                    matches[q] += 1
                                # else: same as for above table                            
                        if q == 'ev':
                            expressionValueHeader = header                        
            elif line.find('!sample_table_end') == 0:
                countZeros = 0
                expressionValueCol = None
            elif countZeros:
                # Column header not set
                parts = line.split('\t')
                if expressionValueCol == None: # First line after table_begin                    
                    for i in range(len(parts)):
                        if parts[i] == expressionValueHeader:
                            expressionValueCol = i
                            break
                    
                    if expressionValueCol == None:                        
                        missingExpressionValueCol = 1                    
                        countZeros = 0 # Ignore sample table
                else: # Expression value row
                    if len(parts) < expressionValueCol:
                        # Ignore
                        pass
                    else:                    
                        ev = parts[expressionValueCol].strip()
                        if ev == '':
                            # Is a missing value
                            emptyExpressionValue = 1
                        elif ev == 'null':                            
                            nullExpressionValue = 1
                        elif ev == '0':
                            zeroAsMissing += 1
                            intExpressionValue = 1
                        elif float(ev) == 0.0:
                            if ev[1] == '.' and len(ev) >= (self.valueDesimals - 1):
                                zeroAsValue += 1
                            else:
                                zeroAsMissing += 1            
            
        fp.close()
        
        # Find queries for which a match was not found and if possible attempt to predict the
        # column number
        for q in self.queries:
            if q not in results:
                msg = 'No matching column found'
                messages[q].append(msg)
                
                if q == 'gn':
                    columnNumber, msgs = self.predictGeneName(filename)
                    if columnNumber != None:
                        results[q] = columnNumber
                        matches[q] += 1
                    messages[q] = messages[q] + msgs
                    
        # Create zero-as-missing-value messages
        if missingExpressionValueCol:
            messages['zamv'].append('Missing expression value column')
        if nullExpressionValue:
            messages['zamv'].append("'null' expression values found")
        if intExpressionValue:
            messages['zamv'].append("'0' (no desimals) expression values found")
        if emptyExpressionValue:
            messages['zamv'].append("Empty expression values found")
        if zeroAsValue == 0 and zeroAsMissing == 0 and not emptyExpressionValue and not nullExpressionValue:
            messages['zamv'].append('No zero expression values found')
            
        # Determine if zero values should be treated as missing
        if zeroAsValue == 0 and zeroAsMissing == 0:
            results['zamv'] = 0
        elif zeroAsValue >= zeroAsMissing:
            results['zamv'] = 0
        else:
            results['zamv'] = 1
            
        return (results, messages, matches)
        
    """
    Predict the column number of the 'gene names' column by comparing the columns in the platform 
    table against a set of known gene names. The column with most gene names is assumed to be the
    gene names column, 
    
    @param inputFile: filename of the input file
    
    @return: (column number, messages)
         column number: column number for predicted gene name column
         messages: list of log messages
         
    """
    def predictGeneName(self, inputFile):        
        self.logger.info('Predict gene names column in file: %s' % (inputFile))
        
        # Messages are not printed but rather returned to the caller for printing/ saving
        messages = []
        
        messages.append('Predicting gene name column')
        
        #
        # Find the header for the column in platform_table that has gene names
        #
        fp = open(inputFile)
        headerLine = None
        
        # Goto platform_table_begin line
        while 1:
            line = fp.readline()
            if line == '':
                messages.append('Warning: no platform table in file')
                fp.close()
                return (None, messages)
            
            if line.find('!platform_table_begin') == 0:            
                headerLine = fp.readline().strip()                        
                break            
            
        # Parse header line to find number of columns and column headers
        colHeaders = headerLine.split('\t')    
        nColHeaders = len(colHeaders)
        
        # List with number of genes per column
        geneCount = []
        for i in range(nColHeaders):
            geneCount.append(0)
        
        # Count the number of genes per column
        nRows = 0
        while 1:
            line = fp.readline()
            if line == '':
                messages.append('Error: unexpected end of platform table in file')
                fp.close()
                return (None, messages)
                
            if line.find('!platform_table_end') == 0:
                break
            
            cols = line.strip().split('\t')        
            for i in range(len(cols)):
                if cols[i] in geneCount:
                    geneCount[i] += 1
            
            nRows += 1
        
        sortedGeneCount = geneCount[:]
        sortedGeneCount.reverse()
        topN = min(len(geneCount), 5)
        msg = 'Top %d gene counts:' % (topN)
        for i in range(topN):
            msg = msg + ' %d,' % (sortedGeneCount[i])
        messages.append(msg[:-1])
                    
        # Find the header for the column with most gene names
        maxCount = max(geneCount)
        maxIndex = geneCount.index(maxCount)
        maxHeader = colHeaders[maxIndex]
        
        if maxCount == 0:
            messages.append('Warning: no gene names found in platform table')
            fp.close()
            return (None, messages)
        else:
            msg = 'Column %s has max gene count: %d of %d lines (%2.2f%%)' % (maxHeader, maxCount, nRows, float(100 * maxCount) / nRows)
            messages.append(msg)
            return (maxIndex, messages)
        
"""
Run a troilkatt script

Command line arguments: %s input output log gene-file, where
    input: input directory
    output: output directory
    log: logfile directory
    gene-file: file with gene-names
        
The decription in usage() has additional details. 
"""
if __name__ == '__main__':
    s = Soft2Pcl()
    s.mainRun()
