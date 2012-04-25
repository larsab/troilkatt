import os
from troilkatt_script import TroilkattScript

"""
Convert a set of CEL files to a PCL file
"""
class Cel2PclAffy(TroilkattScript):
    """
    arguments: 
    [0] Directory with R script and R libriaries
    [1] Organism specific Affy R script
        
    @param: see description for super-class
    """
    def __init__(self):
        TroilkattScript.__init__(self)
                
        argsParts = self.args.split(" ")
        if (len(argsParts) != 2):
            raise Exception('Invalid arguments: %s' % (self.args))
        self.rDir = argsParts[0]
        self.rScript = argsParts[1] 
        
    """
    Cleanup input directory by removing all files.

    @param inputFiles: list of files that should not be removed
    @return none
    """
    def cleanupInputDir(self, inputFiles=None):
        os.chdir(self.inputDir)
        dirContent = os.listdir('.')

        inputFileBasenames = []
        for fn in inputFiles:
            inputFileBasenames.append(os.path.basename(fn))
        
        for fn in dirContent:
            if fn in inputFileBasenames:
                continue
            else:
                os.remove(fn)
    
    """
    Script specific run function. A sub-class should implement this function.
    
    @param inputFiles: list of absolute filenames in the input directory
    @param inputObjects: dictionary with objects indexed by a script specific key (can be None).
    
    @return: dictionary with objects that should be saved in Hbase using the given keys (can be None).
    """
    def run(self, inputFiles, inputObjects):
        oldDir = os.getcwd()
        self.cleanupInputDir(inputFiles)
        #
        # 1. Unpack tar file
        #
        tarName = None
        for fn in inputFiles:
            if self.endsWith(fn, '.tar'):
                tarName = os.path.basename(fn).split(".")[0]
                os.chdir(self.inputDir)
                cmd = 'tar xvf %s > %s 2> %s' % (fn, 
                                                 os.path.join(self.logDir, os.path.basename(fn) + '.untar.output'),
                                                 os.path.join(self.logDir, os.path.basename(fn) + '.untar.error'))
                #print 'Execute: %s' % (cmd)
                if os.system(cmd) != 0:
                    print 'Unpack failed: %s' % (cmd)
                    self.logger.warning('Unpack failed: %s' % (cmd))
                    self.cleanupInputDir()
                    os.chdir(oldDir)
                    return None
                
        if tarName == None:
            raise Exception('RAW.tar file not found in input directory: ' % (self.inputDir))
        
        #
        # 2. Decompress CEL files
        #
        unpackedFiles = os.listdir(self.inputDir)
        for fn in unpackedFiles:
            if self.endsWith(fn.lower(), 'cel.gz'):
                cmd = 'gunzip -f %s > %s 2> %s' % (fn, 
                                                os.path.join(self.logDir, fn + '.gunzip.output'),
                                                os.path.join(self.logDir, fn + '.gunzip.error'))
                #print 'Execute: %s' % (cmd)
                if os.system(cmd) != 0:
                    print 'gunzip failed: %s' % (cmd)
                    self.logger.warning('gunzip failed: %s' % (cmd))
                    self.cleanupInputDir()
                    os.chdir(oldDir)
                    return None
        
        #
        # 3. Run R script to create PCL file in input directory 
        #
        outputPrefix = tarName + '_AFFY' + '.tmp'
        mapPrefix = tarName + '.map'
        rOutputPrefix = os.path.join(self.inputDir, outputPrefix)
        rMapPrefix = os.path.join(self.outputDir, mapPrefix)
        print 'Chdir to: %s' % (self.rDir)
        os.chdir(self.rDir)
        
        # TODO: put in sleipnir dir
        cmd = '/nhome/larsab/troilkatt/apps/bin/troilkatt-container 12 -1 ./R --no-save --args %s %s %s < %s > %s 2> %s' % (self.inputDir,
                                                                                                                            rOutputPrefix,
                                                                                                                            rMapPrefix,
                                                                                                                            self.rScript,
                                                                                                                            os.path.join(self.logDir, 'R.output'),
                                                                                                                            os.path.join(self.logDir, 'R.error'))
        print 'Execute: %s' % (cmd)
        if os.system(cmd) != 0:
            print 'R script failed'
            self.logger.warning('R script failed')
            self.cleanupInputDir()            
            os.chdir(oldDir)
            return None
        
        #
        # 4. Fix PCL file such that it can be used in processing pipeline
        #
        partFiles = os.listdir(self.inputDir)
        for f in partFiles:
            if f.find(outputPrefix) != -1:                
                if f.find('platform') != -1:
                    newName = f.replace('.tmp.platform.', '_') + '.pcl'                
                else:
                    newName = f.replace('.tmp', '.pcl')
                    
                map(os.path.join(self.inputDir, f),
                    os.path.join(self.inputDir, f.replace(outputPrefix, mapPrefix)),
                    os.path.join(self.outputDir, newName))
                
        return None

    """
    Map gene IDs and add necessary rows to R generated PCL file
    
    @param pclFilename: R generated PCL file
    @param mapFilename: R generated map file
    @param outputFilename: final pcl file
    """
    def map(self, pclFilename, mapFilename, outputFilename):    
        #
        # 1. Parse mapfile
        #
        affy2entrez = {}
        fp = open(mapFilename)
        while 1:
            line1 = fp.readline()
            line2 = fp.readline()
            line3 = fp.readline()
            if line1 == '' or line2 == '' or line3 == '':
                break
            
            # first line is the affy Id
            affyId = line1[1:].strip()
            # second is the entrez Id
            entrezId = None
            if line2.find('NA') != -1:
                continue
            elif line2.find('"') != -1:
                entrezId = line2.split('"')[1]
            # else: do nothing
            
            # third is blank
    
            if affy2entrez.has_key(affyId):
                if affy2enrtez[affyId] != entrezId:
                    print 'Warning: duplicate affy key: %s -> %s (old) %s (new)' % (affyId, affy2entrez[affyId], entrezId)
                #else:
                #   print 'Has mapping: %s -> %s' % (affyId, entrezId)
            else:
                affy2entrez[affyId] = entrezId
                #print '|%s| -> |%s|' % (affyId, entrezId)
                #sys.exit(0)
                    
        fp.close()
    
        #
        # 2. Re-map pcl ID's and add missing columns and rows
        #
        lines = open(pclFilename).readlines()
        fp = open(outputFilename, 'w')
        sampleIds = lines[0].split('\t')[1:]
        fp.write('EntrezID\tEntrezID\tGWEIGHT')
        for s in sampleIds:
            fp.write('\t' + s)
        # Last sampleId contains a newline
    
        # Add EWEIGHT column
        fp.write('EWEIGHT\t\t1')
        for h in sampleIds:
            fp.write('\t1')
        fp.write('\n')
    
        for l in lines[1:]:
            cols = l.split('\t')
            affyId = cols[0].strip()
            #aprint '|%s|' % (affyId)
            
            if affy2entrez.has_key(affyId):
                fp.write('%s\t%s\t1' % (affy2entrez[affyId], affy2entrez[affyId]))
                for c in cols[1:]:
                    fp.write('\t' + c)
                # Last column has newline
            else:
                #print 'Entrez ID not found for: %s' % (affyId)
                pass
        fp.close()

        
"""
Run a troilkatt script

Command line arguments: %s input-dir output-dir log-dir args, where
   input-dir      Directory containing files to process (or where to store downloaded files)
   output-dir     Directory where output files are stored.
   log-dir        Directory where logfiles are stored.
   args[0]        Directory with R script
   args[1]        Organism code used by R script

The decription in usage() has additional details. 
"""
if __name__ == '__main__':
    s = Cel2PclAffy()
    s.mainRun()
