import os
from troilkatt_script import TroilkattScript

"""
Convert a set of CEL files to a PCL file
"""
class Cel2PclRefseq(TroilkattScript):
    """
    arguments: 
    [0] Directory with R script and R libriaries
        
    @param: see description for super-class
    """
    def __init__(self):
        TroilkattScript.__init__(self)
                
        argsParts = self.args.split(" ")
        if (len(argsParts) != 1):
            raise Exception('Invalid arguments: %s' % (self.args))
        self.rDir = argsParts[0]      


    """
    Cleanup input directory by removing everything except the input files.

    @param inputFiles: list of files that should not be removed
    @return none
    """
    def cleanupInputDir(self, inputFiles):
        os.chdir(self.inputDir)
        dirContent = os.listdir('.')

        #inputFileBasenames = []
        #for fn in inputFiles:
        #    inputFileBasenames.append(os.path.basename(fn))
        
        for fn in dirContent:
            #if fn in inputFileBasenames:
            #    continue
            #else:
            os.remove(fn)

    def cleanupInputDir2(self, inputFiles):
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
        self.cleanupInputDir2(inputFiles)
        #
        # 1. Unpack tar file
        #
        tarName = None
        gseID = None
        for fn in inputFiles:
            if self.endsWith(fn, '.tar'):
                tarName = os.path.basename(fn).split(".")[0]
                gseID = tarName.replace("_RAW", "")
                os.chdir(self.inputDir)
                cmd = 'tar xvf %s > %s 2> %s' % (fn, 
                                                 os.path.join(self.logDir, os.path.basename(fn) + '.untar.output'),
                                                 os.path.join(self.logDir, os.path.basename(fn) + '.untar.error'))
                #print 'Execute: %s' % (cmd)
                #cmd = "ps"
                if os.system(cmd) != 0:
                    print 'Unpack failed: %s' % (cmd)
                    self.logger.warning('Unpack failed: %s' % (cmd))
                    self.cleanupInputDir(inputFiles)
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
                #cmd = "ps"
                if os.system(cmd) != 0:
                    print 'gunzip failed: %s' % (cmd)
                    self.logger.warning('gunzip failed: %s' % (cmd))
                    self.cleanupInputDir(inputFiles)
                    os.chdir(oldDir)
                    return None
                    
        
        #
        # 3. Run R scripts to create output files in output directories 
        #                
        print 'Chdir to: %s' % (self.rDir)
        os.chdir(self.rDir)
        cmd1 = '/nhome/larsab/troilkatt/apps/bin/troilkatt-container 12 -1 ./Rscript process_cel_gpl96_mas.R %s %s/%s.mas.txt > %s 2> %s' % (self.inputDir,
                                                                                                                                             self.outputDir,
                                                                                                                                             gseID,
                                                                                                                                             os.path.join(self.logDir, 'R-mas.output'),
                                                                                                                                             os.path.join(self.logDir, 'R-mas.error'))
        cmd2 = '/nhome/larsab/troilkatt/apps/bin/troilkatt-container 12 -1 ./Rscript process_cel_gpl96_rma.R %s %s/%s.rma.txt > %s 2> %s' % (self.inputDir,
                                                                                                                                             self.outputDir,
                                                                                                                                             gseID,
                                                                                                                                             os.path.join(self.logDir, 'R-rma.output'),
                                                                                                                                             os.path.join(self.logDir, 'R-rma.error'))
        cmd3 = '/nhome/larsab/troilkatt/apps/bin/troilkatt-container 12 -1 ./Rscript process_cel_gpl96_norm_rma.R %s %s/%s.norm.rma.txt > %s 2> %s' % (self.inputDir,
                                                                                                                                                       self.outputDir,
                                                                                                                                                       gseID,
                                                                                                                                                       os.path.join(self.logDir, 'R-norm-rma.output'),
                                                                                                                                                       os.path.join(self.logDir, 'R-norm-rma.error'))
        
        for cmd in [cmd1, cmd2, cmd3]:        
            print 'Execute: %s' % (cmd)
            #cmd = "ls"
            if os.system(cmd) != 0:
                print 'R script failed'
                self.logger.warning('R script failed')
                self.cleanupInputDir(inputFiles)            
                os.chdir(oldDir)
                return None
                    
        self.cleanupInputDir(inputFiles)
        os.chdir(oldDir)
                
        return None
        
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
    import os
    os.chdir('/nhome/larsab/skarntyde/troilkatt-java')
    s = Cel2PclRefseq()
    s.mainRun()
