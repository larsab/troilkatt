import os
from troilkatt_script import TroilkattScript

"""
Do sinfo analysis on PCL files
"""
class Sinfo(TroilkattScript):
    """
    arguments: 
    [0] path to analyzer_just_sinfo binary
        
    @param: see description for super-class
    """
    def __init__(self):
        TroilkattScript.__init__(self)
                
        argsParts = self.args.split(" ")
        if (len(argsParts) != 1):
            raise Exception('Invalid arguments: %s' % (self.args))
        self.analyzerBin = argsParts[0]      

    
    """
    Script specific run function. A sub-class should implement this function.
    
    @param inputFiles: list of absolute filenames in the input directory
    @param inputObjects: dictionary with objects indexed by a script specific key (can be None).
    
    @return: dictionary with objects that should be saved in Hbase using the given keys (can be None).
    """
    def run(self, inputFiles, inputObjects):
        oldDir = os.getcwd()
        os.chdir(self.inputDir)
        #
        # 1. Create sinfo dir
        #
        if not os.path.isdir("sinfo"):
            os.mkdir("sinfo")
        
        #
        # 2. Run analyzer script
        #
        containerBin = "/nhome/larsab/troilkatt/apps/bin/troilkatt-container"
        for fn in inputFiles:                        
            cmd = '%s 12 -1 %s %s > %s 2> %s' % (containerBin,
                                                 self.analyzerBin,
                                                 os.path.basename(fn),
                                                 os.path.join(self.logDir, 'analyzer.output'),
                                                 os.path.join(self.logDir, 'analyzer.error'))
            if os.system(cmd) != 0:
                print 'analyzer failed: %s' % (cmd)
                self.logger.warning('analyzer failed: %s' % (cmd))
                return None
            
        outputFiles = os.listdir("sinfo")
        for of in outputFiles:
            print 'Move %s to %s' % (of, self.outputDir)
            os.rename(os.path.join("sinfo/", of), 
                      os.path.join(self.outputDir, of))
            
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
    s = Sinfo()
    s.mainRun()
