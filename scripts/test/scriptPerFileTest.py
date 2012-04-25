#
# ScriptPerDirTest unit test file program
#
# This program does the following:
# 1. Copy the input file to the output directory
# 2. Open the file in specified as the third argument, and verify that it contains
#    the filenames of the files copied in the first step.
# 3. Write logfile to log directory
# 4. Verify that program specific arguments are "arg1 arg2"
#

from troilkatt_script import TroilkattScript
import sys, os, shutil

class ScriptPerFileTest(TroilkattScript):
    def __init__(self):
        TroilkattScript.__init__(self)
        
    """
    Unit test run function.
    """
    def run(self):        
        # 1. Copy input file
        shutil.copy(os.path.join(self.inputDir, self.inputFilename), self.outputDir)
         
        # 2. Parse meta-data
        metafile = os.path.join(self.metaDir, "filelist")
        lines = open(metafile).readlines()
        metaBasenames = []        
        for l in lines:
            metaBasenames.append(os.path.basename(l.strip()))
            
        if self.inputFilename not in metaBasenames:
            print 'Input file %s not found in meta file' % (self.inputFilename)
            sys.exit(-1)
                
        # 3. Write something to the log file
        self.logger.warning("Write something to the log file")
        
        # 4. Verify script specific arguments
        myArgs = self.args.split(" ")
        if len(myArgs) != 2:
            print 'Invalid arguments length: %d (expected 2)' % (len(myArgs))
            sys.exit(-1)
        if myArgs[0] != "arg1" or myArgs[1] != "arg2":
            print 'Invalid arguments: %s' % (myArgs)
            sys.exit(-1)
            
if __name__ == '__main__':
    s = ScriptPerFileTest()
    s.run()