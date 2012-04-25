#
# ScriptPerDirTest unit test file program
#
# This program does the following:
# 1. Copy all files in the directory specified as the first argument, to the
#    directory specified as the second argument.
# 2. Open the file specified as the third argument, and verify that it contains
#    the filenames of the files copied in the first step.
# 3. Write logfile to log directory
# 4. Verify that program specific arguments are "arg1 arg2"
#

from troilkatt_script import TroilkattScript
import sys, os, shutil

class ScriptPerDirTest(TroilkattScript):
    def __init__(self):
        TroilkattScript.__init__(self)
        
    """
    Unit test run function.
    """
    def run(self):        
        # 1. Copy files
        inputFiles = self.getAllFiles(self.inputDir)
        for f in inputFiles:
            shutil.copy(os.path.join(self.inputDir, f), self.outputDir)
         
        # 2. Parse meta-data
        metafile = os.path.join(self.metaDir, "filelist")
        lines = open(metafile).readlines()
        if len(lines) != len(inputFiles):
            print 'Number of input files (%d) != number of filenames in metafile (%d)' % (len(inputFiles), len(lines))
        
        inputBasenames = []
        for f in inputFiles:
            inputBasenames.append(os.path.basename(f))
        
        for l in lines:
            mf = os.path.basename(l.strip())
            if mf not in inputBasenames:
                print 'File %s in meta-file not found in input directory' % (mf)
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
    s = ScriptPerDirTest()
    s.run()