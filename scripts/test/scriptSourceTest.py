#
# ExecuteDirTest unit test file program
#
# This program does the following:
# 1. Copy all files in the metafile specified as the first argument, to the
#    directory specified as the second argument.
# 2. Verify that the script specific arguments are "arg1 arg2"
# 3. Write logfile to log directory
#

from troilkatt_script import TroilkattScript
import sys, os, shutil

class ScriptSourceTest(TroilkattScript):
    def __init__(self):
        TroilkattScript.__init__(self)


    """
    Unit test run function.
    """
    def run(self):
        print 'Checkdir: ' + self.metaDir 
        metaFiles = self.getAllFiles(self.metaDir)
        if len(metaFiles) != 1:
            print 'Too many/ or too few metafiles in %s: %d (1 expected)' % (self.metaDir, len(metaFiles))
            sys.exit(-1)
                 
        metafile = metaFiles[0]
        print metafile
        lines = open(metafile).readlines()
        for l in lines:
            mf = l.strip()
            shutil.copy(mf, self.outputDir)
            
        myArgs = self.args.split(" ")
        if len(myArgs) != 2:
            print 'Invalid arguments length: %d (expected 2)' % (len(myArgs))
            sys.exit(-1)
        if myArgs[0] != "arg1" or myArgs[1] != "arg2":
            print 'Invalid arguments: %s' % (myArgs)
            sys.exit(-1)
            
        self.logger.warning("All tests passed")


if __name__ == '__main__':
    s = ScriptSourceTest()
    s.run()