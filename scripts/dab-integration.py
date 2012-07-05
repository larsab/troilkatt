import os
from troilkatt_script import TroilkattScript

"""
Integrate a set of dab files.
"""
class DabIntegration(TroilkattScript):
    """
    arguments: 
    [0] Directory with Sleipnir binaries
    [1] Gold standard file (<organism>_positives.dab)
    [2] Gene file (index<tab>gene ID<newline>)
        
    @param: see description for super-class
    """
    def __init__(self):
        TroilkattScript.__init__(self)
        
        # Note that args and self.args may differ since TroilkattScript.__init__() sets the TROILKATT.*
        # substrings        
        #self.geneSet = self.initGeneSet(self.args)
        
        self.dabFiles = []
        files = os.listdir(self.inputDir)
        for f in files:
            if f.endswith('.dab'):
                self.dabFiles.append(f)
        
        self.dabFiles.sort()
        
        self.metaFileDir = os.path.join(self.inputDir, 'meta')
        os.makedirs(self.metaFileDir)
        self.datasetsFile =  os.path.join(self.metaFileDir, 'datasets.txt')
        of = open(self.datasetsFile, 'w')
        id = 1
        for f in self.dabFiles:
            of.write('%d\t%s\n' % (id, f))
            id += 1
        of.close()
            
        self.zerosFile = os.path.join(self.metaFileDir, 'zeros.txt')
        of = open(self.zerosFile, 'w')
        of.close()

        self.sleipnirDir = args[0]        
        self.goldStandard = args[1]
        self.genesFile = args[2]
        
        self.countsDir = os.path.join(self.outputDir, 'counts')
        os.makedirs(self.countsDir)
        self.networkDir = os.path.join(self.outputDir, 'network')
        os.makedirs(self.networkDir)
        
        self.createQuantFiles()

    """
    Create microarray quant files
    """
    def createQuantFiles(self):
        for f in self.dabFiles:
            id = f.split('.')[0]
            of = open(os.path.join(self.inputDir, id + '.quant'), 'w')
            of.write('-1.5\t-0.5\t0.5\t1.5\t2.5\t3.5\t4.5')
            
    """
    Do integration
    """
    def doIntegration(self):
        counterBin = os.path.join(self.sleipnirDir, 'Counter')
        networksBin = os.path.join(self.networksDir, 'networks.bin')
        cmd1 = '%s -w %s -o %s -d %s -Z %s' % (counterBin, self.goldStandard, self.countsDir, self.inputDir, self.zerosFile)
        cmd2 = '%s -k %s -o %s -s %s -b %s -Z %s' % (counterBin, self.countsDir, networksBin, self.datasetsFile, os.path.join(self.countsDir, 'global.txt'), self.zerosFile)
        cmd3 = '%s -n %s -o %s -d %s -s %s -Z %s -e %s' % (counterBin, networksBin, self.outputDir, self.inputDir, self.datasetsFile, self.zerosFile, self.genesFile)

        os.system(cmd1)
        os.system(cmd2)
        os.system(cmd3)
        
"""
Run a troilkatt script

Command line arguments: %s input-dir output-dir log-dir args, where
   input-dir      Directory containing files to process (or where to store downloaded files)
   output-dir     Directory where output files are stored.
   log-dir        Directory where logfiles are stored.
   args[0]        Directory with Sleipnir binaries
   args[1]        Gold standard file (<organism>_positives.dab)
   args[2]        Gene file (index<tab>gene ID<newline>)

The decription in usage() has additional details. 
"""
if __name__ == '__main__':
    s = DabIntegration()
    s.mainRun()
