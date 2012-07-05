import os
from troilkatt_script import TroilkattScript

"""
Do sinfo analysis on PCL files
"""
class SpellQueries(TroilkattScript):
    
    """
    arguments: 
     [0] path to search_prog2 binary 
     [1] path to adder_converter2 binary
        
    @param: see description for super-class
    """
    def __init__(self):
        TroilkattScript.__init__(self)
                
        argsParts = self.args.split(" ")
        if (len(argsParts) != 2):
            raise Exception('Invalid arguments: %s' % (self.args))
        
        self.searchBin = argsParts[0]
        self.adderBin = argsParts[1]

    
    """
    Cleanup input directory by removing everything
    
    @return none
    """
    def cleanup(self):

        dirContent = os.listdir(self.inputDir)
        for fn in dirContent:    
            if fn.find('norm.mas.pcl') != -1:        
                os.remove(os.path.join(self.inputDir, fn))
            if fn == 'meta.tar.gz':
                os.remove(os.path.join(self.inputDir, fn))
            
        dirContent = os.listdir(self.byGenesDir)
        for fn in dirContent:            
            os.remove(os.path.join(self.byGenesDir, fn))
            
        dirContent = os.listdir(self.sinfoDir)
        for fn in dirContent:            
            os.remove(os.path.join(self.sinfoDir, fn))
            
        dirContent = os.listdir(self.queryDir)
        for fn in dirContent:            
            os.remove(os.path.join(self.queryDir, fn))

    
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
        # 0. Set and create temporary directories
        #                        
        self.byGenesDir = os.path.join(self.inputDir, "by_gene")
        os.mkdir(self.byGenesDir)
        self.sinfoDir = os.path.join(self.inputDir, "sinfo")
        os.mkdir(self.sinfoDir)
        self.queryDir = os.path.join(self.inputDir, "refseq96_q")
        os.mkdir(self.queryDir)
        
        #self.cleanup()
        #return None
        
        #
        # 1. Download PCL files
        # Note! The query files are currently used as input files
        # PCL file download is hard-coded
        #
        cmd = 'hadoop fs -get /user/larsab/troilkatt/spell-queries/pcl/* .'
        #cmd = 'ls'
        print 'Downloading PCL files'
        if os.system(cmd) != 0:
            print 'input file get failed: %s' % (cmd)
            self.logger.warning('input file get failed: %s' % (cmd))
            self.cleanup()
            return None
        
        #
        # 2. Download meta files
        #
        cmd1 = 'hadoop fs -get /user/larsab/troilkatt/spell-queries/meta/meta.tar.gz .'
        #cmd1 = 'ls'
        print 'Downloading and unpacking meta files'
        if os.system(cmd1) != 0:
            print 'meta file get failed: %s' % (cmd1)
            self.logger.warning('meta file get failed: %s' % (cmd1))
            self.cleanup()
            return None
        cmd2 = 'tar xvzf meta.tar.gz'
        #cmd2 = 'ls'
        if os.system(cmd2) != 0:
            print 'meta file unpack failed: %s' % (cmd2)
            self.logger.warning('meta file unpack failed: %s' % (cmd2))
            self.cleanup()
            return None
        
        #
        # 3. Run analyzer
        #
        print 'Running analyzer'
        containerBin = "/nhome/larsab/troilkatt/apps/bin/troilkatt-container"        
        for fn in inputFiles:                        
            cmd = '%s 12 -1 /bin/bash -c "%s %s gpl96.dset.list | %s 25000 3000" > %s 2> %s' % (containerBin,
                                                 self.searchBin,
                                                 fn,
                                                 self.adderBin,
                                                 os.path.join(self.logDir, 'query.output'),
                                                 os.path.join(self.logDir, 'query.error'))
            print cmd
            if os.system(cmd) != 0:
                print 'query failed: %s' % (cmd)
                self.logger.warning('query failed: %s' % (cmd))
                self.cleanup()
                return None
            
        outputFiles = os.listdir(self.byGenesDir)
        for of in outputFiles:
            print 'Move %s to %s' % (of, self.outputDir)
            os.rename(os.path.join(self.byGenesDir, of), 
                      os.path.join(self.outputDir, of))
            
        self.cleanup()
            
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
    s = SpellQueries()
    s.mainRun()
