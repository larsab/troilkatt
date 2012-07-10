import os
from cel2pcl import Cel2Pcl

"""
Convert a set of CEL files to a PCL file
"""
class Cel2PclAffy(Cel2Pcl):
    """
    arguments:
    [0] R binary
    [1] Organism specific Affy R script
        
    @param: see description for super-class
    """
    def __init__(self):
        Cel2Pcl.__init__(self)                                
        
    def parseScriptArgs(self):
        """
        Parse script specific arguments
        
        This function will be called from the superclass constructor.
        """
        argsParts = self.args.split(" ")
        if (len(argsParts) != 2):
            raise Exception('Invalid arguments: %s' % (self.args))
        self.rBin = argsParts[0]
        self.rScript = argsParts[1]        
        
    """
    Script specific run function. A sub-class should implement this function.

    @return: None
    """
    def run(self):
        oldCwd = os.getcwd()
               
        inputFiles = os.listdir(self.inputDir)
        tarName = None
        for fn in inputFiles:
            if not self.endsWith(fn, '.tar'):
                # Not a tar file
                continue
            
            #
            # 0. Prepare
            #
            self.cleanupTmpDir()            
            
            #
            # 1. Unpack tar file
            #
            tarName = os.path.basename(fn).split(".")[0]                
            cmd = 'tar xvf %s -C %s > %s 2> %s' % (os.path.join(self.inputDir, fn),
                                                   self.tmpDir, 
                                                   os.path.join(self.logDir, os.path.basename(fn) + '.untar.output'),
                                                   os.path.join(self.logDir, os.path.basename(fn) + '.untar.error'))
            print 'Execute: %s' % (cmd)
            #cmd = "ps"
            if os.system(cmd) != 0:
                print 'Unpack failed: %s' % (cmd)
                self.logger.warning('Unpack failed: %s' % (cmd))                        
                continue
            
            #
            # 2. Decompress CEL files
            #    
            os.chdir(self.tmpDir)
            unpackedFiles = os.listdir('.')
            for fn in unpackedFiles:
                if self.endsWith(fn.lower(), 'cel.gz'):
                    cmd = 'gunzip -f %s > %s 2> %s' % (fn,
                                                       os.path.join(self.logDir, fn + '.gunzip.output'),
                                                       os.path.join(self.logDir, fn + '.gunzip.error'))
                    print 'Execute: %s' % (cmd)
                    #cmd = "ps"
                    if os.system(cmd) != 0:
                        print 'gunzip failed: %s' % (cmd)
                        self.logger.warning('gunzip failed: %s' % (cmd))
                        continue                    
            
            #
            # 3. Run R script to create PCL file in input directory 
            #
            tmpName = tarName + '.tmp'
            mapName = tarName + ".map"
            rOutputFilename = os.path.join(self.tmpDir, tmpName)
            rMapFilename = os.path.join(self.tmpDir, mapName)        
            cmd = ' %s --no-save --args %s %s %s < %s > %s 2> %s' % (self.rBin,
                                                                     self.tmpDir,
                                                                     rOutputFilename,
                                                                     rMapFilename,                                                                     
                                                                     self.rScript,
                                                                     os.path.join(self.logDir, 'R.output'),
                                                                     os.path.join(self.logDir, 'R.error'))
            
            
            print 'Execute: %s' % (cmd)
            #cmd = "ls"
            if os.system(cmd) != 0:
                print 'R script failed'
                self.logger.warning('R script failed')
                continue
            
            #
            # 4. Merge and convert partial files
            #
            #cmd = '/nhome/larsab/troilkatt/apps/bin/troilkatt-container 8 -1 /nhome/larsab/troilkatt/apps/bin/python /nhome/larsab/skarntyde/troilkatt/src/scripts/mergeCelParts.py %s %s %s' % (self.inputDir, 
            #                                                                                                                      tarName,
            #                                                                                                                      self.outputDir)
            #
            #print 'Execute: %s' % (cmd)
            #cmd = "ls"
            #if os.system(cmd) != 0:
            #    print 'R script failed'
            #    self.logger.warning('R script failed')        
            self.mergeAndConvert(tmpName)        
                    
        if tarName == None:
            raise Exception('No tar file not found in input directory: ' % (self.inputDir))
        else:
            self.cleanupTmpDir()
            os.chdir(oldCwd)
            
"""
Run a troilkatt script

Command line arguments: %s input-dir output-dir log-dir args, where
    inputDir: input directory
    outputDir: output directory
    metaDir: stage specific metafile directory
    globalMetaDir: global metadile directory
    logDir: logfile directory
    tmpDir: temp file directory
    args[0]: R binary
    args[1]: R script to execute
    args[1]: Organism code used by R script

The decription in usage() has additional details. 
"""
if __name__ == '__main__':
    import os    
    s = Cel2PclAffy()
    s.run()
    
