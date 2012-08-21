import os, shutil
from troilkatt_script import TroilkattScript

"""
Convert a set of CEL files to a PCL file
"""
class Cel2Pcl(TroilkattScript):
    """
    arguments (read from sys.argv by the superclass constructor): 
    [0] R binary
    [1] R script to run
    [2] Organism code (hs = human, rn = rat, and so on)
        
    @param: see description for super-class
    """
    def __init__(self):
        TroilkattScript.__init__(self)
        self.parseScriptArgs()
        
    def parseScriptArgs(self):
        """
        Parse script specific arguments
        """
        if self.args == None:
            raise Exception("Invalid arguments: None")
        
        argsParts = self.args.split(" ")
        if (len(argsParts) != 3):
            raise Exception('Invalid arguments: %s' % (self.args))
        self.rBin = argsParts[0]
        self.rScript = argsParts[1]
        self.orgCode = argsParts[2]


    """
    Cleanup tmp directory by removing all extracted files

    @param none
    @return none
    """
    def cleanupTmpDir(self):
        # Clean directory by removing it and then recreating it
        shutil.rmtree(self.tmpDir)
        os.mkdir(self.tmpDir)
    
    """
    Merge partial files output from the R script        
    
    @param R output filename prefix
    """
    def mergeAndConvert(self, tmpName):    
        partFiles = os.listdir(self.tmpDir)
        for f in partFiles:
            if f.find(tmpName) != -1:
                if f.find(".single") != -1 or f.find(".zero") != -1:
                    continue
                #    newName = f.replace('.tmp', '')
                #    os.rename(os.path.join(self.inputDir, f),
                #              os.path.join("/nhome/larsab/troilkatt/single", newName))
                    
                
                if f.find('platform') != -1:
                    newName = f.replace('.tmp.platform.', '.pcl')
                else:
                    newName = f.replace('.tmp', '.pcl')
    
                fin = open(os.path.join(self.tmpDir, f))
                fout = open(os.path.join(self.outputDir, newName), 'w')
                headerLine = fin.readline()
                # Add extra columns for additional gene ID and GWEIGHT
                fout.write('ENTREZ_ID\tENTREZ_ID\tGWEIGHT')
                headerParts = headerLine.split('\t')
                for h in headerParts[1:]: # Ignore empty first column
                    fout.write('\t' + h)
                # last column contains newline
    
                # Add EWEIGHT column
                fout.write('EWEIGHT\t\t1')
                for h in headerParts[1:]:
                    fout.write('\t1')
                fout.write('\n')
    
                # Fix gene IDs and add the two additional columns
                while 1:
                    l = fin.readline()
                    if l == '':
                        break
    
                    cols = l.split('\t')
                    if len(cols) < 2:
                        continue
                    fout.write('%s\t%s\t1' % (cols[0], cols[0]))
                    for c in cols[1:]:
                        fout.write('\t' + c.strip())
                    fout.write('\n')
                fin.close()
                fout.close()
        
    
    """
    Script specific run function. A sub-class should implement this function.

    @return: none
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
            #print 'Execute: %s' % (cmd)
            self.logger.info('Execute: %s' % (cmd))
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
                    #print 'Execute: %s' % (cmd)
                    self.logger.info('Execute: %s' % (cmd))
                    #cmd = "ps"
                    if os.system(cmd) != 0:
                        print 'gunzip failed: %s' % (cmd)
                        self.logger.warning('gunzip failed: %s' % (cmd))
                        continue                    
            
            #
            # 3. Run R script to create PCL file in input directory 
            #
            tmpName = tarName + '.tmp'
            rOutputFilename = os.path.join(self.tmpDir, tmpName)        
            cmd = ' %s --no-save --args %s %s %s < %s > %s 2> %s' % (self.rBin,
                                                                     self.tmpDir,
                                                                     self.orgCode,
                                                                     rOutputFilename,
                                                                     self.rScript,
                                                                     os.path.join(self.logDir, 'R.output'),
                                                                     os.path.join(self.logDir, 'R.error'))
            
            
            #print 'Execute: %s' % (cmd)
            self.logger.info('Execute: %s' % (cmd))
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
    s = Cel2Pcl()
    s.run()
