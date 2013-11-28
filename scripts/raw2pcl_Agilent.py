import os, shutil, re
from troilkatt_script import TroilkattScript

"""
Convert a set of RAW Agilent files to PCL.
Based on prepare_agilent_files.py by qzhu
author: qzhu, atadych
"""
# <arguments>script_per_file 2 8096 /usr/local/bin/python
# TROILKATT.SCRIPTS/cel2pcl.py /usr/local/bin/R TROILKATT.SCRIPTS/R/ProcessCEL.R Hs</arguments>
class Raw2Pcl(TroilkattScript):

    # TODO: merge them into one?
    GPR_SCRIPT = "script_gpr.R"
    SINGLE_SCRIPT = "script_single.R"
    DUAL_SCRIPT = "script_dual.R"

    """
    arguments (read from sys.argv by the superclass constructor):
    [0] platform (eg. GPL6480)
    [1] R binary

    @param: see description for super-class
    """
    def __init__(self):
        TroilkattScript.__init__(self)
        self.parseScriptArgs()
        print 'In: ' + self.inputDir
        print 'Out: ' + self.outputDir
        print 'Meta: ' + self.metaDir
        print 'Tmp: ' + self.tmpDir
        print  'Log: ' + self.logDir
        print 'rBin ' + self.rBin
        print 'Platform: ' + self.platform
        print 'rScripts: ' + self.rScriptPath
        print 'agilent-meta: ' + self.agilentMeta
        if self.inputFilename:
            print 'inputFilename: ' + self.inputFilename

    def parseScriptArgs(self):
        """
        Parse script specific arguments
        rBin - path to R
        rScriptPath - path to R scripts
        platform - what platform
        """
        print self.inputDir
        if self.args == None:
            raise Exception("Invalid additional arguments: None")

        argsParts = self.args.split(" ")
        if (len(argsParts) != 4):
            raise Exception('Invalid arguments: %s. Should be rBin rScriptPath agilentMeta Platform' % (self.args))
        self.rBin = argsParts[0]
        self.rScriptPath = argsParts[1]
        self.agilentMeta = argsParts[2]
        self.platform = argsParts[3]


    def cleanupTmpDir(self):
        """
        Cleanup tmp directory by removing all extracted files

        @param none
        @return none
        """
        print 'About to clean: ' + self.tmpDir
        # Clean directory by removing it and then recreating it
        shutil.rmtree(self.tmpDir)
        os.mkdir(self.tmpDir)
        print 'Done cleaning tmp'


    def read_dataset_metafile(self, n):  # gets all gsm in this dataset (pertaining to this platform)
        f = open(n)
        m = set([])
        for l in f:
            l = l.rstrip("\n")
            l = l.split("|")
            gsm = l[0]
            m.add(gsm)
        f.close()
        print 'Samples: %s' % m
        return m


    def processFile(self,fn):
        if not self.endsWith(fn, '.tar'):
        # Not a tar file
            return


        #
        # 0. Prepare
        #
        self.cleanupTmpDir()

            #
            # 1. Unpack tar file
        #
        print 'Unpacking if necessary'
        tarName = os.path.basename(fn).split(".")[0]
        gseName = (tarName.split("_")[0]).split("-")[0]
        cmd = 'tar xvf %s -C %s > %s 2> %s' % (os.path.join(self.inputDir, fn),
                                                       self.tmpDir,
                                                       os.path.join(self.logDir, os.path.basename(fn) + '.untar.output'),
                                                       os.path.join(self.logDir, os.path.basename(fn) + '.untar.error'))
        print 'Execute: %s\n' % (cmd)
        self.logger.info('Execute: %s' % (cmd))
        if os.system(cmd) != 0:
            print 'Unpack failed: %s' % (cmd)
            self.logger.warning('Unpack failed: %s' % (cmd))
            #continue
            return

            #
            # 2. Decompress RAW files
            #
        os.chdir(self.tmpDir)
        unpackedFiles = os.listdir('.')
        for fn in unpackedFiles:
            if self.endsWith(fn.lower(), '.gz'):
                cmd = 'gunzip -f %s > %s 2> %s' % (fn,
                                                           os.path.join(self.logDir, fn + '.gunzip.output'),
                                                           os.path.join(self.logDir, fn + '.gunzip.error'))
                print 'Execute: %s' % (cmd)
                self.logger.info('Execute: %s' % (cmd))
                # cmd = "ps"
                if os.system(cmd) != 0:
                    print 'gunzip failed: %s' % (cmd)
                    self.logger.warning('gunzip failed: %s' % (cmd))
                    continue


        #
        # 3. Get the GSM information from meta data
        #
        metaFile = os.path.join(self.agilentMeta, self.platform, gseName)
        print metaFile
        gsmSet = self.read_dataset_metafile(metaFile)

        #
        # Check if 2 channels?
        #
        isTwoChannel = False
        fo = open(metaFile)
        fl = fo.readline().rstrip("\n")
        if "Channel 1" in fl:
            isTwoChannel = True
        fo.close()

        isGPR = False
        gsms = set([])
        gsms_full = set([])
        #
        # List all the files that start with GSM name
        #
        os.chdir(self.tmpDir)
        unpackedFiles = os.listdir('.')
        for fn in unpackedFiles:
            if fn.startswith("GSM"):
                mg = re.match("(GSM\d+)(\w+\-*\w*)", fn)
                # Finding GSM name in label like GSM123123_1-1.txt or GSM234444.txt
                gsm = mg.group(1)
                gsm_full_name = fn
                if not gsm in gsmSet:
                    print "Warning", gsm, "is not in platform, deleted"
                    continue

                if gsm in gsms:
                    print "Warning", gsm, "appears at least 2 times. Ignoring " + gsm_full_name
                    continue

                print 'In file %s there is GSM name: %s' % (gsm_full_name, gsm)
                gsms_full.add(gsm_full_name)
                gsms.add(gsm)

        # Create targets file based on all GSM files (GSM filenames can be longer like GSM123123_1-1.txt)
        #
        os.chdir(self.tmpDir)
        targets = "targets.txt"
        fw = open(targets, "w")
        fw.write("FileName\tCy3\tCy5\n")
        for i in sorted(gsms_full):
            fw.write("%s\t1\t2\n" % (os.path.join(self.tmpDir, i)))
            print 'Wrote "%s\t1\t2\n' % (os.path.join(self.tmpDir, i))
        fw.close()
        print 'Wrote to dir: %s' % self.tmpDir

        # create file with correct column names GSM123123
        columnFile = "columns.txt"
        fw = open(columnFile, "w")
        for gsm in sorted(gsms):
            fw.write("%s\n" % gsm)
        fw.close()

        #
        # Determine which R script to use
        #
        if isGPR:
            self.rScript = self.GPR_SCRIPT
        else:
            if isTwoChannel:
                self.rScript = self.DUAL_SCRIPT
            else:
                self.rScript = self.SINGLE_SCRIPT



        #
        # 3. Run R script to create PCL file in the output directory
        #
        mg = re.match("(GSE\d+)", tarName)
        tmpName = mg.group(1) + '-' + self.platform + '.pcl'
        print tmpName
        rOutputFilename = os.path.join(self.outputDir, tmpName)
        # /usr/local/bin/R --no-save --args ~/tmp/TMP/targets.txt ~/tmp/TMP/OUTOWA.txt ~/tmp/TMP/cols.txt <  R/script_single.R
        cmd = ' %s --no-save --args %s %s %s < %s > %s 2> %s' % (self.rBin,
                                                             os.path.join(self.tmpDir, targets),
                                                             rOutputFilename,
                                                             os.path.join(self.tmpDir, columnFile),
                                                             os.path.join(self.rScriptPath, self.rScript),
                                                             os.path.join(self.logDir, 'R.output'),
                                                             os.path.join(self.logDir, 'R.error'))

        print 'Execute: %s' % (cmd)
        self.logger.info('Execute: %s' % (cmd))
        if os.system(cmd) != 0:
            print 'R script failed'
            self.logger.warning('R script failed')

        print 'Done. Output file in: ' + rOutputFilename

#        if tarName == None:
#           raise Exception('No tar file not found in input directory: ' % (self.inputDir))
#       else:
# self.cleanupTmpDir()
#          os.chdir(oldCwd)

        """
    Script specific run function. A sub-class should implement this function.

    @return: none
    """
    def run(self):
        if not os.path.isdir(self.outputDir):
            os.mkdir(self.outputDir)
        if self.inputFilename:
            print 'PROCESSING single file: ' + self.inputFilename
            self.processFile(self.inputFilename)
        else:
            print 'PROCESSING all files from: ' + self.inputDir
            inputFiles = os.listdir(self.inputDir)
            for fn in inputFiles:
                self.processFile(fn)

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
    s = Raw2Pcl()
    s.run()
