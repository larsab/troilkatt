import sys, logging
import os
import time

"""
Parent class for all troilkatt scripts. It provides functions for parsing arguments
and the troilkatt configuration file. In addition it has a run function that runs
the process() scripts.
"""
class TroilkattScript:
    def __init__(self):
        self.defaultArgs = {'configFile': 'conf/troilkatt.xml',
                            'datasetFile': 'conf/pipelines',
                            'logging': 'debug'}
        
        # 
        # Parse arguments, configuration file and setup logging
        #
        self.name = sys.argv[0]
        args, options = self.parseArgs(self.name, sys.argv[1:])
        self.inputDir = args['inputDir']
        self.outputDir = args['outputDir']
        self.tmpDir = args['tmpDir']
        self.logDir = args['logDir']
        self.metaDir = args['metaDir']        
        programArguments = args['stageArgs']
        
        if options.logging in ['debug', 'info']:
            print 'Parse configuration file: %s' % (options.configFile)
        from troilkatt_properties import TroilkattProperties
        self.troilkattProperties = TroilkattProperties(options.configFile)
        self.globalMetaDir = self.troilkattProperties.get('troilkatt.globalfs.global-meta.dir')
                
        # self.args is initialized below
        self.timestamp = options.timestamp
        self.inputFilename = options.inputFilename    
                
        logname = os.path.basename(self.name)
        if self.inputFilename != None:
            logname = logname.replace('.py', '-%s.log' % (self.inputFilename))
        else:
            logname = logname.replace('.py', '.log')
        self.setupLogging(options.logging, self.logDir, logname)            
        self.logger = logging.getLogger('script.%s' % (self.name))                
        
        # Set troilkatt variables in arguments string
        # Not must be initialized last since it depends on many of the other variables
        if programArguments == None:
            self.args = None
        else:
            self.args = self.setTroilkattVariables(programArguments)

    """
    Parse standalone command line arguments. See the usage() output for the currently supported command
    line arguments. Note also that more options are specified in the troilkatt.xml configuration
    file.        
    """
    def parseArgs(self, progrname, args):
        from optparse import OptionParser
        usage = """
usage %prog [options] inputDir outputDir metaDir globalMetaDir logDir tmpDir <args>
        
Required arguments:
   input-dir      Directory containing files to process (or where to store downloaded files)
   output-dir     Directory where output files are stored.
   meta-dir       Directory for stage meta-data
   log-dir        Directory where logfiles are stored.
   tmp-dir        Directory for temporary files
   
Optional arguments:
   args           Stage specific arguments.
    
Options:
   -c FILE        Configuration file
   -l LEVEL       Logging level to use
   -t TIMESTAMO   TIMESTAMP for this iteration
   -f FILE        Execute script only for this FILE in the input directory
"""       
        parser = OptionParser(usage)        
        
        parser.add_option("-c", "--config-file", 
                          action="store", type="string", dest="configFile", 
                          default=self.defaultArgs['configFile'],
                          metavar="FILE", help="Specify configuration FILE to use [default: %default]")
        parser.add_option("-l", "--logging", 
                          action="store", type="string", dest="logging", default=self.defaultArgs['logging'],
                          metavar="LEVEL", help="Specify logging level to use {debug, info, warning, error, or critical} [default: %default]")
        parser.add_option("-t", "--timestamp", 
                          action="store", type="string", dest="timestamp", default=None,
                          metavar="INT", help="Timestamp given as INTeger [default: %default]")
        parser.add_option("-f", "--file", 
                          action="store", type="string", dest="inputFilename", default=None,
                          metavar="INT", help="File in input directory for which script is executed [default: %default]")
        #parser.add_option("-h", "--help", 
        #                action="store_true", dest="help",
        #                  help="Display command line options")

        options, args = parser.parse_args()
        if len(args) < 5:
            parser.error("Incorrect number of arguments")        
        
        argDict = {'inputDir': args[0],        
                   'outputDir': args[1],
                   'metaDir': args[2],                   
                   'logDir': args[3],
                   'tmpDir': args[4]}    
        optionalStart = 5
        
        stageArgsStr = None        
        for a in args[optionalStart:]:
            if a[0] != '-':
                if stageArgsStr == None:
                    stageArgsStr = a
                else:
                    stageArgsStr = stageArgsStr + ' ' + a
                optionalStart += 1
        argDict['stageArgs'] = stageArgsStr
                    
        return (argDict, options)
    
    """
    Setup the logging module.
    
    @param levelName: minimum logging level. All messages with importance higher than this are logged.
     (levels = debug, info, warning, error, and critical).
    @param logDir: the log files are stored in this directory.
    @param filename: logfile
    """
    def setupLogging(self, levelName, logDir, filename='logfile'):
        LEVELS = {'debug': logging.DEBUG,
                  'info': logging.INFO,
                  'warning': logging.WARNING,
                  'error': logging.ERROR,
                  'critical': logging.CRITICAL} 
        
        level = LEVELS.get(levelName, logging.NOTSET)
        
        if not os.path.isdir(logDir):
            if levelName in ['debug', 'info']:
                print 'Creating new logfile directory: %s' % (logDir)
            os.mkdir(logDir)
        
        
        logname = os.path.join(logDir, filename)
        logging.basicConfig(level=level, filename=logname, filemode='w')    
        
        print 'Logging initialized: log is stored in: %s' % (logname)
    
    """
    Replace TROILKATT. substrings with per process variables.
    
    @param argsStr: string that contains TROILKATT substrings to be replaced    
    @param inputDir: input directory. Can also be None (default)
    @param fileset: fileset being processed ('all', 'new', 'updated', or 'deleted'). Can also
     be None (default)
    
    @return: args string with TROILKATT substrings replaced 
    """
    def setTroilkattVariables(self, argsStr):
        if argsStr == None:
            raise TypeError('Invalid argument string: None')
        
        newStr = argsStr
        newStr = newStr.replace('TROILKATT.INPUT_DIR', 
                                os.path.normpath(self.inputDir))
        newStr = newStr.replace('TROILKATT.OUTPUT_DIR', 
                                os.path.normpath(self.outputDir))        
        newStr = newStr.replace('TROILKATT.LOG_DIR', 
                                os.path.normpath(self.logDir))        
        
        newStr = newStr.replace("TROILKATT.DIR", 
                os.path.normpath(self.troilkattProperties.get("troilkatt.localfs.dir")))
        newStr = newStr.replace("TROILKATT.BIN", 
                os.path.normpath(self.troilkattProperties.get("troilkatt.localfs.binary.dir")))
        newStr = newStr.replace("TROILKATT.UTILS", 
                os.path.normpath(self.troilkattProperties.get("troilkatt.localfs.utils.dir")))
        newStr = newStr.replace("TROILKATT.GLOBALMETA_DIR", 
                os.path.normpath(self.troilkattProperties.get("troilkatt.globalfs.global-meta.dir")))
        newStr = newStr.replace("TROILKATT.SCRIPTS", 
                os.path.normpath(self.troilkattProperties.get("troilkatt.localfs.scripts.dir")))
        
        # Command line argument helpers
        newStr = newStr.replace("TROILKATT.REDIRECT_OUTPUT", ">")
        newStr = newStr.replace("TROILKATT.REDIRECT_ERROR", "2>")
        newStr = newStr.replace("TROILKATT.REDIRECT_INPUT", "<")
        newStr = newStr.replace("TROILKATT.SEPERATE_COMMAND", ";")
            
        return newStr
    
    """
    Replace either TROILKATT.FILE or TROILKATT.FILE_NOEXT substring respectively with the 
    filename or the filename without any extensions.
    
    @param argStr: string that contains the substring to replace
    @param inputFile: input filename (absolute)
    
    @return: string where TROILKATT.FILE is replaced by the basename in filename
    """ 
    def setTroilkattFilename(self, argStr, inputFile):
        basename = os.path.basename(inputFile)  
        noext = basename.split('.')[0]
        
        newStr = argStr.replace('TROILKATT.FILE_NOEXT', noext)
        newStr = newStr.replace('TROILKATT.FILE', basename) 
                    
        return newStr
        
    """
    @param dir: directory to read.
    @param absoluteFilenames: true if absolute filesnames should be returned, otherwise
     the filenames are relative to the directory.
    
    @return: a list with all files in the input directory.
    
    """    
    def getAllFiles(self, dir, absoluteFilenames=True):
        allFiles = os.listdir(dir)
        
        for f in allFiles:
            if f[0] == '.':
                allFiles.remove(f)
        
        if absoluteFilenames:
            return self.relative2absolute(dir, allFiles)
        else:
            return allFiles

    """
    Create list with absolute pathnames.
    
    @param dir: directory with files
    @param filenames: list with relative filesnames
        
    @return: list with absolute filenames
    """
    def relative2absolute(self, dir, files):
        if files == None:
            
            return None
        
        absFiles = []
        for f in files:
            absFiles.append(os.path.join(dir, f))
            
        return absFiles  
    
    """
    @return: true if the filename ends with the given string
    """
    def endsWith(self, filename, postfix):
        if filename.rfind(postfix) == len(filename) - len(postfix):
            return 1
        else:
            return 0 

                
    """
    Script specific run function. A sub-class should implement this function.
    """
    def run(self):
        raise Exception("Subclass should implement the run() function")
    
     
    
"""
Run a troilkatt script

Command line arguments: trilkatt_script.py [options] inputDir outputDir metaDir globalMetaDir logDir <args>, where
    options: troilkatt specific options (configuration file, logging level, etc)
    inputDir: input directory
    outputDir: output directory
    metaDir: stage specific metafile directory
    globalMetaDir: global metadile directory
    logDir: logfile directory
    tmpDir: temp file directory
    <args>: optional list of stage specific arguments
        
The decription in usage() has additional details. 
"""
if __name__ == '__main__':
    s = TroilkattScript()
    s.run()
