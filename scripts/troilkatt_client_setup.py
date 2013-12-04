"""
Setup a cluster for troilkatt.

Run this script from the node where the main troilkatt server is to be run.

Command line arguments: troilkatt_script.py configFile, where
    configFile: configuration file to use to setup the cluster 
"""

import os, os.path
import stat

# User: rwx, group: ---, other: --- 
SERVER_DIR_MODE = "rwx------"

# User: rwx, group: rwx, other: rwx
CLIENT_DIR_MODE =  "rwxrwxrwx"

# User: rwx, group: r-x, other: r-x
BIN_DIR_MODE =  "rwxr-xr-x"

def modeStr2Bits(modeStr):
    """
    Convert an "ls -l" mode string (rw-rw----) to a bitmap that can be used with chmod.
    
    """
    assert(len(modeStr) == 9)
    
    mode = 0
    
    # Owner
    if modeStr[0] == 'r':
        mode = mode & stat.S_IRUSR
    if modeStr[1] == 'w':
        mode = mode & stat.S_IWUSR
    if modeStr[2] == 'x':
        mode = mode & stat.S_IXUSR
        
    # Group
    if modeStr[3] == 'r':
        mode = mode & stat.S_IRGRP
    if modeStr[4] == 'w':
        mode = mode & stat.S_IWGRP
    if modeStr[5] == 'x':
        mode = mode & stat.S_IXGRP
        
    # Other
    if modeStr[6] == 'r':
        mode = mode & stat.S_IROTH
    if modeStr[7] == 'w':
        mode = mode & stat.S_IWOTH
    if modeStr[8] == 'x':
        mode = mode & stat.S_IXOTH
        
    return mode

def modeBits2Str(mode):
    """
    Convert file permissions to a string similar to "ls -l" output (drw-rw----).
    
    Note the first character in the string is the directory bit.
    """    
    rv = None

    # Owner
    if mode & stat.S_IRUSR:
        rv = "r"
    else:
        rv = "-"    
    if mode & stat.S_IWUSR:
        rv = rv + "w"
    else:
        rv = rv + "-"   
    if mode & stat.S_IXUSR:
        rv = rv + "x"
    else:
        rv = rv + "-"
        
    # Group
    if mode & stat.S_IRGRP:
        rv = rv + "r"
    else:
        rv = rv + "-"    
    if mode & stat.S_IWGRP:
        rv = rv + "w"
    else:
        rv = rv + "-"   
    if mode & stat.S_IXGRP:
        rv = rv + "x"
    else:
        rv = rv + "-"
        
    # Other
    if mode & stat.S_IROTH:
        rv = rv + "r"
    else:
        rv = rv + "-"    
    if mode & stat.S_IWOTH:
        rv = rv + "w"
    else:
        rv = rv + "-"        
    if mode & stat.S_IXOTH:
        rv = rv + "x"
    else:
        rv = rv + "-"
     
    return rv
    
def createLocalDir(dir, modeStr):
    """
    Create a directory with the given permission mode. 
    
    Return True on success, False otherwise.
    """
    
    mode = modeStr2Bits(modeStr)
    
    if os.path.isdir(dir):
        print "\tDirectory already exists: %s" % (dir)
        return verifyLocalDir(dir, mode)
    else:
        try:
            os.mkdir(dir, mode)
            print "\tDirectory created: %s" % (dir)
            print "\tMode: %s" % (mode)
            return verifyLocalDir(dir, mode)
        except OSError, e:
            print '\tCould not create directory: %s' % (dir)
            return False
        
def createHDFSDir(dir, modeStr):
    """
    Create a directory in HDFS with the given permission mode. 
    
    Return True (always since there are no tests).
    """
    
    mode = modeStr2Bits(modeStr)
    
    print '\Create hadoop directory'
    print '\tNote! No automatic checks'
    os.system('hadoop fs -mkdir %s' % (dir))
    os.system('hadoop fs -chmod %s' % (modeStr))
    
    return True # always
        
def createClientDirs(dir, modeStr, hosts, clientScript):
    """
    Create a directory on clients with the given permission mode.
    
    @param dir: directory to create
    @param modeStr: permission bits (in string format) for created directory 
    @param hosts: list of client hostnames
    @param clientScript: absolute path to "troilkatt_client_setup.py" script
    @return True on success, False otherwise.
    """
    for h in hosts:
        print'\tCreate directory %s on %s with mode %s' % (h, dir, modeStr)
        if os.system("ssh %s '%s %s %s'") % (h, clientScript, dir, modeStr) != 0:
            print '\tRemote command Failed'
            return False
    
    return True
        
def verifyLocalDir(dir, modeStr):
    """
    Make sure that the directory has the given mode. Return True or False.
    """
    
    if os.path.isdir(dir) == False:
        print '\tNot a directory: %s' % (dir)
        return False # Not a directory
    
    dirMode = os.stat(dir)
    
    if modeStr == modeBits2Str(dirMode):
        return True
    else:
        return False
    
def verifyLocalFile(filename, modeStr):
    """
    Make sure that the directory has the given mode. Return True or False.
    """
    
    if os.path.isfile(filename) == False:
        print '\tNot a file: %s' % (filename)
        return False # Not a file
    
    fileMode = os.stat(filename)
    
    if modeStr == modeBits2Str(fileMode):
        return True
    else:
        return False
        
if __name__ == '__main__':
    """
    Setup a cluster
    """
    import sys
    
    assert(len(sys.argv) == 3), "Usage %s configFile hostfile" % (sys.argv[0])
    
    configFile = sys.argv[1]
    print "Configuration file is: %s" % (configFile)
    
    from troilkatt_properties import TroilkattProperties
    tp = TroilkattProperties(configFile)
    
    hosts = parseHostfile(sys.argv[2])
    
    # Guessing the path for the troilkatt_client_setup.py scrips
    clientScript= os.path.join(os.getcwd(), sys.argv[0])
    assert(os.path.isfile(clientScript)), 'Could not find troilkatt_client_setup.py script (is not %s)' % (clientScript) 
    
    print "Admin email is: %s" % (tp.get("troilkatt.admin.email"))
    persistentStorage = tp.get("troilkatt.persistent.storage")
    print "Using %s for persistent storage" % (persistentStorage)
    print "Global-meta data versions retained for %s days (-1 = forever)" % (tp.get("troilkatt.global-meta.retain.days"))
    print "Persistent status file: %s" % (tp.get("troilkatt.tfs.status.file"))
    print "Update interval: %s hours" % (tp.get("troilkatt.update.interval.hours"))
    print "SGE slots per node: %s" % (tp.get("troilkatt.sge.slots.per.node"))
    print "MongoDB server: %s:%s" % (tp.get("troilkatt.mongodb.server.host"), tp.get("troilkatt.mongodb.server.port"))
        
    failures = 0
    
    print "Creating local directories"
    # Client directories are created below
    print "\tTroilkatt server local directory"
    if createLocalDir(tp.get("troilkatt.localfs.dir"), SERVER_DIR_MODE) != True:
        print '\tFAILED'
        failures += 1
    # Only the server access this directory
    print "\n\tTroilkatt server log directory"
    if createLocalDir(tp.get("troilkatt.localfs.log.dir"), SERVER_DIR_MODE) != True:
        print '\tFAILED'
        failures += 1
    # This directory should be globally accessible
    print "\n\tGlobal-meta directory"
    if createLocalDir(tp.get("troilkatt.globalfs.global-meta.dir"), CLIENT_DIR_MODE) != True:
        print '\tFAILED'
        failures += 1
    # Shared SGE files
    # Local SGE files
    print "\n\tSGE shared directory"
    if createLocalDir(tp.get("troilkatt.globalfs.sge.dir"), CLIENT_DIR_MODE) != True:
        print '\tFAILED'
        failures += 1
    # MapReduce JobServer files
    print "\n\tMapReduce directory"
    if createLocalDir(tp.get("troilkatt.localfs.mapreduce.dir"), SERVER_DIR_MODE) != True:
        print '\tFAILED'
        failures += 1
    
    print "Creating directories on client nodes"
    print "\tLocal directory"
    if createClientDirs(tp.get("troilkatt.localfs.dir"), CLIENT_DIR_MODE, hosts, clientScript) != True:
        print '\tFAILED'
        failures += 1
    # Local SGE files
    print "\n\tSGE directory"
    if createClientDirs(tp.get("troilkatt.localfs.sge.dir"), CLIENT_DIR_MODE, hosts, clientScript) != True:
        print '\tFAILED'
        failures += 1
    # Local MapReduce files
    print "\n\tMapReduce directory"
    if createClientDirs(tp.get("troilkatt.localfs.mapreduce.dir"), CLIENT_DIR_MODE, hosts, clientScript) != True:
        print '\tFAILED'
        failures += 1
    
    print "Create HDFS directory"
    if persistentStorage == "hdfs":
        if createHDFSDir("troilkatt.tfs.root.dir", CLIENT_DIR_MODE) != True:
            print '\tFAILED'
            failures += 1
    elif persistentStorage == "nfs":
        if createLocalDir(tp.get("troilkatt.tfs.root.dir"), CLIENT_DIR_MODE) != True:
            print '\tFAILED'
            failures += 1
    else:
        print "ERROR: invalid persistent storage: " + persistentStorage
        
    print "Verifying binary and script binaries (on this machine only)"
    print "\tBin"
    if verifyLocalDir(tp.get("troilkatt.localfs.binary.dir"), BIN_DIR_MODE) != True:
        print '\tFAILED'
        failures += 1
    print "\tUtils"
    if verifyLocalDir(tp.get("troilkatt.localfs.utils.dir"), BIN_DIR_MODE) != True:
        print '\tFAILED'
        failures += 1
    print "\tScripts"
    if verifyLocalDir(tp.get("troilkatt.localfs.scripts.dir"), BIN_DIR_MODE) != True:
        print '\tFAILED'
        failures += 1
    print "\tJar"
    if verifyFile(tp.get("troilkatt.jar"), S_IRUSR & stat.S_IRGRP& stat.S_IROTH) != True:
        print '\tFAILED'
        failures += 1
    print "\tLibjars"
    libJars = tp.get("troilkatt.libjars").split(",")
    allPassed= True
    for l in libJars:
        if verifyFile(l, S_IRUSR & stat.S_IRGRP& stat.S_IROTH) != True:
            allPassed = False
    if allPassed != True:
        print '\tFAILED'
        failures += 1
    print "\tClasspath"
    libJars = tp.get("troilkatt.classpath").split(":")
    allPassed= True
    for l in libJars:
        if verifyFile(l, S_IRUSR & stat.S_IRGRP& stat.S_IROTH) != True:
            allPassed = Fale
    if allPassed != True:
        print '\tFAILED'
        failures += 1
    print "\tContainer"
    if verifyFile(tp.get("troilkatt.container.bin"), BIN_DIR_MODE) != True:        
        print '\tFAILED'
        failures += 1
        
    if persistentStorage == "hdfs":
        print '\nNote! HDFS directory is not checked manually'
        print 'It has path: ' + tp.get("troilkatt.tfs.root.dir")
        print 'And it should have mode = ' + CLIENT_DIR_MODE
    
        
    print '\n\nFAILURES: %d\n' % (failures)
    