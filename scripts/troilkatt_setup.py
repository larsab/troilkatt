"""
Setup a cluster for troilkatt.

Run this script from the node where the main troilkatt server is to be run.

Command line arguments: troilkatt_script.py configFile, where
    configFile: configuration file to use to setup the cluster 
"""

import os, os.path
import stat

# User: rwx, group: ---, other: --- 
SERVER_DIR_MODE =  S_IRUSR & stat.S_IWUSR & stat.S_IXUSR

# User: rwx, group: rwx, other: rwx
CLIENT_DIR_MODE =  S_IRUSR & stat.S_IWUSR & stat.S_IXUSR & stat.S_IRGRP & stat.S_IWGRP & stat.S_IXGRP & stat.S_IROTH &  stat.S_IWOTH & stat.S_IXOTH

# User: rwx, group: r-x, other: r-x
BIN_DIR_MODE =  S_IRUSR & stat.S_IWUSR & stat.S_IXUSR & stat.S_IRGRP & stat.S_IXGRP & stat.S_IROTH &  stat.S_IXOTH

def getModeStr(mode):
    """
    Convert file permissions to a string similar to "ls -l" output.
    """    
    rv = None
    if mode & stat.S_IFDIR:
        rv = "d"
    else:
        rv = "-"
        
    # Owner
    if mode & stat.S_IRUSR:
        rv = rv + "r"
    else:
        rv = rv + "-"    
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
        
    rv = rv + "(" + oct(mode) + ")"
     
    return rv
    
def createLocalDir(dir, mode):
    """
    Create a directory with the given permission mode. Return True on success, False otherwise.
    """
    if os.path.isdir(dir):
        print "\tDirectory already exists: %s" % (dir)
        verifyLocalDir(dir, mode)
    else:
        os.mkdir(dir, mode)
        print "\tDirectory created: %s" % (dir)
        print "\tMode: %s" % (mode)
        
        
def verifyLocalDir(dir, mode):
    """
    Make sure that the directory has the given mode. Return True or False.
    """
    
    dirMode = 
    # Mask off non-permission bits
    
    if mode & dirMode == mode:
        return True
    else:
        return False
    
def verifyLocalFile(dir, mode):
    """
    Make sure that the directory has the given mode. Return True or False.
    """
        
if __name__ == '__main__':
    import sys
    
    assert(len(sys.argv) == 2), "Usage %s configFile" % (sys.argv[0])
    
    configFile = sys.argv[1]
    print "Configuration file is: %s" % (configFile)
    
    from troilkatt_properties import TroilkattProperties
    tp = TroilkattProperties(configFile)
    
    print "Admin email is: %s" % (tp.get("troilkatt.admin.email"))
    persistentStorage = tp.get("troilkatt.persistent.storage")
    print "Using %s for persistent storage" % (persistentStorage)
    print "Global-meta data versions retained for %s days (-1 = forever)" % (tp.get("troilkatt.global-meta.retain.days"))
    print "Persistent status file: %s" % (tp.get("troilkatt.tfs.status.file"))
    print "Update interval: %s hours" % (tp.get("troilkatt.update.interval.hours"))
    print "SGE slots per node: %s" % (tp.get("troilkatt.sge.slots.per.node"))
    print "MongoDB server: %s:%s" % (tp.get("troilkatt.mongodb.server.host"), tp.get("troilkatt.mongodb.server.port"))
    
    
    print "Creating local directories"
    # Client directories are created below
    print "\tTroilkatt server local directory"
    createLocalDir(tp.get("troilkatt.localfs.dir"), SERVER_DIR_MODE)
    # Only the server access this directory
    print "\n\tTroilkatt server log directory"
    createLocalDir(tp.get("troilkatt.localfs.log.dir"), SERVER_DIR_MODE)
    # This directory should be globally accessible
    print "\n\tGlobal-meta directory"
    createLocalDir(tp.get("troilkatt.globalfs.global-meta.dir"), CLIENT_DIR_MODE)
    # Shared SGE files
    # Local SGE files
    print "\n\tSGE shared directory"
    createLocalDir(tp.get("troilkatt.globalfs.sge.dir"), CLIENT_DIR_MODE)
    # MapReduce JobServer files
    print "\n\tMapReduce directory"
    createLocalDir(tp.get("troilkatt.localfs.mapreduce.dir"), SERVER_DIR_MODE)
    
    
    print "Creating directories on client nodes"
    print "\tLocal directory"
    createLocalDir(tp.get("troilkatt.localfs.dir"), CLIENT_DIR_MODE)
    # Local SGE files
    print "\n\tSGE directory"
    createLocalDir(tp.get("troilkatt.localfs.sge.dir"), CLIENT_DIR_MODE)
    # Local MapReduce files
    print "\n\tMapReduce directory"
    createLocalDir(tp.get("troilkatt.localfs.mapreduce.dir"), CLIENT_DIR_MODE)
    
    print "Create HDFS files"
    if persistentStorage == "hdfs":
        createHDFSDir("troilkatt.tfs.root.dir", CLIENT_DIR_MODE)
    elif persistentStorage == "nfs":
        createLocalDir(tp.get("troilkatt.tfs.root.dir"), CLIENT_DIR_MODE)
    else:
        print "ERROR: invalid persistent storage: " + persistentStorage
        
    print "Verifying binary and script binaries"
    print "\tBin"
    verifyLocalDir(tp.get("troilkatt.localfs.binary.dir"), BIN_DIR_MODE)
    print "\tUtils"
    verifyLocalDir(tp.get("troilkatt.localfs.utils.dir"), BIN_DIR_MODE)
    print "\tScripts"
    verifyLocalDir(tp.get("troilkatt.localfs.scripts.dir"), BIN_DIR_MODE)
    print "\tJar"
    verifyFile(tp.get("troilkatt.jar"), S_IRUSR & stat.S_IRGRP& stat.S_IROTH)
    print "\tLibjars"
    libJars = tp.get("troilkatt.libjars").split(",")
    for l in libJars:
        verifyFile(l, S_IRUSR & stat.S_IRGRP& stat.S_IROTH)
    print "\tClasspath"
    libJars = tp.get("troilkatt.classpath").split(":")
    for l in libJars:
        verifyFile(l, S_IRUSR & stat.S_IRGRP& stat.S_IROTH)
    print "\tContainer"
    verifyFile(tp.get("troilkatt.container.bin"), BIN_DIR_MODE)
    