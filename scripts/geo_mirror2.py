import os.path

from ftplib import FTP

from geo_mirror import GeoMirror

"""
Maintain a mirror of all GSD files in geo
"""
class GeoMirror2(GeoMirror):
    """
    arguments: 
     [0]: directory for storing unknown files. 
        
    @param: see description for super-class
    """
    def __init__(self):
        GeoMirror.__init__(self)               
        
        self.ftp = FTP('ftp.ncbi.nih.gov')        
        #self.softDir = '/sdc/troilkatt/input/spell'
        self.softDir = '/pub/geo/DATA/SOFT/GDS'         
                
        self.logger.debug('geo-mirror module initialized')
        
    
    """
    Query geo and retrieve new and updated files.
    
    @param inputObjects: dictionary with objects indexed by a script specific key (can be None).
    
    @return: dictionary with objects that should be saved in Hbase using the given keys (can be None).
    """
    def crawl(self, inputObjects):        
        if 'gselist' in inputObjects:            
            self.gselist = inputObjects['gselist']
        else:
            self.gselist = []
        #print 'DEBUG: Previously mirrored filelist is not downloaded'
        #self.gselist = [] 
        
        self.ftp.login('anonymous', self.adminEmail)
        
        allFiles = self.getAllRemoteFiles()        
        newFiles = self.getNewFiles(allFiles)        
        
        downloadedFiles = []
        for f in newFiles:
            if self.downloadFile(f):
                downloadedFiles.append(f)
            self.unpackAll(self.downloadDir)
            self.move2Output()
            self.deleteCompressed(self.downloadDir)
            self.move2Log()
            self.moveAll(self.downloadDir, self.unknownDir)        
        self.ftp.quit()
        
        self.saveDirsFile(downloadedFiles, 'downloaded')
        
        # Update downloaded subdirs list
        for gse in downloadedFiles:
            if gse not in self.gselist:
                self.gselist.append(gse)            
            
        # outputObjects
        return {'gselist': self.gselist}
    
    """
    Get list of all subdirectories in the SOFT/by_series NCBI Geo FTP server.
    
    @return list with by_series/ subdirectories
    """
    def getAllRemoteFiles(self):        
        # Get file list
        self.logger.info('Retrieve directory list from FTP server (this may take some time)')
        files = self.ftp.nlst(self.softDir)
        #files = os.listdir(self.softDir)
        
        self.saveDirsFile(files, 'all')
        
        return files
    
    """
    Compare the current subdirs with the previously downloaded directories to create a list of 
    files to download
    
    @param downloadedList: subdirectory list retrieved from the FTP server
    
    @return list of subdirectories in downloadedList that are not in self.gselist    
    """
    def getNewFiles(self, downloadedList):
        newList = []
        for s in downloadedList:
            if s not in self.gselist:
                newList.append(s)
                
        self.saveDirsFile(newList, 'new')
        
        return newList
    
    """
    Download all files in a subdirectory. The files are saved in the downloadDirectory
    
    @param filename: GSD file to download
    
    @return true on success, false on failure
    """
    def downloadFile(self, filename):                            
        self.logger.debug('FTP retrieve: %s' % (filename))        
        dstName = os.path.join(self.downloadDir, os.path.basename(filename))
        try:
            self.ftp.retrbinary('RETR %s' % (filename), open(dstName, 'wb').write, 32*1024*1024)
            #shutil.copy(srcName, dstName)
        except Exception, e:
            self.logger.warning("Could not downaload file: " + dstName)
            self.logger.warning(e)
            os.remove(dstName)        
            
        return True
            
"""
Run a troilkatt crawler

Command line arguments: %s <options> download output log <args>, where
    <options>: -i object input directory, and -o object output directory
    download: download directory
    output: output directory
    log: logfile directory
    <args>: optional list of stage specific arguments
        
The decription in parseArgs() has additional details. 
"""
if __name__ == '__main__':
    c = GeoMirror2()
    c.mainRun()
    print 'Done'