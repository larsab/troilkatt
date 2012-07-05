"""
Geo Crawler.
"""

import os, shutil
import time

from Bio import Entrez

from troilkatt_crawler import TroilkattCrawler

class GeoCrawler(TroilkattCrawler):
    """
    arguments: Entrez query string    
        
    @param: see description for super-class
    """
    def __init__(self):
        TroilkattCrawler.__init__(self)
        
        # E-mail address is often used as password or field in queries against NIH servers
        self.adminEmail = 'lbongo@princeton.edu' 
                               
        # The eutil arguments used to query geo for records to check
        self.eSearch = self.parseQuery(self.args)
        
        # These are set in crawl()
        self.gse2timestamp = None
        self.lastCrawlTs = None
                
        # The ges-filename which is always stored in the output directory
        self.gseFilename = os.path.join(self.outputDir, 'gesfile')
        
        # Make sure the download directory exists and has the proper sub-directories
        self.createDirectories(self.downloadDir)
                
        self.logger.debug('geo-crawler module initialized')
        
    """
    Query geo and retrieve new and updated files.
    
    @param inputObjects: dictionary with objects indexed by a script specific key (can be None).
    
    @return: dictionary with objects that should be saved in Hbase using the given keys (can be None).
    """
    def crawl(self, inputObjects):
        #
        # Parse inputObjects
        #
        
        # id -> directory mapping 
        if 'gse2timestamp' in inputObjects:
            self.gse2timestamp = inputObjects['gse2timestamp']
        else: # Not in Hbase (i.e. is first time)
            self.gse2timestamp = {}
                
        # Year and month of last geo-crawl
        if 'lastCrawlTS' in inputObjects:
            self.lastCrawlTS = inputObjects['lastCrawlTS']
        else:
            self.lastCrawlTS = None
            
        # Current crawl TS
        crawlTS = time.strftime("%Y/%m", time.localtime())        
            
        oldTerm = self.eSearch['term']
        # TODO: No way in Entrex to specify a date as today?
        self.eSearch['term'] = self.eSearch['term'].replace('TODAY', crawlTS)
        if self.lastCrawlTS == None:
            self.eSearch['term'] = self.eSearch['term'].replace('LASTCRAWL', '1900/01')
        else:
            self.eSearch['term'] = self.eSearch['term'].replace('LASTCRAWL', self.lastCrawlTS)        
        
        #
        # Do Geo search
        #        
        self.logger.info('Crawl at %s' % (crawlTS))        
        handle = Entrez.esearch(**self.eSearch)
        
        self.eSearch['term'] = oldTerm
        
        record = Entrez.read(handle)
        self.saveEsearchRecord(record) 
        
        #
        # Retrieve meta data for each entry in the result set
        #        
        self.logger.info('Retrieve summary files for %s records from the geo-database' % (record['Count']))        
        handle2 = Entrez.esummary(db=self.eSearch['db'], email=self.adminEmail,
                                  query_key=record['QueryKey'], WebEnv=record['WebEnv'])
        xmlStr = handle2.read()            
        self.saveEsummary(xmlStr)                
        
        #
        # Save new meta data
        #        
        docSums, gses = self.xml2DocSumDict(xmlStr)        
        self.saveGseLogfile(gses, 'all')        
        
        #
        # Download new and updated records, and copy these to the current data set directory
        #
        newGses = self.getNewGses(gses)
        downloaded = self.rsync(newGses)        
        self.saveGseLogfile(downloaded, 'downloaded')                    
                
        # Log failures
        failures = []
        for ges in gses:
            if ges not in downloaded:
                failures.append(ges)
        if len(failures) >= 1:
            self.logger.warning('%d records could not be downlaoded' % (len(failures)))
        self.saveGseLogfile(failures, 'failures')
                
        # Copy respectively all, new and updated records to three repositories
        # Note that the order of this calls is important since updated/new entries are determined
        # by checking the downloaded records against records in current       
        newFiles = self.moveSoftFiles(downloaded, 'all')
        # Update downloaded Gse list
        for gse in newFiles:
            self.gse2timestamp[gse] = crawlTS
            
        # outputObjects
        return {'gse2timestamp': self.gse2timestamp, 'lastCrawlTS': crawlTS}
    
    """
    Parse arguments string and initialize self.eSearch
    """
    def parseQuery(self, arguments):                
        self.logger.debug('Parsing query provided as argument: %s' % (arguments))
        
        argDict = {}
        
        try:
            arguments = arguments.replace('+', ' ')
            parts = arguments.split('&')
            for p in parts:
                key, value = p.split('=')
                argDict[key] = value
        except Exception, e:
            self.logger.critical('Could not parse eSearch string: %s' % (arguments))
            self.logger.critical('Exception: %s' % (e))
            raise e                
        
        if 'db' not in argDict.keys():
            self.logger.critical('db not specified in eSearch string: %s' % (arguments))
            raise 'Invalid eSearch query'
        if 'term' not in argDict.keys():
            self.logger.warning('term not specified in eSearch string: %s' % (arguments))            
            raise 'Invalid eSearch query'
        
        if 'usehistory' not in argDict.keys():
            argDict['usehistory'] = 'y'
        else:
            self.logger.critical('Assumption in code that usehistory=y')
            self.logger.critical('But is it necessary?')
            raise 'Invalid eSearch query'
            
        argDict['tool'] = 'troilkatt'
        argDict['email'] = self.adminEmail
                        
        return argDict
    
    """
    Helper function for initializeCrawl that creates a directory structure used to save
    data downloaded from geo.
    
    @param name of top-level directory.
    
    @return: none
    """
    def createDirectories(self, toplevel):
        self.logger.debug('Initialize GEO directory: %s' % (toplevel))
        
        if not os.path.isdir(toplevel):
            self.logger.info('Output directory %s does not exist, creating new' % (toplevel))
            os.mkdir(toplevel)
            
        dir = os.path.join(toplevel, 'SOFT')
        if not os.path.isdir(dir):
            self.logger.debug('Sub-directory %s does not exist, creating new' % (dir))
            os.mkdir(dir)
        
        dir = os.path.join(toplevel, 'SOFT/by_series')
        if not os.path.isdir(dir):
            self.logger.debug('Sub-directory %s does not exist, creating new' % (dir))
            os.mkdir(dir)        
            
    """
    Create a logifle with all GSE's downaloaded during this crawl.
    
    @param name (not including the .gse extension automatically added)
    @param gses: list of GSE's    
    """            
    def saveGseLogfile(self, gses, name):
        filename = os.path.join(self.logDir, '%s.gse' % (name))
        self.logger.debug('Save information about %s records in: %s' % (name, filename))
        f = open(filename, 'w')
        for gse in gses:
            f.write('GSE%s\n' % (gse))
        f.close()
        
    """
    Save retreived esearch results in a user (and machine) readable format. This file
    is not used for anything.
    
    @param record: eSearch record given as a dictionary    
        
    @return: none
    """    
    def saveEsearchRecord(self, record):
        import pprint
        
        filename = os.path.join(self.logDir, 'esearch.xml')         
        self.logger.debug('Save eSearch result in: %s' % (filename))
        
        recordStr = pprint.pformat(record)
        f = open(filename, 'w')
        f.write(recordStr)
        f.close()
    
    """
    Save retreived meta-data as raw XML file. This file is not used for anything.  
    
    @param xmlStr: retrieved eSummary in XML-format
    """
    def saveEsummary(self, xmlStr):        
        filename = os.path.join(self.logDir, 'esummary.xml')
        
        self.logger.debug('Save Raw XML file for eSummaries in: %s' % (filename))
        
        f = open(filename, 'w')
        f.write(xmlStr)
        f.close()
        
    """
    Parse filename to retrieve GEO id
    
    @param filename: file to parse
    
    @return: unqiue GEO ID
    """
    def getGeoID(self, filename):
        # Split filename into directories
        parts = filename.split('/')
        # Traverese down file-path until the directory for this experiment is found
        for i in range(len(parts)):
            p = parts[i]
            if p == 'by_series' or p == 'series':
                geoId = parts[i + 1]
                if geoId.find('GSE') != 0:
                    self.logger.critical('ID does not start with GSE: %s for %s' % (geoId, filename))
                    raise Exception('Invalid GSE id for %s' % (filename))
                else:
                    return geoId
                
        raise Exception('Could not find GSE id for %s' % (filename))  
        
    """
    Find GSE's not already downloaded by comparing these to the values saved in the last crawl.
    
    @param gseList: list of GSE's in query
      
    @return: (newList, updatedList):
      newList: list of GSE's not already downloaded
      updatedList: list of GSE's already downloaded but for which the meta-data has changed      
    """
    def getNewGses(self, gseList):
        self.logger.debug("Compare retrieved GSE's with GSE's downloaded earlier")
        
        #oldList = self.gse2timestamp.keys()
        print "WARNING: ignoring previously listed GSE's"
        oldList = []
        
        newList = []       
        
        for gse in gseList:
            if not gse in oldList:
                newList.append(gse)
                
        self.logger.info('Of %d records in query %d are new (%d records in DB)' % (len(gseList), len(newList), len(oldList)))
        return newList
    
    """
    Get a dictionary of {id: docsum} entries in an XML document.
    
    @param xmlStr: eSummary xml document in string format as returned from the server      
      
    @return: (docSum, gses): docSum is the {id: docsum} dictionary, while gses is a list of 
     GSE ID's for the items. 
    """ 
    def xml2DocSumDict(self, xmlStr):
        from xml.dom import minidom
        
        self.logger.debug('Parse eSummary XML string')
        
        # Parse xml data given as a string
        import StringIO
        xmlFd = StringIO.StringIO(xmlStr)        
        xmldoc = minidom.parse(xmlFd)
        
        # One entry per record
        entryList = xmldoc.getElementsByTagName('DocSum')
    
        docSumDict = {}
        gses = []
        for e in entryList:
            # Get ID
            ids = e.getElementsByTagName('Id')
            if len(ids) != 1:
                self.logger.critical("parse eSummary: multiple ID's for entry: %s" % (e.toxml()))
                raise Exception('eSummary XML parse failure')
                          
            id = ids[0].firstChild.data
            
            # Get DocSum entry
            docSumDict[id] = e.toxml()
            
            # GSE entries
            items = e.getElementsByTagName('Item')
            gseFound = 0
            
            for i in items:                
                if i.getAttribute('Name') == u'GSE':                    
                    if len(i.childNodes) == 0:
                        self.logger.warning("Ignoring ID %s: no children in GSE attribute" % (id))
                        continue  
                    elif len(i.childNodes) > 1:
                        self.logger.warning("ID %s has: %d children in GSE attribute" % (id, len(i.childNodes)))
                        self.logger.critical('parse eSummary: multiple child nodes for: %s' % (e.toxml()))                        
                        raise Exception('eSummary XML parse failure: multiple GSE children')
                                            
                    value = i.firstChild.data.encode('utf-8')
                    
                    parts = value.split(';')
                    for gse in parts:
                        if gse not in gses:
                            gses.append(gse)
                    
                    gseFound = 1
                    break
                
            if not gseFound:
                self.logger.warning('Ignoring ID %s: no GSE entries' % (id))
                self.logger.debug('Element: %s' % (e.toxml()))  
                   
        gses.sort() 
        return (docSumDict, gses)
    
    """
    Download SOFT, MINiML, and supplementary files from the NCBI server using the rsync
    protocol.
    
    @param gseList: GSE id's of records to download    
        
    @return: list with GSE id's for which either the SOFT or MINiML, and the records 
     were downloaded
        
    """
    def rsync(self, gseList):
        host = 'rsync://rsync.ncbi.nih.gov'
        
        self.logger.info('Download %d records from: %s' % (len(gseList), host))
        
        #
        # Create .sh file with rsync callas
        #
        cmdFilename = os.path.join(self.logDir, 'rsyncCmd.sh')
        cmdFile = open(cmdFilename, 'w')        
        for id in gseList:
            # SOFT-formated data            
            srcPath = '/pub/geo/DATA/SOFT/by_series/GSE%s' % (id)
            destPath = os.path.join(self.downloadDir, 'SOFT/by_series')                
            cmdFile.write('rsync -av %s:%s %s\n' % (host, srcPath, destPath))                                                           
                                    
        cmdFile.close()
        
        #
        # Do the rsync
        #
        import stat
        os.chmod(cmdFilename, stat.S_IRWXU)
        self.logger.debug('Run rsync script. Output and error files are stored in %s' % (self.logDir))
        #os.system('%s > %s 2> %s' % (cmdFilename, 
        #                             os.path.join(self.logDir, 'rsync.output'),
        #                             os.path.join(self.logDir, 'rsync.error')))
        print '\nWARNING!: rsync not run when debugging\n'
        
        #
        # Unpack all received files
        #
        self.logger.debug('Unpack %d downloaded directories' % (len(gseList)))
        for id in gseList:
            # SOFT-formated data                        
            destPath = os.path.join(self.downloadDir, 'SOFT/by_series/GSE%s' % (id))                            
            #self.unpackAll(destPath)                        
            print '\nWARNING!: unpack not run when debugging\n'
            
        print '\nWARNING!: returning empty filelist when debugging\n'
        return []
        #return gseList        
    
    """
    Move all new and updated files to a specified directory.
    
    @param gseList: list with gse for records to check    
    @param type: "all", "new", or "updated": copy all records, just new records, or just updated 
       records. Note that for updated all files in a directory are copied if one or more of
       the files have been updated. "all" is default.
      
    @return: list with absolute filenames of new files      
    """
    def moveSoftFiles(self, gseList, type="all"):                
        self.logger.debug('Move %s files' % (type))                
        
        # Absolute filenames
        newFiles = []        
        
        for gse in gseList:
            softPostfix = 'SOFT/by_series/GSE%s' % (gse)
            srcPath = os.path.join(self.downloadDir, softPostfix)
            files = os.listdir(srcPath)
            
            for f in files:
                if self.endsWith(f, '.soft'):
                    dst = os.path.join(self.outputDir, f)                    
                    shutil.move(os.path.join(srcPath, f), dst)
                    newFiles.append(dst) 
                                                    
        # Log updates and new GSE entries
        self.saveGseLogfile(gseList, 'new')
        
        return newFiles
                
"""
Run a troilkatt crawler

Command line arguments: %s download output log <args>, where
    download: download directory
    output: output directory
    log: logfile directory
    <args>: optional list of stage specific arguments
        
The decription in usage() has additional details. 
"""
if __name__ == '__main__':
    c = GeoCrawler()
    c.mainRun()

    