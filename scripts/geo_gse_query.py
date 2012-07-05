"""
Query Geo to recieve a list of new GSE identificators
"""
import os, shutil
import time

from Bio import Entrez

from troilkatt_crawler import TroilkattCrawler

class GeoGseQuery(TroilkattCrawler):
    """
    arguments: [0]: Output filename
               [1]: Entrez query string    
        
    @param: see description for super-class
    """
    def __init__(self):
        TroilkattCrawler.__init__(self)
        
        # E-mail address is often used as password or field in queries against NIH servers
        self.adminEmail = 'lbongo@princeton.edu' 
                               
        # The eutil arguments used to query geo for records to check
        print self.args
        argsParts = self.args.split()
        self.outputFilename = os.path.join(self.downloadDir, argsParts[0])
        self.eSearch = self.parseQuery(argsParts[1])
        
        # These are set in crawl()
        self.allGse = None
        self.lastCrawlTs = None
                
        # The ges-filename which is always stored in the output directory
        self.gseFilename = os.path.join(self.outputDir, 'gesfile')
        
        # Make sure the download directory exists and has the proper sub-directories        
        if not os.path.isdir(self.downloadDir):
            self.logger.info('Output directory %s does not exist, creating new' % (self.downloadDir))
            os.mkdir(self.downloadDir)
                
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
        if 'all.gse' in inputObjects:
            self.allGse = inputObjects['all.gse']
        else: # Not in Hbase (i.e. is first time)
            self.allGse = {}
                
        # Year and month of last geo-crawl
        if 'lastCrawlTS' in inputObjects:
            self.lastCrawlTS = inputObjects['lastCrawlTS']
        else:
            self.lastCrawlTS = None
            
        # Current crawl TS
        crawlTS = time.strftime("%Y/%m", time.localtime())                            
        
        #
        # Do Geo search
        #        
        self.logger.info('Crawl at %s' % (crawlTS))        
        handle = Entrez.esearch(**self.eSearch)        
        
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
        gses = self.xml2DocSumDict(xmlStr)        
        self.saveGseFile(os.path.join(self.logDir, 'result.gse'), gses)        
        
        #
        # Download new and updated records, and copy these to the current data set directory
        #
        newGses = self.getNewGses(gses)
        self.saveGseFile(self.outputFilename, newGses)
            
        # outputObjects
        return {'all.gse': self.allGse, 'lastCrawlTS': crawlTS}
    
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
    Create a logifle with all GSE's downaloaded during this crawl.
    
    @param name (not including the .gse extension automatically added)
    @param gses: list of GSE's    
    """            
    def saveGseFile(self, filename, gses):        
        self.logger.debug('Save GSE list in file: %s' % (filename))
        print 'SAVE GSE in', filename
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
    Find GSE's not already downloaded by comparing these to the values saved in the last crawl.
    
    @param gseList: list of GSE's in query
      
    @return: (newList, updatedList):
      newList: list of GSE's not already downloaded
      updatedList: list of GSE's already downloaded but for which the meta-data has changed      
    """
    def getNewGses(self, gseList):
        self.logger.debug("Compare retrieved GSE's with GSE's downloaded earlier")
        
        #oldList = self.allGse
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
      
    @return: gses: list of GSE ID's for the items. 
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
            
        gses = []
        for e in entryList:
            # Get ID
            ids = e.getElementsByTagName('Id')
            if len(ids) != 1:
                self.logger.critical("parse eSummary: multiple ID's for entry: %s" % (e.toxml()))
                raise Exception('eSummary XML parse failure')
                          
            id = ids[0].firstChild.data
            
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
        return gses    
    
                
"""
Do a geo query

Command line arguments: %s tmp output log <args>, where
    tmp: tmp directory
    output: output directory
    log: logfile directory
    <args>: optional list of stage specific arguments
        
The decription in usage() has additional details. 
"""
if __name__ == '__main__':
    c = GeoGseQuery()
    c.mainRun()

    