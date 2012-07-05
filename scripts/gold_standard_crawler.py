import os.path

import urllib

"""
Download and create all gold standards. This is done by executing a make command that
takes care of the downloads.

"""
class GoldStandardCrawler(TroilkattCrawler):
    """
    @param: see description for super-class
    
    arguments:
    """
    def __init__(self):
        TroilkattCrawler.__init__(self)
        
        #
        # List of files to download (filename, URL) tuples
        #
        
        # Human gold standard is based on go terms
        humanFiles = [('gene_ontology_ext.obo', 'http://www.geneontology.org/ontology/obo_format_1_2/gene_ontology_ext.obo'),
                             ('gene_association_human.gz', ' http://cvsweb.geneontology.org/cgi-bin/cvsweb.cgi/go/gene-associations/gene_association.goa_human.gz?rev=HEAD')]
        
        # Arabidopsis on po
        poFiles= []
        
         
        self.organisms =  {'human': {'downloadFiles': humanFiles,
                                     'slimFile': '',
                                     'negativeSlimFile': ''}}
        self.downloadURLs = 
        
        self.logger.debug('Gold standard crawler module initialized')
        
    
    """
    Download gold standards
    
    @param inputObjects: dictionary with objects indexed by a script specific key (can be None).
    
    @return: dictionary with objects that should be saved in Hbase using the given keys (can be None).
    """
    def crawl(self, inputObjects):        
        
        # Download files
        for dl in self.downloadURLs:
            filename, url = dl
            urllib.urlretrieve(url, os.path.join(self.downloadDir, filename))
        
        # Unpack all compressed files
        self.unpackAll(self.downloadDir)
        self.deleteCompressed(self.downloadDir)
        
        # Move all downloaded and unpacked files to the output directory
        files = os.listdir(self.downloadDir)            
        for f in files:
            src = os.path.join(self.downloadDir, f)
            dst = os.path.join(self.outputDir, f)                    
            shutil.move(src, dst)
            

        mkdir positives
        ~/apps/sleipnir/bin/BNFunc -i ~/troilkatt/data/GO_slim_sleipnir.txt -d positives -y gene_ontology.obo -g gene_association_human
        mkdir negatives
        ~/apps/sleipnir/bin/BNFunc -i ~/troilkatt/data/go_terms_negative.txt -d negatives -y gene_ontology.obo -g gene_association_human
        
        ./Answerer -p positives -n negatives -o $huamn_positives.dab
                
        quantFile = open('human_positives.quant')
        quantFile.write('0.5\t1.5\n')
        quantFile.close()
        # outputObjects
        return None
    
            
"""
Run the gold standard crawler

Command line arguments: %s <options> download output log <args>, where the required arguments are:
    1. -i <object input directory>
    2. -o <object output directory>
    3. download directory
    4. output directory
    5. logfile directory
    6. unknown file directory
        
The decription in parseArgs() has additional details. 
"""
if __name__ == '__main__':
    c = GoldStandardCrawler()
    c.mainRun()
    print 'Done'