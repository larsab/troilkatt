import os, shutil
from troilkatt_script import TroilkattScript

"""
Parent class for all troilkatt scripts. It provides functions for parsing arguments
 and the troilkatt configuration file. In addition it has a run function that runs
 the process() scripts.
"""
class StartPSpell(TroilkattScript):
    def __init__(self):
        TroilkattScript.__init__(self)    
                
    """
    Script specific run function. A sub-class should implement this function.
    
    @param inputFiles: list of absolute filenames in the input directory
    @param inputObjects: dictionary with objects indexed by a script specific key (can be None).
    
    @return: dictionary with objects that should be saved in Hbase using the given keys (can be None).
    """
    def run(self, inputFiles, inputObjects):
        outputObjects = None
        
        myDir = '/nhome/larsab/wenli'
        if not os.path.isdir(myDir):
            os.mkdir(myDir)
            
        for f in inputFiles:
            shutil.copy(f, 
                        os.path.join(myDir, os.path.basename(f)))                
        
        return outputObjects
     
    
"""
Run a troilkatt script

Command line arguments: %s input output log <args>, where
    input: input directory
    output: output directory
    log: logfile directory
    <args>: optional list of stage specific arguments
        
The decription in usage() has additional details. 
"""
if __name__ == '__main__':
    s = StartPSpell()
    s.mainRun()
