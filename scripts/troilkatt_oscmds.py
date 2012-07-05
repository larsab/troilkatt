"""
Troilkatt script to execute a set of commands specifies in a file. These commands may include
Troilkatt specific symbols that will be substituted before the commands are executed. This 
allows easily writing scripts to download and parse meta-data and gold standard files.

The command file has the following format:
-One command per line
-# specifies comments

The commands will be executed using os.system(), and each individual command must take care
of logging by for example redirecting stdout and stderr to files in TROILKATT.LOG_DIR
"""

import os.path
from troilkatt_script import TroilkattScript

class TroilkattCrawler(TroilkattScript):
    """
    arguments: file with gene-names
        
    @param: see description for super-class
    """
    def __init__(self):
        TroilkattScript.__init__(self)
        
        # List of commands to be executed
        self.cmds = [] 
        
    """
    Helper function to read and parse the commands file
    """
    def readCommads(self, cmdFilename):
        fp = open(cmdFilename)
        if fp == None:
            raise Exception("Could not open file: " + cmdFilename)
        