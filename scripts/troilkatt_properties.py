"""
Troilkatt default configuration options.
"""
class TroilkattProperties:
    """
    @param filename: configuration filename
    """
    def __init__(self, filename):
        # Name to value mapping 
        self.properties = {}        
            
        from xml.dom import minidom
        
        xmldoc = minidom.parse(filename)
    
        propertyList = xmldoc.getElementsByTagName('property')        
        for p in propertyList:
            # For each element get down the tree until the text node    
            nameText = parseElementText(p, 'name', None)
            self.properties[nameText] = parseElementText(p, 'value', None)
           
        
    """
    @param name: property name
    
    #return: default value for the property.
    """
    def getProperty(self, name):
        return self.get(name)
    
    """
    Another name for getProperty()
    
    @param name: property name
    
    #return: default value for the property.
    """
    def get(self, name):
        try:
            return self.properties[name]
        except KeyError, e:
            print 'Configuration file does not have property: %s' % (name)
            raise e
        except Exception, e2:
            raise e2
        
    
    """
    @return: a dictionary with all properties
    """
    def getAllProperties(self):
        return self.getAll()
    
    """
    Another name for getAllProperties()
    
    @return: a dictionary with all properties
    """
    def getAll(self):
        return self.properties
    
"""
Parse configuration file helper function: get text of a node

@param node: minidom node that contains only one text tag
@param tagName: tag to search for in tree
@param logger: loggin handle, or None if log information should be printed to stdout
  
@return: stripped text
"""
def parseElementText(node, tagName, logger):
    list = node.getElementsByTagName(tagName)
    if len(list) > 1:
        msg = 'More than one field per stage: %s' % (node.toxml())
        if logger == None:
            print msg
        else:
            logger.critical(msg)
        raise 'parse element text failure'
    
    text = list[0].firstChild.data
    text = text.encode()
    return text.strip()