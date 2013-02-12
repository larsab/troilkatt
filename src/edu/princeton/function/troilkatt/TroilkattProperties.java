package edu.princeton.function.troilkatt;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.w3c.dom.*;
import org.xml.sax.*;

import javax.xml.parsers.*;

/**
 * Troilkatt propeties data structure. The properties are read from the configuration
 * XML file.
 */
public class TroilkattProperties {
	// Name to value mapping
	protected HashMap<String, String> properties = new HashMap<String, String>();
	// Configuration filename (absolute path)
	protected String filename;
	
	protected String[] validProperties = {
			"troilkatt.admin.email",
			"troilkatt.persistent.storage",
			"troilkatt.localfs.dir",
			"troilkatt.localfs.log.dir",			
			"troilkatt.globalfs.global-meta.dir",
			"troilkatt.global-meta.retain.days",
			"troilkatt.localfs.sge.dir",
			"troilkatt.globalfs.sge.dir",
			"troilkatt.localfs.mapreduce.dir",
			"troilkatt.localfs.binary.dir",
			"troilkatt.localfs.utils.dir",
			"troilkatt.localfs.scripts.dir",
			"troilkatt.tfs.root.dir",					
			"troilkatt.tfs.status.file",
			"troilkatt.update.interval.hours",
			"troilkatt.jar",
			"troilkatt.libjars",
			"troilkatt.container.bin"};
	
	/**
	 * Constructor.
	 * 
     * @param filename configuration filename
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ParserConfigurationException 
	 * @throws TroilkattPropertiesException if the configuration file could not be parsed
     */
	public TroilkattProperties(String filename) throws TroilkattPropertiesException {		
		this.filename = (new File(filename)).getAbsolutePath();
		
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();		
		DocumentBuilder builder;
		try {
			builder = dbf.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			System.err.println("Could not parse Troilkatt configuration file: " + e.toString());
			throw new TroilkattPropertiesException("Could not parse Troilkatt configuration file");
		}		
		Document xmldoc;
		try {
			xmldoc = builder.parse(filename);
		} catch (SAXException e) {			
			System.err.println("Could not parse Troilkatt configuration file: " + e.toString());
			throw new TroilkattPropertiesException("Could not parse Troilkatt configuration file");
		} catch (IOException e) {
			System.err.println("Could not read Troilkatt configuration file: " + e.toString());
			throw new TroilkattPropertiesException("Could not read Troilkatt configuration file");
		}
    
        NodeList propertyList = xmldoc.getElementsByTagName("property");        
        for (int i = 0; i < propertyList.getLength(); i++) {
        	Element p = (Element) propertyList.item(i);
            // For each element get down the tree until the text node    
            String nameText = parseElementText(p, "name");
            properties.put(nameText, parseElementText(p, "value"));
        }
        verifyProperties();
	}   		
                    
    /**
     * Get a property value.
     *
     * @param name property name
     * @return default value for the property.
     * @throws TroilkattPropertiesException 
     */
    public String get(String name) throws TroilkattPropertiesException {
    	String value = properties.get(name);
    	
    	if (value == null) {
    		throw new TroilkattPropertiesException("Configuration file does not have property: " + name);
    	}
    	
    	return value;
    }
    
    /**
     * Set a property
     * 
     * @param name property name
     * @param value new value for property
     */
    public void set(String name, String value) {
    	properties.put(name, value);
    }
    
    /**
     * Alias for getAllProperties()
     *
     * @return a HashMap<String, String> with all properties
     */
    public HashMap<String, String> getAll() {
        return properties;
    }
    
    /**
     * @return this configurations filename
     */
	public String getConfigFile() {
		return filename;
	}
	
	/**
	 * Parse configuration file helper function to get text of a node.
	 *
	 * @param node minidom node that contains only one text tag
	 * @param tagName tag to search for in tree
	 * @return node text
	 * @throws TroilkattPropertiesException 
	 */
	private String parseElementText(Element node, String tagName) throws TroilkattPropertiesException {
		NodeList list = node.getElementsByTagName(tagName);     	
	
		if (list.getLength() > 1) {
			System.err.println("More than one field per stage: " + node.getTextContent());    		
			throw new TroilkattPropertiesException("Invalid element: " + tagName);
		}
	
		return list.item(0).getFirstChild().getTextContent();	
	}

	/**
	 * Verify that all required elements are in the configuration file.
	 * @throws TroilkattPropertiesException  if a property was not found in the file
	 * 
	 */
	private void verifyProperties() throws TroilkattPropertiesException {
		for (String p: properties.keySet()) {
			boolean found = false;
			for (String v: validProperties) {
				if (p.equals(v)) {
					found = true;
					break;
				}
			}
			if (! found) {
				throw new TroilkattPropertiesException("Invalid property in configuration file: " + p);
			}
		}
		
		for (String v: validProperties) {
			boolean found = false;
			for (String p: properties.keySet()) {
				if (p.equals(v)) {
					found = true;
					break;
				}
			}
			if (! found) {
				throw new TroilkattPropertiesException("Property not found in configuration file: " + v);
			}
		}
	}
}
 