package edu.princeton.function.troilkatt.tools;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Superclass for parsing a GEO soft file. The GeoSeriesParser and GeoDatasetParser implements
 * parsers for respectively GSExxx_family.soft and GDSxxx.soft files. 
 * 
 * The class is written such that it can be used from a Java application, MapReduce task,
 * or a JavaScript application. The main difference between these is the type of stream
 * classes they support. This class therefore only provides parsing methods that must be
 * called by another class which also implements line reads and other I/O.
 * 
 * The meta-data fields extracted from a SOFT file are:
 * - DatasetID (used as key)
 * - Title
 * - Date
 * - PubMedID
 * - Organisms
 * - Series description
 * - Platform IDs
 * - Platform names
 * - Platform descriptions
 * - Number of genes (rows) 
 * - Number of samples
 * - Sample IDs
 * - Sample descriptions
 */
public class GeoSoftParser {
	/* Lists of meta field names: first for fields with only one value, and the second for fields
	 * with multiple values. id is not included in the list. */
	public final String[] singleKeys = {"id", "title", "date", "description"};
	public final String[] multiKeys = {"pmid", "organisms", "platformIDs", "platformTitles", "rowCounts", "sampleIDs",
			"sampleTitles", "channelCounts", "valueTypes"};
	
	/* Map soft file tags to meta data fields
	 * The mapping is specified in the subclass setupTags() method. */
	protected HashMap<String, String> metaTags;        // meta tag -> meta ID
	/* Meta data found in file */
	protected HashMap<String, ArrayList<String>> meta; // meta ID -> [meta value 1, meta value 2,...] 
	
	public GeoSoftParser() {
		metaTags = new HashMap<String, String>();
		meta = new HashMap<String, ArrayList<String>>();
	}
	
	/**
	 * Setup soft file meta tags to meta data field mapping.
	 * This function will initialize the meta hashmap. It should be called
	 * from the subclasses constructor.
	 * 
	 * NOTE! All tags should be in lower case
	 * NOTE! The subclass should add a call to checkTags() at the end to verify that the hash map is properly
	 * initialized
	 */
	 protected void setupTags() {
		 throw new RuntimeException("A subclass should implement this method");
	 }
	 
	 /**
	  * Verify that the metaTags map contains and ID, and all fields in the singleKeys and multiKeys lists
	  * 
	  * @return true if all tests pass, false if one or more fails.
	  */
	 protected boolean checkTags() {
		 boolean idFound = false;
		 for (String k: metaTags.keySet()) {
			 if (metaTags.get(k).equals("id")) {
				 idFound = true;
				 break;
			 }
		 }
		 
		 if (! idFound) {
			 System.err.println("ID not found");
			 return false;
		 }
		 
		 for (String n: singleKeys) {
			 boolean nameFound = false;
			 for (String k: metaTags.keySet()) {
				 if (metaTags.get(k).equals(n)) {
					 nameFound = true;
					 break;
				 }
			 }
			 
			 if (!nameFound) {				 
				 System.err.println("Meta field: " + n + " not found");
				 return false;
			 }
		 }
		 
		 for (String n: multiKeys) {
			 boolean nameFound = false;
			 for (String k: metaTags.keySet()) {
				 String metaKey = metaTags.get(k); 
				 if (metaKey.equals(n)) {
					 nameFound = true;
					 break;
				 }
			 }
			 
			 if (!nameFound) {
				 System.err.println("Meta field: " + n + " not found");
				 return false;
			 }
		 }
		 
		 return true; // all found
	 }

	 /**
	  * This function is called by the driver class to parse a line read from
	  * a soft file.
	  * 
	  * @param line: line read from a soft file
	  * @return true if the line contained a valid meta tag. False otherwise
	  */
	public boolean parseLine(String line) {
		String[] lineParts = line.toString().split("=");
		if (lineParts.length < 2) {
			//System.err.println("Ignoring meta line: " + line);
			return false;
		}
			
		// tags should be in lower case
		String lkey = lineParts[0].trim().toLowerCase();
		// values are unchanged
		String lvalue = lineParts[1].trim();
			
		if (metaTags.containsKey(lkey)) {
			String tagKey = metaTags.get(lkey);
			if (! meta.containsKey(tagKey)) {
				meta.put(tagKey, new ArrayList<String>());
			}
			ArrayList<String> valsList = meta.get(tagKey);
			if (! valsList.contains(lvalue)) {
				valsList.add(lvalue);
			}
			return true;
			
		}
		else {
			return false;
		}
	}
	
	/**
	 * Parse a file
	 * 
	 * @param filename file to parse
	 * @return None, but the global singleKeys and multiKeys are initialized
	 * @throws IOException 
	 */
	public void parseFile(String filename) throws IOException {
		BufferedReader ins = new BufferedReader(new FileReader(filename));		
		String line;
		while ((line = ins.readLine()) != null) {
			parseLine(line);
		}			
		ins.close();
	}
	
	/**
	 * Return meta data hash map. Key is the meta data field, and value is a list of meta-data fields 
	 * extracted from the file.
	 */
	public HashMap<String, ArrayList<String>> getMeta() {
		return meta;
	}

	/**
	 * Return a meta-data value for a meta-data field that should only have one meta-data value.
	 * 
	 * @param key meta data field name. The names are specified in setupTags()
	 * @return meta-data field, or null if no meta-data was found for the name.
	 * @throws ParseException if multiple fields were found for the given name.
	 */
	public String getSingleValue(String key) throws ParseException {
		if (isMultiKey(key)) {
			throw new ParseException("Is multi-value tag: " + key);	
		}
		
		ArrayList<String> val = meta.get(key);
		if (val == null) { // field was not found in file
			return null;
		}
		
		if (val.size() > 1) {
			String val0 = val.get(0);
			// Make sure all values are of equal size
			for (int i = 0; i < val.size(); i++) {
				if (! val0.equals(val.get(i))) {
					throw new ParseException("Multiple differing fields found for: " + key);
				}
			}
			return val0;
		}
		else {
			return val.get(0);
		}
	}

	/**
	 * Return a meta-data values for a meta-data field.
	 * 
	 * @param key meta data field name. The names are specified in setupTags()
	 * @return meta-data field, or null if meta-data was not found for the name.
	 */
	public ArrayList<String> getValues(String key) {		
		return meta.get(key);
	}

	/**
	 * Check if key is in the multiKeys array
	 * 
	 * @param tagKey key to check
	 * @return true if is multiKeys[], false otherwise
	 */
	private boolean isMultiKey(String tagKey) {
		for (String k: multiKeys) {
			if (k.equals(tagKey)) {
				return true;
			}
		}
		return false;
	}
}
