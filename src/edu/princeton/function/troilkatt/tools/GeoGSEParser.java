package edu.princeton.function.troilkatt.tools;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class GeoGSEParser extends GeoSoftParser {

	/**
	 * Constructor.
	 */
	public GeoGSEParser() {
		super();
		setupTags();
		checkTags();
	}

	/**
	 * Setup soft file meta tags to meta data field mapping.
	 * This function will initialize the meta hashmap. It should be called
	 * from the subclasses constructor.
	 * 
	 * NOTE! All tags should be in lower case
	 */
	protected void setupTags() {		
		// Series information: one per file
		metaTags.put("!series_geo_accession", "id");
		metaTags.put("!series_title", "title");
		metaTags.put("!series_last_update_date", "date");
		metaTags.put("!series_pubmed_id", "pmid");
		metaTags.put("!series_summary", "description");

		// Platform information: possibly multiple per file
		metaTags.put("!platform_organism", "organisms");
		metaTags.put("!platform_geo_accession", "platformIDs");
		metaTags.put("!platform_title", "platformTitles");
		metaTags.put("!platform_data_row_count", "rowCounts");

		// Sample information: possibly multiple per file
		metaTags.put("!sample_geo_accession", "sampleIDs");
		metaTags.put("!sample_title", "sampleTitles");				
		metaTags.put("!sample_channel_count", "channelCounts");
		// Value types are not supported for series, but a mapping must still be added
		metaTags.put("!__invalid for series", "valueTypes");

		if (! checkTags()) {
			throw new RuntimeException("Tags not initialized properly");
		}
	}

	/**
	 * Return a meta-data value for a meta-data field that should only have one meta-data value.
	 * 
	 * Special cases for GSE SOFT files
	 * 
	 * @param key meta data field name. The names are specified in setupTags()
	 * @return meta-data field, or null if no meta-data was found for the name.
	 * @throws ParseException if multiple fields were found for the given name.
	 */
	@Override
	public String getSingleValue(String key) throws ParseException {
		// Description may be split over multiple fields
		if (key.equals("description")) {
			ArrayList<String> vals = getValues(key);
			if (vals == null) { // No values found
				return null;
			}
			String val = null;
			for (String v: vals) {
				if (val == null) {
					val = v;
				}
				else {
					val = val + " " + v;
				}
			}
			return val;
		}

		return super.getSingleValue(key);
	}

	/**
	 * Parse a GEO GDS soft file
	 *  
	 * @param argv command line arguments. 
	 *  0: input filename (GSEXXX_family.soft)		 
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main(String[] argv) throws IOException, ParseException {

		if (argv.length < 1) {
			System.err.println("Usage: java GeoGSEParser inputFilename.soft");
			System.exit(2);
		}

		BufferedReader ins = new BufferedReader(new FileReader(argv[0]));
		GeoGSEParser parser = new GeoGSEParser();
		String line;
		while ((line = ins.readLine()) != null) {
			parser.parseLine(line);
		}			
		ins.close();

		for (String k: parser.singleKeys) {
			String val = parser.getSingleValue(k);
			if (val != null) {
				System.out.println(k + ": " + val);
			}
		}
		for (String k: parser.multiKeys) {
			ArrayList<String> vals = parser.getValues(k);					
			if (vals != null) {
				System.out.println(k + ":");
				for (String v: vals) {
					System.out.println("\t" + v);
				}
			}
		}
	}
}
