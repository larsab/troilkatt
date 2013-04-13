package edu.princeton.function.troilkatt.tools;

import java.io.IOException;
import java.util.ArrayList;

public class GeoGDSParser extends GeoSoftParser {

	/**
	 * Constructor.
	 */
	public GeoGDSParser() {
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
		// Dataset information: one per file
		metaTags.put("^dataset", "id");
		metaTags.put("!dataset_title", "title");
		metaTags.put("!dataset_update_date", "date");
		metaTags.put("!dataset_pubmed_id", "pmid");
		metaTags.put("!dataset_description", "description");
		metaTags.put("!dataset_sample_count", "sampleCount");

		// Platform information: possibly multiple per file
		metaTags.put("!dataset_platform_organism", "organisms");
		metaTags.put("!dataset_sample_organism", "organisms");
		metaTags.put("!dataset_platform", "platformIDs");			
		metaTags.put("!dataset_platform_technology_type", "platformTitles");
		metaTags.put("!dataset_feature_count", "rowCounts");

		// Sample information: possibly multiple per file
		metaTags.put("!subset_sample_id", "sampleIDs");
		metaTags.put("!subset_description", "sampleTitles");		 
		metaTags.put("!dataset_channel_count", "channelCounts");
		metaTags.put("!dataset_value_type", "valueTypes");

		if (! checkTags()) {
			throw new RuntimeException("Tags not initialized properly");
		}
	}

	/**
	 * Return a meta-data values for a meta-data field.
	 * 
	 * Special cases for GDS SOFT files.
	 * 
	 * @param key meta data field name. The names are specified in setupTags()
	 * @return meta-data field, or null if meta-data was not found for the name.
	 */
	@Override
	public ArrayList<String> getValues(String key) {
		ArrayList<String> vals = meta.get(key); 

		// May be multiple sampleIDs per key separated by commas
		if (key.equals("sampleIDs")) {
			ArrayList<String> newVals = new ArrayList<String>();
			for (String v: vals) {
				String[] parts = v.split(",");
				for (String p: parts) {
					// Duplicates may occur
					String pt = p.trim();
					if (newVals.contains(pt) == false) {
						newVals.add(pt);
					}
				}
			}
			return newVals;
		}

		return vals;
	}	
	
	/**
	 * Parse a GEO GDS soft file
	 *  
	 * @param argv command line arguments. 
	 *  0: input filename (GSEXXX_family.soft or GDSXXX.soft)		 
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main(String[] argv) throws IOException, ParseException {

		if (argv.length < 1) {
			System.err.println("Usage: java GeoGDSPArser inputFilename.soft");
			System.exit(2);
		}

		GeoGDSParser parser = new GeoGDSParser();
		parser.parseFile(argv[0]);		

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
