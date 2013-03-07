package edu.princeton.function.troilkatt.sge;

import java.io.IOException;
import java.util.ArrayList;

import edu.princeton.function.troilkatt.tools.GeoGDSParser;
import edu.princeton.function.troilkatt.tools.ParseException;

/**
 * Update GEO meta data
 *
 */
public class UpdateGEOMeta {
	
	/**
	 * Update GEO meta data by parsing a GEO GDS or GSE soft file
	 *  
	 * @param argv command line arguments. 
	 *  0: input filename (GSEXXX_family.soft or GDSXXX.soft)		 
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main(String[] argv) throws IOException, ParseException {

		if (argv.length < 1) {
			System.err.println("Usage: java UpdateGEOMeta inputFilename.soft");
			System.exit(2);
		}

		GeoGDSParser parser = new GeoGDSParser();
		parser.parseFile(argv[0]);		

		// TODO: Instead of printing values, store these in MongoDB
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
