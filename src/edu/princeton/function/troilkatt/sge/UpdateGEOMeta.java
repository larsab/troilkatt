package edu.princeton.function.troilkatt.sge;

import java.io.IOException;
import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import edu.princeton.function.troilkatt.tools.GeoGDSParser;
import edu.princeton.function.troilkatt.tools.ParseException;

/**
 * Update GEO meta data stored in MongoDB
 *
 */
public class UpdateGEOMeta {
	
	/**
	 * Update GEO meta data by parsing a GEO GDS or GSE soft file
	 *  
	 * @param argv command line arguments. 
	 *  0: input filename (GSEXXX_family.soft or GDSXXX.soft)
	 *  1: mongodb server hostname 
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main(String[] argv) throws IOException, ParseException {

		if (argv.length < 2) {
			System.err.println("Usage: java UpdateGEOMeta inputFilename.soft mongo.server.address");
			System.exit(2);
		}

		GeoGDSParser parser = new GeoGDSParser();
		parser.parseFile(argv[0]);		
		
		MongoClient mongoClient = new MongoClient(argv[1]);
		DB db = mongoClient.getDB( "troilkatt" );
		DBCollection coll = db.getCollection("geoMeta");

		BasicDBObject entry = new BasicDBObject("key", parser.getSingleValue("id"));
		// Store meta values in MongoDB, but also output these to a meta-file (stdout)
		for (String k: parser.singleKeys) {
			String val = parser.getSingleValue(k);
			if (val != null) {
				entry.append("meta:" + k, val);				
				System.out.println(k + ": " + val);
			}
		}
		for (String k: parser.multiKeys) {
			ArrayList<String> vals = parser.getValues(k);					
			if (vals != null) {
				String valStr = "meta:" + k + ":";				
				for (String v: vals) {
					valStr = valStr + "\t" + v;
				}
				// Newlines are used to seperate entries in mongodb fields (similar to Hbase fields)
				entry.append(k, valStr.replace("\t", "\n"));
				System.out.println(valStr);				
			}
		}
		coll.insert(entry);
		
		mongoClient.close();
	}
}
