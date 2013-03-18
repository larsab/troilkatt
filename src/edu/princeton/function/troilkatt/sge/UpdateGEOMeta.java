package edu.princeton.function.troilkatt.sge;

import java.io.IOException;
import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import edu.princeton.function.troilkatt.tools.FilenameUtils;
import edu.princeton.function.troilkatt.tools.GeoGDSParser;
import edu.princeton.function.troilkatt.tools.ParseException;

/**
 * Add timestamped GEO meta entries to MongoDB collection
 *
 */
public class UpdateGEOMeta {
	
	/**
	 * Update GEO meta data by parsing a GEO GDS or GSE soft file.
	 *  
	 * @param argv command line arguments. 
	 *  0: input filename (GSEXXX_family.soft or GDSXXX.soft)
	 *  1: MongoDB server hostname 
	 *  2: timestamp to add to MongoDB entries
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main(String[] argv) throws ParseException, IOException {
		if (argv.length < 3) {
			System.err.println("Usage: java UpdateGEOMeta inputFilename.soft timestamp mongo.server.address");
			System.exit(2);
		}

		GeoGDSParser parser = new GeoGDSParser();
		String inputFilename = argv[0];
		parser.parseFile(inputFilename);
		
		long timestamp = Long.valueOf(argv[1]); 
		
		MongoClient mongoClient = new MongoClient(argv[2]);
		DB db = mongoClient.getDB( "troilkatt" );
		DBCollection coll = db.getCollection("geoMeta");
		// Note! no check on getCollection return value, since these are not specified 
		// in the documentation

		String dsetID = FilenameUtils.getDsetID(inputFilename, true);
		BasicDBObject entry = new BasicDBObject("key", dsetID);
		entry.append("timestamp", timestamp);
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
				String valStr = null;				
				for (String v: vals) {
					if (valStr == null) {
						valStr = v;
					}
					else {
						valStr = valStr + "\t" + v;
					}
				}
				// Newlines are used to seperate entries in mongodb fields (similar to Hbase fields)
				entry.append("meta:" + k, valStr.replace("\t", "\n"));
				System.out.println(k + ":" + valStr);				
			}
		}
		
		coll.insert(entry);
		// Note! no check on error value. If something goes wrong an exception seems to be thrown
		// The javadoc does not specify the return value, including how to check for errors 
		
		mongoClient.close();		
	}
}
