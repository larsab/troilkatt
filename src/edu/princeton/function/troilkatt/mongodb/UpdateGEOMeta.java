package edu.princeton.function.troilkatt.mongodb;

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
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main(String[] argv) throws ParseException, IOException {
		if (argv.length < 2) {
			System.err.println("Usage: java UpdateGEOMeta inputFilename.soft mongo.server.address");
			System.exit(2);
		}

		GeoGDSParser parser = new GeoGDSParser();
		String inputFilename = argv[0];
		parser.parseFile(inputFilename);		
		
		MongoClient mongoClient = new MongoClient(argv[1]);
		DB db = mongoClient.getDB( "troilkatt" );
		DBCollection coll = db.getCollection("geoMeta");
		// Note! no check on getCollection return value, since these are not specified 
		// in the documentation
		
		String dsetID = FilenameUtils.getDsetID(inputFilename, true);
		BasicDBObject entry = GeoMetaCollection.getNewestEntry(coll, dsetID);
		if (entry == null) {
			System.err.println("No MongoDB entry for: " + dsetID);
			System.exit(-1);
		}
		long entryTimestamp = GeoMetaCollection.getTimestamp(entry);
		if (entryTimestamp == -1) {
			System.err.println("Could not find timestamp for MongoDB entry: " + dsetID);
			System.exit(-1);
		}
		
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
		
		coll.update(new BasicDBObject("key", dsetID), entry);
		GeoMetaCollection.updateEntry(coll, dsetID, entryTimestamp, entry);	
		// Note! no check on error value. If something goes wrong an exception seems to be thrown
		// The javadoc does not specify the return value, including how to check for errors 
		
		mongoClient.close();		
	}
}
