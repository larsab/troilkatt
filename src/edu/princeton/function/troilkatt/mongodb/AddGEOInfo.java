package edu.princeton.function.troilkatt.mongodb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import edu.princeton.function.troilkatt.tools.FilenameUtils;
import edu.princeton.function.troilkatt.tools.ParseException;
import edu.princeton.function.troilkatt.tools.Pcl2Info;

/**
 * Calculate info values and save these in a MongoDB collection.
 * 
 * Note that this will create a new entry in MongoDB
 */
public class AddGEOInfo {
	
	/**
	 * Update GEO meta data by parsing a GEO GDS or GSE soft file.
	 *  
	 * @param argv command line arguments. 
	 *  0: input filename (GSEXXX_family.pcl or GDSXXX.pcl)
	 *  1: MongoDB server hostname 
	 *  2: timestamp to add to MongoDB entries
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main(String[] argv) throws ParseException, IOException {
		if (argv.length < 3) {
			System.err.println("Usage: java UpdateGEOMeta inputFilename.pcl timestamp mongo.server.address");
			System.exit(2);
		}
		String inputFilename = argv[0];
		long timestamp = Long.valueOf(argv[1]);
		String serverAdr = argv[2];
		
		/*
		 * Do calculation
		 */
		BufferedReader ins = new BufferedReader(new FileReader(argv[0]));
		Pcl2Info converter = new Pcl2Info();
		HashMap<String, String> results = converter.calculate(ins);
		ins.close();
		converter.printInfoFile(argv[0]);
		
		/*
		 * Print calculated results to stdout
		 */
		converter.printInfoFile(inputFilename);
		
		/*
		 * Save calculated meta-data in a new MongoDB entry
		 */	
		MongoClient mongoClient = new MongoClient(serverAdr);
		DB db = mongoClient.getDB( "troilkatt" );
		DBCollection coll = db.getCollection("geoMeta");
		// Note! no check on getCollection return value, since these are not specified 
		// in the documentation
		
		String dsetID = FilenameUtils.getDsetID(inputFilename, true);
		BasicDBObject entry = new BasicDBObject("key", dsetID);
				
		for (String k: results.keySet()) {
			entry.append("calculated:" + k, results.get(k));			
		}
		entry.append("timestamp", timestamp);
		
		coll.insert(entry);
		// Note! no check on error value. If something goes wrong an exception seems to be thrown
		// The javadoc does not specify the return value, including how to check for errors 
		
		mongoClient.close(); 
	}
}
