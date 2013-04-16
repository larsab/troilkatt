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
 * Update GEO MongoDB entries with meta-data calculated for the expression values
 * in a PCL file.  
 */
public class UpdateGEOInfo {
	
	/**
	 * Update GEO meta data by parsing a GEO GDS or GSE soft file.
	 *  
	 * @param argv command line arguments. 
	 *  0: input filename (GSEXXX_family.pcl or GDSXXX.pcl)
	 *  1: MongoDB server ip address
	 *  2: MongoDB server listen port 
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main(String[] argv) throws ParseException, IOException {
		if (argv.length < 3) {
			System.err.println("Usage: java UpdateGEOInfo inputFilename.pcl mongo.server.ip mongo.server.port");
			System.exit(2);
		}
		String inputFilename = argv[0];		
		String serverAdr = argv[1];
		int serverPort = Integer.valueOf(argv[2]);
		
		/*
		 * Do calculation
		 */
		BufferedReader ins = new BufferedReader(new FileReader(inputFilename));
		Pcl2Info converter = new Pcl2Info();
		HashMap<String, String> results = converter.calculate(ins);
		ins.close();
		converter.printInfoFile(inputFilename);
		
		/*
		 * Print calculated results to stdout
		 */
		converter.printInfoFile(inputFilename);
		
		/*
		 * Update MongoDB entry with calculated meta-data
		 */	
		MongoClient mongoClient = new MongoClient(serverAdr, serverPort);
		DB db = mongoClient.getDB( "troilkatt" );
		DBCollection coll = db.getCollection("geoMeta");
		// Note! no check on getCollection return value, since these are not specified 
		// in the documentation
		
		String dsetID = FilenameUtils.getDsetID(inputFilename, true);
		BasicDBObject entry = GeoMetaCollection.getNewestEntry(coll, dsetID);			
		if (entry == null) {
			System.err.println("Could not find MongoDB entry for: " + dsetID);
			System.exit(-1);
		}		
				
		for (String k: results.keySet()) {
			entry.append("calculated:" + k, results.get(k));			
		}
		
		long entryTimestamp = GeoMetaCollection.getTimestamp(entry);
		if (entryTimestamp == -1) {
			System.err.println("Could not find timestamp for MongoDB entry: " + dsetID);
			System.exit(-1);
		}		
						
		GeoMetaCollection.updateEntry(coll, dsetID, entryTimestamp, entry);
		// Note! no check on error value. If something goes wrong an exception seems to be thrown
		// The javadoc does not specify the return value, including how to check for errors 
		
		mongoClient.close(); 
	}
}
