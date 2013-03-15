package edu.princeton.function.troilkatt.mongodb;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;


/**
 * Update GEO GSM to GDS/GSE mappings stored in MongoDB
 * 
 * Note! This should be run as a single process and not in parallel
 *
 */
public class UpdateGEOGSM {
		
	/**
	 * Update GEO GSM to GDS/GSE mappings
	 * 
	 * @param argv command line arguments
	 *  0: mongodb hostname
	 * @throws UnknownHostException 
	 */
	public static void main(String[] argv) throws UnknownHostException {
		if (argv.length < 1) {
			System.err.println("Usage: java UpdateGEOGSM mongo.server.address");
			System.exit(2);
		}
	
		MongoClient mongoClient = new MongoClient(argv[0]);
		DB db = mongoClient.getDB( "troilkatt" );
		DBCollection collMeta = db.getCollection("geoMeta");

		// sampleID to GDS/GSE mapping
		HashMap<String, ArrayList<String>> gsm2gids = new HashMap<String, ArrayList<String>>();

		
		// Use a cursor to loop over all meta entries
		DBCursor cursor = collMeta.find();
		
		while(cursor.hasNext()) {
			DBObject entry = cursor.next();

			// Get GDS or GSE ID
			String gid = (String) entry.get("meta:id");
			if (gid == null) {
				throw new RuntimeException("Invalid mongoDB entry: no meta:id field: " + entry);
			}

			// Read in meta:sampleIDs for each GDS and GSE entry
			String gsmString = (String) entry.get("meta:sampleIDs");
			if (gsmString == null) {
				System.err.println("No meta:sampleIDs field for: " + gid);
			}			   
			String[] gsms = gsmString.split("\n");

			// Update sampleID -> GDS/GSE mappings
			for (String gsm: gsms) {
				if (! gsm.startsWith("GSM")) {
					System.err.println("Invalid GSM id: " + gsm + " in row: " + gid);						
				}

				if (! gsm2gids.containsKey(gsm)) {
					ArrayList<String> val = new ArrayList<String>();
					gsm2gids.put(gsm, val);
				}
				ArrayList<String> val = gsm2gids.get(gsm);
				val.add(gid);					
			}
		}
		cursor.close();
		
		
		// Save sampleID -> GDS/GSE mappings
		DBCollection collGSM = db.getCollection("gsm2gid");		
		for (String gsm: gsm2gids.keySet()) {
			BasicDBObject entry = new BasicDBObject("id", gsm);
			ArrayList<String> gids = gsm2gids.get(gsm);
			String val = null;
			for (String g: gids) {
				if (val == null) {
					val = g;
				}
				else {
					val = val + "\n" + g;
				}
			}
			entry.append("in", val);
			collGSM.insert(entry);
		}
		
		mongoClient.close();
	}
			
	

}
