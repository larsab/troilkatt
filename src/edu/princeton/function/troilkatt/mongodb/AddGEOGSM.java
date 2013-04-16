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
 * Add GEO GSM to GDS/GSE mappings to gsm2gid MongoDB collection
 * 
 * Note! This should be run as a single process and not in parallel
 *
 */
public class AddGEOGSM {
		
	/**
	 * Add GEO GSM to GDS/GSE mappings
	 * 
	 * @param argv command line arguments
	 *  0: timestamp to add to the created MongoDB entry
	 *  1: MongoDB server address
	 *  2: MongoDB server listen port
	 * @throws UnknownHostException 
	 */
	public static void main(String[] argv) throws UnknownHostException {
		if (argv.length < 2) {
			System.err.println("Usage: java UpdateGEOGSM timestamp mongo.server.address");
			System.exit(2);
		}
		
		long timestamp = Long.valueOf(argv[0]);
		String serverAdr = argv[1];
		int serverPort = Integer.valueOf(argv[2]);
	
		MongoClient mongoClient = new MongoClient(serverAdr, serverPort);
		DB db = mongoClient.getDB( "troilkatt" );
		DBCollection collMeta = db.getCollection("geoMeta");
		// No check on return value, since the javadoc does not specify what these are

		// sampleID to GDS/GSE mapping
		HashMap<String, ArrayList<String>> gsm2gids = new HashMap<String, ArrayList<String>>();
		// Keep track of timestamps for each entry
		// DEBUG:
		HashMap<String, Long> gid2timestamp = new HashMap<String, Long>();
		
		// Use a cursor to loop over all meta entries
		DBCursor cursor = collMeta.find();
		// no limit, since the number of entries should be relatively low
		cursor.limit(0); 
		// sort in descending order according to timestamp
		cursor.sort(new BasicDBObject("timestamp", -1));
		while (cursor.hasNext()) {
			DBObject entry = cursor.next();

			// Get GDS or GSE ID
			String gid = (String) entry.get("key");
			if (gid == null) {
				throw new RuntimeException("Invalid mongoDB entry: no key field: " + entry);
			}
			
			// Make sure only the newest entry is used (the entries should be sorted by timestamp)
			long entryTimestamp = GeoMetaCollection.getTimestamp(entry);
			if (entryTimestamp == -1) {
				System.err.println("Could not find timestamp for MongoDB entry: " + gid);
				System.exit(-1);
			}			
			if (gid2timestamp.containsKey(gid)) {
				if (entryTimestamp > gid2timestamp.get(gid)) {
					throw new RuntimeException("Invalid mongoDB sort");
				}
				else {
					continue;
				}
			}
			else {
				gid2timestamp.put(gid, entryTimestamp);
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
					continue;
				}

				ArrayList<String> val = gsm2gids.get(gsm);
				if (val == null) { // no entry in hash table
					val = new ArrayList<String>();
					gsm2gids.put(gsm, val);
				}				
				val.add(gid);					
			}
		}
		cursor.close();
				
		// Save sampleID -> GDS/GSE mappings
		DBCollection collGSM = db.getCollection("gsm2gid");		
		for (String gsm: gsm2gids.keySet()) {
			BasicDBObject entry = new BasicDBObject("id", gsm);
			entry.append("timestamp", timestamp);
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
			// No check on return value, since the javadoc does not specify what these are
		}
		
		mongoClient.close();
	}
}
