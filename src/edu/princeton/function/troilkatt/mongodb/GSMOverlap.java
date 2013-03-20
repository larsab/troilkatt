package edu.princeton.function.troilkatt.mongodb;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
 
public class GSMOverlap {
	/*
	 *  Caches with meta data already read from the geoMetaTable
	 */
	// key: GSE or GDS id, value: "date\torganism1,organism2,..."
	protected static HashMap<String, String> gid2meta;
	// key: GSE or GDS id, value: number of GSMs for series/dataset
	protected static HashMap<String, Integer> gid2nSamples;
	
	
	/**
	 * Find overlapping samples
	 * 
	 * @param args command line arguments
	 *  0: mongodb hostname
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws UnknownHostException {
		// Read in GSM to GDS/GSE mappings
		if (args.length < 1) {
			System.err.println("Usage: java UpdateGEOGSM mongo.server.address");
			System.exit(2);
		}
		
		// key: gid1\tgid2, where gid1 < gid2
		// value: list of overlapping gsm IDs
		HashMap<String, ArrayList<String>> overlappingSamples = new HashMap<String, ArrayList<String>>();
		// Keep track of timestamps for each entry
		// DEBUG
		HashMap<String, Long> gsm2timestamp = new HashMap<String, Long>();
				
		MongoClient mongoClient = new MongoClient(args[0]);
		DB db = mongoClient.getDB( "troilkatt" );
		DBCollection collGSM = db.getCollection("gsm2gid");	
		
		// Count overlapping samples
		DBCursor cursor = collGSM.find();
		// no limit, since the number of entries should be relatively low
		cursor.limit(0); 
		// sort in descending order according to timestamp
		cursor.sort(new BasicDBObject("timestamp", -1));		
		while(cursor.hasNext()) {
			DBObject entry = cursor.next();

			// Get GSM ID
			String gsm = (String) entry.get("id");
			if (gsm == null) {
				throw new RuntimeException("Invalid mongoDB entry: no meta:id field: " + entry);
			}

			// Make sure only the newest entry is used (the entries should be sorted by timestamp)
			long entryTimestamp = GeoMetaCollection.getTimestamp(entry);
			if (entryTimestamp == -1) {
				throw new RuntimeException("Could not find timestamp for MongoDB entry: " + gsm);
			}
			if (gsm2timestamp.containsKey(gsm)) {
				if (entryTimestamp > gsm2timestamp.get(gsm)) {
					throw new RuntimeException("Invalid mongoDB sort");
				}
				else {
					continue;
				}
			}
			else {
				gsm2timestamp.put(gsm, entryTimestamp);
			}

			// Get overlapping GSE IDs
			String inStr = (String) entry.get("in");
			String[] gids = inStr.split("\n");

			// Find pairwise overlapping samples
			Arrays.sort(gids);
			for (int i = 0; i < gids.length; i++) {
				for (int j = i + 1; j < gids.length; j++) {
					String key = gids[i] + "\t" + gids[j];
					ArrayList<String> gsms = overlappingSamples.get(key);
					if (gsms == null) { // key not found
						gsms = new ArrayList<String>();				
						overlappingSamples.put(key, gsms);
					}
					if (! gsms.contains(gsm)) {
						gsms.add(gsm);
					}
				}
			}
		}		
		cursor.close();		

		// Write output file
		DBCollection collMeta = db.getCollection("geoMeta");
		for (String gidPair: overlappingSamples.keySet()) {
			String parts[] = gidPair.split("\t");
			assert(parts.length == 2);
			String gid1 = parts[0];
			String gid2 = parts[1];
			ArrayList<String> gsms = overlappingSamples.get(gidPair);
			String meta1 = getMeta(collMeta, gid1);
			assert(meta1 != null);
			String meta2 = getMeta(collMeta, gid2);
			assert(meta2 != null);
			
			// Build output value
			StringBuilder sb = new StringBuilder();
			sb.append(gidPair);
			sb.append("\t");
			sb.append(gsms.size());
			sb.append(",");
			sb.append(gid2nSamples.get(gid1));
			sb.append(",");
			sb.append(gid2nSamples.get(gid2));
			sb.append("\t");
			sb.append(gsms.get(0));
			for (int i = 1; i < gsms.size(); i++) {
				sb.append(",");
				sb.append(gsms.get(i));
			}
			sb.append("\t");
			sb.append(meta1);
			sb.append("\t");
			sb.append(meta2);				
			String outputValue = sb.toString();
			
			System.out.println(outputValue);
		} 
	}

	/**
	 * Get meta data string for a series/dataset, and update gid2meta and gid2nSamples
	 * 
	 * @param GSE or GDS id
	 * @return meta-data string, or null if the meta-data could not be read from MongoDB
	 */
	private static String getMeta(DBCollection collMeta, String gid) {
		if (gid2meta.containsKey(gid)) { // Already loaded
			return gid2meta.get(gid); 
		}
		
		BasicDBObject query = new BasicDBObject("id", gid);
		DBCursor cursor = collMeta.find(query);
		if (cursor.size() != 1) {
			System.err.println("None or multiple entries for: " + gid + "(" + cursor.size() + ")");
			return null;
		}
		DBObject result = cursor.next();		       
		cursor.close();
		
		String orgString = (String)result.get("meta:organisms");
		if (orgString == null) {
			System.err.println("Null value for meta:organisms column in row: " + gid);
			return null;
		}
		String[] orgs = orgString.split("\n");
		
		String date = (String)result.get("meta:date");
		if (date == null) {
			System.err.println("Could not read date from meta data");
			return null;
		}
		String meta = date + "\t" + orgs[0];
		for (int i = 1; i < orgs.length; i++) {
			meta = meta + "," + orgs[i]; 
		}			
		
		String samplesString = (String)result.get("meta:sampleIDs");
		if (samplesString == null) {
			System.err.println("Null value for meta:sampleIDs column in row: " + gid);
			return null;
		}
		
		String[] sampleIDs = samplesString.split("\n");
		int nSamples = sampleIDs.length;
		if (nSamples < 1) {
			System.err.println("No samples for row: " + gid);
			return null;
		}
		
		gid2meta.put(gid, meta);
		gid2nSamples.put(gid, sampleIDs.length);
		return meta;		
	}
}
