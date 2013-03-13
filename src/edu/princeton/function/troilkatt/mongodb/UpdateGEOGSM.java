package edu.princeton.function.troilkatt.mongodb;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.Text;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import edu.princeton.function.troilkatt.tools.GeoGDSParser;

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
	 */
	public static void main(String[] argv) {
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
		try {
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
		} finally {
		   cursor.close();
		}
		
		// Save sampleID -> GDS/GSE mappings
		DBCollection collGSM = db.getCollection("gsm2gid");
		
		for (String gsm: gsm2gids.keySet()) {
			BasicDBObject entry = new BasicDBObject("id", gsm);
			// Store meta values in MongoDB, but also output these to a meta-file (stdout)
			for (String k: parser.singleKeys) {
				String val = parser.getSingleValue(k);
				if (val != null) {
					entry.append(k, val);				
					System.out.println(k + ": " + val);
				}
			}
			for (String k: parser.multiKeys) {
				ArrayList<String> vals = parser.getValues(k);					
				if (vals != null) {
					String valStr = k + ":";				
					for (String v: vals) {
						valStr = valStr + "\t" + v;
					}
					// Newlines are used to seperate entries in mongodb fields (similar to Hbase fields)
					entry.append(k, valStr.replace("\t", "\n"));
					System.out.println(valStr);				
				}
			}
			coll.insert(entry);
	}
		
		
		
		

		// output file with sampleID -> GDS/GSE mappings
	}

mongoClient.close();

}
