package edu.princeton.function.troilkatt.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * Utility functions for updating GEO meta data stored in MongoDB
 *
 */
public class GeoMetaCollection {
	public static String mongoDB = "troilkatt";
	public static String mongoCollectin = "geoMeta";
	
	/**
	 * Return the newst entry for a dataset
	 * 
	 * @param coll initialized geoMeta collection handle
	 * @param dsetID key used to find entries
	 * @return entry on success or Null if an entry could not be found for the given key
	 */
	public static BasicDBObject getNewestEntry(DBCollection coll, String dsetID) {
		DBCursor cursor = coll.find(new BasicDBObject("key", dsetID));
		cursor.limit(0);
		
		if (cursor.count() == 0) {
			System.err.println("No MongoDB entry for: " + dsetID);
			return null;
		}
		
		// sort in descending order according to timestamp
		cursor.sort(new BasicDBObject("timestamp", -1));
		DBObject newestEntry = cursor.next();	
		
		// extract timestamp from newest entry
		String timestampStr = (String) newestEntry.get("timestamp");
		if (timestampStr == null) {
			System.err.println("Invalid mongoDB entry: no timestamp field: " + newestEntry);
			return null;
		}
		
		BasicDBObject entry = new BasicDBObject("key", dsetID);
		// Put existing fields to new entry
		entry.putAll(newestEntry.toMap());
		return entry;
	}
	
	/**
	 * Return the timestamp of an entry
	 * 
	 * @param coll initialized geoMeta collection handle
	 * @param entry 
	 * @return timestamp on success or -1 if an entry could not be found for the given key
	 */
	public static long getTimestamp(DBCollection coll, DBObject entry) {
		String timestampStr = (String) entry.get("timestamp");
		if (timestampStr == null) {
			System.err.println("Invalid mongoDB entry: no timestamp field: " + entry);
			return -1;
		}
		return Long.valueOf(timestampStr);
	}
	
	/**
	 * Update an entry. This method assumes that all entries can be uniquely identified using the key and timestamp fields.
	 * 
	 * @param coll initialized geoMeta collection handle
	 * @param dsetID key used to find entries
	 * @param timestamp timestamp of entry to update
	 * @param entry entry with new fields to add. Note only the fields in this entry are added. Non-included fields
	 * are lost.
	 * @return none
	 */
	public static void updateEntry(DBCollection coll, String dsetID, long timestamp, BasicDBObject entry) {
		BasicDBObject query = new BasicDBObject("key", dsetID);
		// Add timestamp to make sure correct entry is updated
		query.append("timestamp", timestamp);
		coll.update(query, entry);
		// Note! no check on error value. If something goes wrong an exception seems to be thrown
		// The javadoc does not specify the return value, including how to check for errors 
	}
}