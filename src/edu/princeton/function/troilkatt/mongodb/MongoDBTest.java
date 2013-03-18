package edu.princeton.function.troilkatt.mongodb;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/**
 * Make sure we can connect to the MongoDB server, write to it, and read data from it.
 *
 */
public class MongoDBTest {
	/**
	 * Update GEO GSM to GDS/GSE mappings
	 * 
	 * @param args command line arguments
	 *  0: mongodb hostname
	 * @throws UnknownHostException 
	 */
	public static void main(String[] argv) throws UnknownHostException {
		if (argv.length < 1) {
			System.err.println("Usage: java UpdateGEOGSM mongo.server.address [read|write|drop]");
			System.exit(2);
		}
		boolean doRead = true;
		boolean doWrite = true;
		boolean doDrop = false;
		
		if (argv.length == 2) {
			if (argv[1].equals("read")) {
				doWrite = false;
			}
			else if (argv[1].equals("write")) {
				doRead = false;
			}
			else if (argv[1].equals("drop")) {
				doDrop = true;
			}
			else {
				System.err.println("Invalid argument, must be [read | write | drop] and not: " + argv[1]);
				System.exit(2);
			}
		}

		MongoClient mongoClient = new MongoClient(argv[0]);
		DB db = mongoClient.getDB("troilkatt");
		DBCollection coll = db.getCollection("test");
		
		if (doDrop) {
			System.out.println("Do drop\n");
			coll.drop();
			System.out.println("Done\n");
			System.exit(0);
		}

		if (doWrite) {
			System.out.println("Adding data\n");
			
			// Save some data
			BasicDBObject entry1 = new BasicDBObject("id", "foo");
			entry1.append("CAPS", "FOO");
			entry1.append("seqNumber", 1);
			entry1.append("type", "basic");
			coll.insert(entry1);
			BasicDBObject entry2 = new BasicDBObject("id", "bar");
			entry2.append("CAPS", "BAR");
			entry2.append("seqNumber", 2);
			entry2.append("type", "basic");
			coll.insert(entry2);
			BasicDBObject entry3 = new BasicDBObject("id", "baz");
			entry3.append("CAPS", "BAZ");
			entry3.append("seqNumber", 2);
			entry3.append("type", "special");
			coll.insert(entry3);
			
			// Update entry				
			BasicDBObject entry3u = new BasicDBObject("id", "baz");						
			// Note! Update all fields on "old" entry must be added to new entry
			DBCursor cursor = coll.find(new BasicDBObject("id", "baz"));
			DBObject firstEntry = cursor.next();
			entry3u.putAll(firstEntry.toMap());			
			BasicDBObject query = new BasicDBObject("id", "baz");	
			entry3u.append("seqNumber", 3);
			coll.update(query, entry3u);
			
			// Overwrite entry
			BasicDBObject entry2o = new BasicDBObject("id", "bar");
			entry2o.append("CAPS", "BAR");
			entry2o.append("seqNumber", 2);
			entry2o.append("type", "basic");
			coll.insert(entry2o);
		}

		if (doRead) {		
			// Use a cursor to loop over all entries
			DBCursor cursor = coll.find();

			System.out.println("Number of entries in collection: " + cursor.count());
			System.out.println("Collection size: " + cursor.size());

			System.out.println("\nLooping:\n");
			while(cursor.hasNext()) {			
				DBObject entry = cursor.next();
				printEntry(entry);
				System.out.println(entry);
			}
			cursor.close();
			
			System.out.println("\nQuery for id=foo:\n");
			BasicDBObject query = new BasicDBObject("id", "foo");
			cursor = coll.find(query);
			assert(cursor.size() == 1);
			printEntry(cursor.next());
			
			System.out.println("\nQuery for type=basic:\n");
			query = new BasicDBObject("type", "basic");
			cursor = coll.find(query);
			assert(cursor.size() == 2);
			printEntry(cursor.next());
			printEntry(cursor.next());		
			cursor.close();
		} 

		mongoClient.close();
		
		System.out.println("\nDone\n");
	}
	
	private static void printEntry(DBObject entry) {
		String id = (String) entry.get("id");
		assert(id != null);
		String caps = (String) entry.get("CAPS");
		assert(caps != null);
		Object seqNumberObject = entry.get("seqNumber");
		assert(seqNumberObject != null);
		int seqNumber = (Integer) seqNumberObject;
		
		System.out.printf("[%d] %s (%s)\n", seqNumber, id, caps);
	}
}
