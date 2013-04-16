
package edu.princeton.function.troilkatt.source;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class MongoDBSource extends Source {	
	protected String serverAdr;
	protected String collectionName;
	protected String whereKey;	
	protected Pattern wherePattern;
	protected String selectKey;	
	
	/**
	 * Constructor.
	 * 
	 * For arguments description see the superclass.
	 *
	 * @param arguments [0] MongoDB server address
	 *                  [1] collection
	 *                  [2] query key
	 *                  [3] regular expression used to select entries
	 *                  [4] key of the field to return
	 */
	public MongoDBSource(String name, String arguments, String outputDir,
			String compressionFormat, int storageTime, String localRootDir,
			String nfsStageMetaDir, String nfsStageTmpDir, Pipeline pipeline)
			throws TroilkattPropertiesException, StageInitException {
		super(name, arguments, outputDir, compressionFormat, storageTime,
				localRootDir, nfsStageMetaDir, nfsStageTmpDir, pipeline);
				
		String[] argsParts = splitArgs(this.args);
		if (argsParts.length != 5) {
			logger.error("Invalid arguments: ");
			for (String p: argsParts) {
				logger.error("\t" + p);
			}
			throw new StageInitException("Invalid number of arguments: expected 5, got " + argsParts.length);
		}
		
		serverAdr = argsParts[0];
		collectionName = argsParts[1];		
		whereKey = argsParts[2];
		
		try {
			// All checks are for lowercase valuess
			wherePattern = Pattern.compile(argsParts[3].toLowerCase());
		} catch (PatternSyntaxException e) {
			logger.fatal("Invalid filter pattern: " + args);
			throw new StageInitException("Invalid filter pattern: " + args);
		}
		
		selectKey = argsParts[4];
	}
	
	/**
	 * Helper function to scan a MongoDB collection using a regular expression. The code executes:
	 * 
	 * SELECT <selectKey>
	 * WHERE  <whereKey> = whereRegExp
	 * 
	 * @param sk selectKey: field to return
	 * @param wk whereKey: field to check
	 * @param wre whereRegExp: regexp to use for the check
	 * @param l optional Logger. null if no logger should be used
	 * return value list with strings for the select field
	 */
	public static ArrayList<String> scanMongoDB(DBCursor cursor, String sk, String wk, Pattern wre, Logger l) {
		ArrayList<String> returnVals = new ArrayList<String>();
		
		// Already checked entries
		HashSet<String> keys = new HashSet<String>();
		
		while(cursor.hasNext()) {
			DBObject entry = cursor.next();
		
			String whereVal = (String) entry.get(wk);			
			if (whereVal == null) {
				if (l != null) {
					l.warn("Ignoring row that does not include where field: " + wk);
				}				
				continue;
			}
			
			// ignore case
			Matcher matcher = wre.matcher(whereVal.toLowerCase());
			if (matcher.find() == false) { // no match		
				continue;
			}
							
			String selectVal = (String)entry.get(sk);
			if (selectVal == null) {
				if (l != null) {
					l.warn("Ignoring row that does not include select field: " + sk);
				}
				continue;
			}

			String keyVal = (String) entry.get("key");
			if (keyVal == null) {
				if (l != null) {
					l.error("Ignoring row that does not include key: " + entry);
				}
				continue;
			}
			
			if (keys.contains(keyVal)) { // already checked this entry
				continue;
			}
			
			returnVals.add(selectVal);
			keys.add(keyVal);
		}
		
		return returnVals;
	}
	
	/**
	 * Retrieve a set of files to be processed by a pipeline. This function is periodically 
	 * called from the main loop.
	 * 
	 * @param metaFiles list of meta filenames that have been downloaded to the meta directory.
	 * Any new meta files are added to tis list
	 * @param logFiles list for storing log filenames.
	 * @param timestamp of Troilkatt iteration.
	 * @return list of output files in NFS.
	 * @throws StageException thrown if stage cannot be executed.
	 */
	protected ArrayList<String> retrieve(ArrayList<String> metaFiles, 
			ArrayList<String> logFiles, long timestamp) throws StageException {
		
		MongoClient mongoClient;
		try {
			mongoClient = new MongoClient(serverAdr);
		} catch (UnknownHostException e1) {
			logger.fatal("Could not connect to MongoDB server: " + e1);
			throw new StageException("Could not connect to MongoDB server: " + e1.getMessage());
		}
		DB db = mongoClient.getDB("troilkatt");
		DBCollection coll = db.getCollection(collectionName);
		
		DBCursor cursor = coll.find();
		// no limit, since the number of entries should be relatively low
		cursor.limit(0); 
		// sort in descending order according to timestamp
		cursor.sort(new BasicDBObject("timestamp", -1));
		
		ArrayList<String> outputFiles = scanMongoDB(cursor, selectKey, whereKey, wherePattern, logger);
		
		cursor.close();
		mongoClient.close();
		
		return outputFiles;
	}
	
}
