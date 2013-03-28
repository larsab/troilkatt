
package edu.princeton.function.troilkatt.source;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

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
			String hdfsStageMetaDir, String hdfsStageTmpDir, Pipeline pipeline)
			throws TroilkattPropertiesException, StageInitException {
		super(name, arguments, outputDir, compressionFormat, storageTime,
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
				
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
			wherePattern = Pattern.compile(argsParts[3]);
		} catch (PatternSyntaxException e) {
			logger.fatal("Invalid filter pattern: " + args);
			throw new StageInitException("Invalid filter pattern: " + args);
		}
		
		selectKey = argsParts[4];
	}
	
	/**
	 * Retrieve a set of files to be processed by a pipeline. This function is periodically 
	 * called from the main loop.
	 * 
	 * @param metaFiles list of meta filenames that have been downloaded to the meta directory.
	 * Any new meta files are added to tis list
	 * @param logFiles list for storing log filenames.
	 * @param timestamp of Troilkatt iteration.
	 * @return list of output files in HDFS.
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
		
		ArrayList<String> outputFiles = new ArrayList<String>();

		DBCursor cursor = coll.find();
		// no limit, since the number of entries should be relatively low
		cursor.limit(0); 
		// sort in descending order according to timestamp
		cursor.sort(new BasicDBObject("timestamp", -1));
		
		// Already checked entries
		HashSet<String> keys = new HashSet<String>();
		
		while(cursor.hasNext()) {
			DBObject entry = cursor.next();
		
			String whereVal = (String) entry.get(whereKey);
			if (whereVal == null) {
				logger.warn("Ignoring row that does not include where field: " + whereKey);
				continue;
			}
			
			Matcher matcher = wherePattern.matcher(whereVal);
			if (matcher.find() == false) { // no match		
				continue;
			}
							
			String selectVal = (String)entry.get(selectKey);
			if (selectVal == null) {
				logger.warn("Ignoring row that does not include select field: " + selectKey);
				continue;
			}

			String keyVal = (String) entry.get("key");
			if (keyVal == null) {
				logger.error("Ignoring row that does not include key: " + entry);
				continue;
			}
			
			if (keys.contains(keyVal)) { // already checked this entry
				continue;
			}
			
			outputFiles.add(selectVal);
			keys.add(keyVal);
		}
		
		cursor.close();
		mongoClient.close();
		
		return outputFiles;
	}
	
}
