package edu.princeton.function.troilkatt.mongodb;

import java.net.UnknownHostException;
import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.pipeline.Stage;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.tools.FilenameUtils;

/**
* Save the list of input files retrieved by this stage. Also output the input files
* without any changes.
*/
public class SaveFilelist extends Stage {
	protected String serverAdr;
	
	/**
	 * The argument specifies the file in where the input file list is written. 
	 *    
	 * @param see description for super-class
	 */
	public SaveFilelist(int stageNum, String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		super(stageNum, name, args, 
				outputDirectory, compressionFormat, storageTime, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		serverAdr = this.args;
	}
	
	/**
	 * The process2 function is overriden since files to be processed are not downloaded, and there are no
	 * meta nor logfiles
	 */
	@Override
	public ArrayList<String> process2(ArrayList<String> inputHDFSFiles, long timestamp) throws StageException {
		logger.debug("Start process2() at " + timestamp);
		
		MongoClient mongoClient;
		try {
			mongoClient = new MongoClient(serverAdr);
		} catch (UnknownHostException e) {
			logger.fatal("Could not connect to MongoDB server: " + e);
			throw new StageException("Could not connect to MongoDB server: " + e.getMessage());
		}
		DB db = mongoClient.getDB( "troilkatt" );
		DBCollection coll = db.getCollection("geoMeta");
		// Note! no check on getCollection return value, since these are not specified 
		// in the documentation
		
		for (String f: inputHDFSFiles) {
			String dsetID = FilenameUtils.getDsetID(f, true);
			
			DBCursor cursor = coll.find(new BasicDBObject("key", dsetID));
			cursor.limit(0);
			
			if (cursor.count() == 0) {
				System.err.println("No MongoDB entry for: " + dsetID);
				System.exit(-1);
			}
			
			// sort in descending order according to timestamp
			cursor.sort(new BasicDBObject("timestamp", -1));
			DBObject newestEntry = cursor.next();	
			
			// extract timestamp from newest entry
			String timestampStr = (String) newestEntry.get("timestamp");
			if (timestampStr == null) {
				throw new StageException("Invalid mongoDB entry: no timestamp field: " + newestEntry);
			}
			long entryTimestamp = Long.valueOf(timestampStr);
			
			BasicDBObject entry = new BasicDBObject("key", dsetID);
			// Put existing fields to new entry
			entry.putAll(newestEntry.toMap());
			entry.append("files:pclFilename", f);
			
			BasicDBObject query = new BasicDBObject("key", dsetID);
			// Add timestamp to make sure correct entry is updated
			query.append("timestamp", entryTimestamp);
			coll.update(query, entry);
			// Note! no check on error value. If something goes wrong an exception seems to be thrown
			// The javadoc does not specify the return value, including how to check for errors 
		}
		
		mongoClient.close();
		
		// And return the input files
		logger.debug("Process2() done at " + timestamp);
		return inputHDFSFiles;
	}
	
	/**
	 * Recover from a crashed iteration. The default recovery just re-processes all files. 
	 * If needed, subclasses should implement stage specific recovery functions. 
	 * 
	 * @param inputFiles list of input files to process.
	 * @param timestamp timestamp added to output files.
	 * @return list of output files
	 * @throws StageException 	 
	 */
	@Override
	public  ArrayList<String> recover(ArrayList<String> inputFiles, long timestamp) throws StageException {
		return process2(inputFiles, timestamp);
	}
}