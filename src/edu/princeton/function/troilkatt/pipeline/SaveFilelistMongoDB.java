package edu.princeton.function.troilkatt.pipeline;

import java.net.UnknownHostException;
import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.mongodb.GeoMetaCollection;
import edu.princeton.function.troilkatt.tools.FilenameUtils;

/**
* Save the list of input files retrieved by this stage. Also output the input files
* without any changes.
*/
public class SaveFilelistMongoDB extends Stage {
	protected String serverAdr;
	protected String filenameField;
	
	/**
	 * The argument specifies the file in where the input file list is written. 
	 *  
	 * @param args mongodb.server field.key. Where mongodb.server is the IP address of the machine running the 
	 * MongoDB server, and field.key is the field where the filenames are stored.
	 * @param see description for super-class
	 */
	public SaveFilelistMongoDB(int stageNum, String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		super(stageNum, name, args, 
				outputDirectory, compressionFormat, storageTime, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		String[] argsParts = this.args.split(" ");
		if (argsParts.length != 2) {
			throw new StageInitException("Invalid stage arguments: " + args);
		}
		serverAdr = argsParts[0];
		filenameField = argsParts[1];
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
			
			BasicDBObject entry = GeoMetaCollection.getNewestEntry(coll, dsetID);
			if (entry == null) {
				throw new StageException("Could not find MongoDB entry for: " + dsetID);
			}
			long entryTimestamp = GeoMetaCollection.getTimestamp(coll, entry);
			if (entryTimestamp == -1) {
				throw new StageException("Could not find timestamp for MongoDB entry: " + dsetID);
			}
			
			// Add filename
			entry.append(filenameField, f);
			
			GeoMetaCollection.updateEntry(coll, dsetID, entryTimestamp, entry);			
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