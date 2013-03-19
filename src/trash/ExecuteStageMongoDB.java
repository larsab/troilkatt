package edu.princeton.function.troilkatt.sge;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import edu.princeton.function.troilkatt.PipelineException;
import edu.princeton.function.troilkatt.PipelinePlaceholder;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattNFS;
// for some statics functions that check and parse key-entry values
import edu.princeton.function.troilkatt.mapreduce.TroilkattMapReduce;
import edu.princeton.function.troilkatt.pipeline.ExecutePerFileSGE;
import edu.princeton.function.troilkatt.pipeline.Stage;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageFactory;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * Execute a Troilkatt stage in parallel as an SGE job. The stage arguments may contain
 * 'MONGODB.SAVE key=value', 'MONGODB.READ key', 'MONGODB.FIND collection:key1=value1,key2=value2',
 * and 'MONGODB.SORT key=value'
 * entries 
 *
 * Mapper class that gets as input a filename and outputs a filename. For each file
 * it does the following:
 * 1. Find a MongoDB entry using the MONGODB.FIND symbol (if any). Then sort the returned
 *    entries using MONGODB.SORT. Finally, replace the MONGODB.READ with the value read
 *    for the specified key from the first sorted entry that matches the find key/values.
 * 2. Remove MONGODB.SAVE from the arguments 
 * 3. Copy a file to the local filesystem (the files can be tens of gigabytes in size)
 * 4. Execute a command that takes the local file as argument
 * 5. Copy the resulting file to NFS
 * 6. Update the first sorted entry that matches the find key/values, with the MONGODB.SAVE
 *    values. Then update the entry in the collection.
 */
public class ExecuteStageMongoDB extends ExecuteStage {	
	
	
	/**
	 * Constructor
	 * 
	 * @param argsFilename filename of arguments file written by SGEStage
	 * @param taskNumber
	 * @param jobID SGE job ID
	 * 
	 * @throws StageInitException 
	 * @throws PipelineException 
	 */		
	public ExecuteStageMongoDB(String argsFilename, int taskNumber, String jobID) throws StageInitException {
		super(argsFilename, taskNumber, jobID);
	}

	/**
	 * Arguments: [0] sgeArgsFilename
	 *            [1] task number
	 *            [2] jobID
	 *            [3] taskID 
	 *            [4] MongoDB server address
	 */
	public static void main(String[] args) {		
		if (args.length < 4) {
			System.out.println("Usage: sgeArgsFilename taskNumber jobID MongoDB.server");
			System.exit(-2);
		}
				
		String argsFilename = args[0];		
		int taskNumber = Integer.valueOf(args[1]);		
		String jobID = args[2];	
		String serverAdr = args[3];
		
		MongoClient mongoClient = null;
		try {
			mongoClient = new MongoClient(serverAdr);
		} catch (UnknownHostException e2) {
			System.err.println("Could not connect to MongoDB server: " + e2);
			System.exit(-1);
		}
		
		if (taskNumber < 0) {
			System.err.println("Invalid task number: " + taskNumber);
			System.exit(-1);
		}
						
		try {
			ExecuteStageMongoDB o = new ExecuteStageMongoDB(argsFilename, taskNumber, jobID);			
			if (o.inputFiles.size() < taskNumber) {
				System.out.println("Invalid task number: " + taskNumber + ", but only " + o.inputFiles.size() + " input files");
				System.exit(-1);
			}
			String mongoCollection = null;
			HashMap<String, String> mongoFinds = new HashMap<String, String>();
			HashMap<String, String> mongoReads = new HashMap<String, String>();
			HashMap<String, String> mongoSaves = new HashMap<String, String>();
			String mongoSortKey = null;
			String mongoSortValue = null;
			
			if (o.stage.args.contains("MONGODB.FIND")) {
				String[] argsSplit = o.stage.args.split(" ");
				// Parse MongoDB symbols
				for (int i = 0; i < argsSplit.length; i++) {
					if (argsSplit[i].equals("MONGODB.FIND")) {
						String[] findParts = argsSplit[i+1].split(":");
						if (findParts.length != 2) {
							System.err.println("Invalid MONGODB.FIND parameters: " + argsSplit[i+1]);
						}
						mongoCollection = findParts[0].trim();
						String[] findKVs = findParts[1].split(",");
						if (findKVs.length == 0) {
							System.err.println("Invalid MONGODB.FIND key=value: " + argsSplit[i+1]);
						}
						for (String kv: findKVs) {
							String[] kvParts = kv.split("=");
							if (kvParts.length != 2) {
								System.err.println("Invalid MONGODB.FIND key=value: " + kv);
							}
							mongoFinds.put(kvParts[0].trim(), kvParts[1].trim());
						}
						i++;
					}
					else if (argsSplit[i].equals("MONGODB.READ")) {
						String[] readParts = argsSplit[i+1].split("=");
						if (readParts.length != 2) {
							System.err.println("Invalid MONGODB.READ parameters: " + argsSplit[i+1]);
						}
						mongoReads.put(readParts[0].trim(), readParts[1].trim());
						i++;
					}
					else if (argsSplit[i].equals("MONGODB.SAVE")) {
						String[] saveParts = argsSplit[i+1].split("=");
						if (saveParts.length != 2) {
							System.err.println("Invalid MONGODB.SAVE parameters: " + argsSplit[i+1]);
						}
						mongoSaves.put(saveParts[0].trim(), saveParts[1].trim());
						i++;
					}
					else if (argsSplit[i].equals("MONGODB.SORT")) {
						String[] sortParts = argsSplit[i+1].split("=");
						if (sortParts.length != 2) {
							System.err.println("Invalid MONGODB.READ parameters: " + argsSplit[i+1]);
						}
						mongoSortKey = sortParts[0].trim();
						mongoSortValue = sortParts[1].trim();
						i++;
					}				
				}				
				
				DB db = mongoClient.getDB( "troilkatt" );
				DBCollection coll = db.getCollection(mongoCollection);
				
				BasicDBObject findEntry = new BasicDBObject();
				for (String k: mongoFinds.keySet()) {
					findEntry.append(k, mongoFinds.get(k));
				}				
				DBCursor cursor = coll.find(findEntry);
				cursor.limit(0);
				if (cursor.count() == 0) {
					System.err.println("No MongoDB entry found");
					System.exit(-1);
				}
				
				if (mongoSortKey != null) {
					cursor.sort(new BasicDBObject(mongoSortKey, mongoSortValue));
				}
				DBObject mongoEntry = cursor.next();
				
				String oldArgs = o.stage.args;
				String newArgs = "";
				for (int i = 0; i < argsSplit.length; i++) {
					if (argsSplit[i].equals("MONGODB.FIND")) {
						String[] findParts = argsSplit[i+1].split(":");
						if (findParts.length != 2) {
							System.err.println("Invalid MONGODB.FIND parameters: " + argsSplit[i+1]);
						}
						mongoCollection = findParts[0].trim();
						String[] findKVs = findParts[1].split(",");
						if (findKVs.length == 0) {
							System.err.println("Invalid MONGODB.FIND key=value: " + argsSplit[i+1]);
						}
						for (String kv: findKVs) {
							String[] kvParts = kv.split("=");
							if (kvParts.length != 2) {
								System.err.println("Invalid MONGODB.FIND key=value: " + kv);
							}
							mongoFinds.put(kvParts[0].trim(), kvParts[1].trim());
						}
						i++;
					}
					else if (argsSplit[i].equals("MONGODB.READ")) {
						String[] readParts = argsSplit[i+1].split("=");
						if (readParts.length != 2) {
							System.err.println("Invalid MONGODB.READ parameters: " + argsSplit[i+1]);
						}
						mongoReads.put(readParts[0].trim(), readParts[1].trim());
						i++;
					}
					else if (argsSplit[i].equals("MONGODB.SAVE")) {
						String[] saveParts = argsSplit[i+1].split("=");
						if (saveParts.length != 2) {
							System.err.println("Invalid MONGODB.SAVE parameters: " + argsSplit[i+1]);
						}
						mongoSaves.put(saveParts[0].trim(), saveParts[1].trim());
						i++;
					}
					else if (argsSplit[i].equals("MONGODB.SORT")) {
						String[] sortParts = argsSplit[i+1].split("=");
						if (sortParts.length != 2) {
							System.err.println("Invalid MONGODB.READ parameters: " + argsSplit[i+1]);
						}
						mongoSortKey = sortParts[0].trim();
						mongoSortValue = sortParts[1].trim();
						i++;
					}			
			}
			
			// Note! SGE task IDs start from 1, so task N+1 process the N'th input file
			o.process2(o.inputFiles.get(taskNumber - 1));
		} catch (StageInitException e1) {
			System.out.println("Could not initialize stage: " + e1);
			e1.printStackTrace();
			System.exit(-1);
		} catch (StageException e) {
			System.out.println("Could not execute stage: " + e);
			e.printStackTrace();
			System.exit(-1);
		}
		System.out.println("Done");
		System.exit(0);
	}
}
