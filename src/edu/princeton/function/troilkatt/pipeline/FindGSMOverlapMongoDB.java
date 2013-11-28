package edu.princeton.function.troilkatt.pipeline;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.hbase.TroilkattTable;
import edu.princeton.function.troilkatt.mongodb.GeoMetaCollection;
import edu.princeton.function.troilkatt.tools.FilenameUtils;
import edu.princeton.function.troilkatt.tools.GeoGSMOverlap;
import edu.princeton.function.troilkatt.tools.ParseException;

/**
 * Find overlapping samples to remove. For each dataset/series the samples to keep
 * are stored in the calculated:sampleIDs-overlapRemoved field in the geoMeta 
 * MongoDB collection.
 * 
 * Output file format, one line per datasets that has one or more, or all, datasets
 * removed. The line format is for a dataset/series that should be deleted: 
 *   
 *   dataset/series ID<tab>all<newline>
 *   
 * and for a dataset/series where N of the samples should be deleted:
 * 
 *   dataset/series ID<tab>sample ID 1, sample ID 2, ..., sample ID N<newline>
 *
 */
public class FindGSMOverlapMongoDB extends Stage {
	/*
	 * Stage arguments
	 */
	// Minimum number of samples in a series/dataset
	// Datasets with fewer samples are removed
	protected int minSamples;
	// Maximum number of overlapping samples between two series/datasets
	// For a pair of datasets with too much overlap, the overlapping samples are
	// removed from one dataset
	protected int maxOverlap;
	// Output filename, relative to output directory
	protected String outputFilename;
	// Filename where a list of all series/datasets is read
	protected String listFilename;
	
	// MongoDB server
	protected String serverAdr;
	protected int serverPort;
	
	/**
	 * Constructor 
	 *
	 * @param stageNum stage number in pipeline.
	 * @param name name of the stage.
	 * @param args [0] output filename, relative to output directory
	 *             [1] minimum number of samples in a series/dataset
	 *             [2] maximum number of overlapping samples in a series/dataset
	 *             [3]�list of all series/dataset files for which overlap has been calculated
	 *             [4] MongoDB server IP address
	 * @param outputDirectory output directory in NFS.
	 * @param compressionFormat compression to use for output files
	 * @param storageTime persistent storage time for output files in days. If -1 files
	 * are stored forever. If zero files are deleted immediately after pipeline execution is done.
	 * @param localRootDir directory on local FS used as root for saving temporal files
	 * @param nfsStageMetaDir meta file directory for this stage in NFS.
	 * @param nfsStageTmpDir tmp directory for this stage in NFS  (can be null).
	 * @param pipeline reference to the pipeline this stage belongs to.
	 * @throws TroilkattPropertiesException if there is an error in the Troilkatt configuration file
	 * @throws StageInitException if the stage cannot be initialized
	 */
	public FindGSMOverlapMongoDB(int stageNum, String name, String args,
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String nfsStageMetaDir,
			String nfsStageTmpDir, Pipeline pipeline)
			throws TroilkattPropertiesException, StageInitException {
		super(stageNum, name, args, outputDirectory, compressionFormat,
				storageTime, localRootDir, nfsStageMetaDir, nfsStageTmpDir,
				pipeline);
		
		logger.info("args: " + this.args);
		String[] argsParts = this.args.split(" ");
		if (argsParts.length != 6) {
			throw new StageInitException("Invalid number of arguments: expected 6, got " + argsParts.length);
		}
				
		outputFilename = argsParts[0];		 
		
		try {
			minSamples = Integer.valueOf(argsParts[1]);
			maxOverlap = Integer.valueOf(argsParts[2]);
			serverPort = Integer.valueOf(argsParts[5]);
		} catch (NumberFormatException e) {			
			logger.fatal("Number format exception: ", e);
			throw new StageInitException("Invalid arguments, one of these is not an integer: " + argsParts[1] + " or " + argsParts[2]  + " or " + argsParts[5]);
		}
		
		listFilename = argsParts[3];
		if (OsPath.isfile(listFilename) == false) {
			logger.warn("Specified list filename is not a file: " + listFilename);
		}
		
		serverAdr = argsParts[4];
		
	}
	
	/**
	 * Function called to find overlap.  
	 * 
	 * @param inputFiles list of input files on localFS to process.
	 * @param metaFiles list of meta files.
	 * @param logFiles list for storing log files.
	 * @return list of output files.
	 * @throws StageException thrown if stage cannot be executed.
	 */
	@Override
	public ArrayList<String> process(ArrayList<String> inputFiles, 
			ArrayList<String> metaFiles, ArrayList<String> logFiles,
			long timestamp) throws StageException {
		
		GeoGSMOverlap finder = new GeoGSMOverlap();
		
		MongoClient mongoClient;
		try {
			mongoClient = new MongoClient(serverAdr,serverPort);
		} catch (UnknownHostException e1) {
			logger.fatal("Could not connect to MongoDB server: ", e1);
			throw new StageException("Could not connect to MongoDB server: " + e1.getMessage());
		}
		DB db = mongoClient.getDB( "troilkatt" );
		DBCollection coll = db.getCollection("geoMeta");
		// Note! no check on getCollection return value, since these are not specified 
		// in the documentation
		
		/*
		 * Initialize finder tool with the input file data
		 */
		try {
			Collections.sort(inputFiles);
			for (String f: inputFiles) {
				String[] lines = FSUtils.readTextFile(f);
				for (String l: lines) {
					finder.addOverlapLine(l);
				}

			}
		} catch (IOException e) {
			logger.error("Could not read from input file", e);			
		} catch (ParseException e) {
			logger.error("Could not parse input file", e);			
		}
		
		/*
		 * Find overlap
		 */
		String logFilename = OsPath.join(stageLogDir, "overlap.log");
		BufferedWriter logFile = null;
		try {			
			logFile = new BufferedWriter(new FileWriter(logFilename));			
			logFiles.add(logFilename); 			
		} catch (IOException e) {			
			logger.error("Could not open log file", e);
			throw new StageException("Could not open log file: " + logFilename);
		}
		
		BufferedWriter outputFile = null;		
		try {			
			outputFile = new BufferedWriter(new FileWriter(OsPath.join(stageOutputDir, outputFilename)));
		} catch (IOException e) {
			logger.error("Could not open output file", e);
			throw new StageException("Could not open output file: " + outputFilename);
		}
					
		// This function also creates the output and log files
		try {
			finder.find(outputFile, logFile, minSamples, maxOverlap);			
		} catch (IOException e) {
			logger.error("Find failed: ", e);
			throw new StageException("Could not calculate overlap");			
		}
		
		try {
			outputFile.close();
			logFile.close();
		} catch (IOException e) {
			logger.warn("Could not close output or log file", e);
		}
		
		/*
		 * Update GEO meta table
		 * 
		 * TODO: move this to BatchPCLRemoveOverlapping ?
		 */
		HashSet<String> duplicates = finder.getDuplicateIDs();
		HashSet<String> supersets = finder.getSupersetIDs();
		HashSet<String> removed = finder.getRemovedIDs();
		HashMap<String, HashSet<String>> removedSamples = finder.getRemovedSamples();
		
		String[] allSoftFiles = null;
		try {
			allSoftFiles = FSUtils.readTextFile(listFilename);
		} catch (IOException e) {
			logger.error("Could not read from list file: " + listFilename, e);
			throw new StageException("Could not update Hbase meta table");
		}
		
		try {
			for (String f: allSoftFiles) {
				String gid = FilenameUtils.getDsetID(f);

				if (duplicates.contains(gid)) {
					// Dataset is deleted due to being a duplicate of another dataset
					putPostSamples(coll, gid, null, timestamp);
					continue;
				}
				
				if (supersets.contains(gid)) {
					// Dataset is deelted due to being a superset of included subsets
					putPostSamples(coll, gid, null, timestamp);
					continue;
				}
				
				if (removed.contains(gid)) {
					// Dataset has been removed
					putPostSamples(coll, gid, null, timestamp);
					continue;
				}				
				
				String preSamplesStr = (String) GeoMetaCollection.getField(coll, gid, "meta:sampleIDs");
				if (preSamplesStr == null) {
					logger.warn("Ignoring dataset (no samples): " + gid);
					continue;
				}
				ArrayList<String> preSamples = TroilkattTable.string2array(preSamplesStr);
				
				if (removedSamples.containsKey(gid)) {
					// Some samples have been removed
					for (String g: removedSamples.get(gid)) {
						preSamples.remove(g);
					}
				}
				
				putPostSamples(coll, gid, preSamples, timestamp);
					
			}
		} catch (IOException e) {
			logger.fatal("Could not update MondoFB collection", e);
			throw new StageException("Could not update MondoFB collection");
		}
		
		mongoClient.close();
		
		return getOutputFiles();
	}
	
	
	/**
	 * Write the processed:sampleIDs-overlapRemoved value to the GEO meta table
	 * 
	 * @param coll initialized geoMeta collection handle
	 * @param gid dataset/series identifier used as row key in the GEO meta table. 
	 * @param sampleIDs list of sampleIDs to add. This value can also be null if there
	 * are no samples left after overlap removal.
	 * @return none
	 * @throws IOException in case MongoDB entry could not be read (TODO: throw something else)
	 */
	protected void putPostSamples(DBCollection coll, String gid, ArrayList<String> sampleIDs, long timestamp) throws IOException {
		
		BasicDBObject entry = GeoMetaCollection.getNewestEntry(coll, gid);
		if (entry == null) {
			throw new IOException("No MongoDB entry for: " + gid);			
		}
		long entryTimestamp = GeoMetaCollection.getTimestamp(entry);
		if (entryTimestamp == -1) {
			throw new IOException("Could not find timestamp for MongoDB entry: " + gid);			
		}
		
		String val;
		if ((sampleIDs == null) || (sampleIDs.isEmpty())) {
			val = "none";
		}
		else {			
			val = TroilkattTable.array2string(sampleIDs);
		}
		entry.append("calculated:sampleIDs-overlapRemoved", val);
		
		GeoMetaCollection.updateEntry(coll, gid, entryTimestamp, entry);
	}
}
