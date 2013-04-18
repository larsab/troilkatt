package edu.princeton.function.troilkatt.source;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.net.ftp.FTPClient;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * Download all raw files for an organism from the GEO FTP server.
 *
 */
public class GeoRawOrgMongoDB extends GeoGDSMirror {
	public static final String rawFtpDir = "/pub/geo/DATA/supplementary/series";
	
	// Text file with the list of files that were returned by the retrieve() method in the last iteration
	protected final String metaFilename = "idlist";
	
	// Regexp used to select matching organisms
	protected Pattern orgPattern;
	
	// MongoDB
	protected String serverAdr;
	protected String collectionName;
	
	// Debug
	//private HashSet<String> currentIDList;
	
	/**
	 * Constructor
	 * 
	 * @param arguments [0] organism name (latin name in hypens such as 'Homo sapiens').
	 *                  [1] MongoDB server address
	 */
	public GeoRawOrgMongoDB(String name, String arguments, String outputDir,
			String compressionFormat, int storageTime, 
			String localRootDir, String nfsStageMetaDir, String nfsStageTmpDir,
			Pipeline pipeline)
			throws TroilkattPropertiesException, StageInitException {
		super(name, arguments, outputDir, compressionFormat, storageTime,
				localRootDir, nfsStageMetaDir, nfsStageTmpDir, pipeline, rawFtpDir);
		
		
		
		/* setup regexp used to find organism names */
		String[] argsParts = splitArgs(this.args);
		try {
			orgPattern = Pattern.compile(argsParts[0].toLowerCase());
			logger.info("Organism: " + argsParts[0]);
		} catch (PatternSyntaxException e) {
			logger.fatal("Invalid filter pattern: " + argsParts[0], e);
			throw new StageInitException("Invalid filter pattern: " + argsParts[0]);
		}		
		
		serverAdr = argsParts[1];
		collectionName = "geoMeta";
		
		// Debug
		/*if (argsParts.length != 2) {
			logger.fatal("Debug mode requires second argument that specifies files to download");
			throw new StageInitException("Missing ID file");
		}
		else { 
			String[] lines = null;
			try {
				lines = FSUtils.readTextFile(argsParts[1]);
			} catch (IOException e) {
				logger.fatal("could not read from ID file");
			}
			currentIDList = new HashSet<String>();
			for (String l: lines) {
				String id = FilenameUtils.getDsetID(l);
				if (! id.startsWith("GSE")) {
					throw new StageInitException("Invalid ID: " + id);
				}
				currentIDList.add(id);
			}
		}*/
	}

	/**
	 * Download and return list of new GEO supplementary files.
	 * 
	 * @param metaFiles ignored
	 * @param logFiles list of meta-files which includes a file with the files new, and 
	 * downloaded files for each iteration.
	 * @return list of output files
	 * @throws StageException 
	 */
	@Override
	protected ArrayList<String> retrieve(ArrayList<String> metaFiles, ArrayList<String> logFiles,
			long timestamp) throws StageException {
		logger.info("Retrieve at: " + timestamp);
		
		/* Setup MongoDB */
		MongoClient mongoClient;
		try {
			mongoClient = new MongoClient(serverAdr);
		} catch (UnknownHostException e1) {
			logger.fatal("Could not connect to MongoDB server: ", e1);
			throw new StageException("Could not connect to MongoDB server: " + e1.getMessage());
		}
		DB db = mongoClient.getDB("troilkatt");
		DBCollection coll = db.getCollection(collectionName);
		
		DBCursor cursor = coll.find();
		// no limit, since the number of entries should be relatively low
		cursor.limit(0); 
		// sort in descending order according to timestamp
		cursor.sort(new BasicDBObject("timestamp", -1));
				
		/*
		 *  Get list of files that have been previously downloaded
		 */
		HashSet<String> oldIDs = readMetaFile(metaFiles, metaFilename);
				
		// Create log file with old IDs
		String oldLog = OsPath.join(stageLogDir, "old");
		try {
			FSUtils.writeTextFile(oldLog, oldIDs.toArray(new String[oldIDs.size()]));
			logFiles.add(oldLog);
		} catch (IOException e1) {
			logger.warn("Could not create log file: " + oldLog, e1);			
		}		
		logger.info("Old IDs: " + oldIDs.size());
				
		/*
		 *  Get a list of datset IDs for an organism from the geo-meta data table
		 */
		ArrayList<String> inputIDsList = MongoDBSource.scanMongoDB(cursor, "key", "meta:organisms", orgPattern, logger);
		HashSet<String> inputIDs = new HashSet<String>();
		for (String k: inputIDsList) {
			// ignore GDS entries
			if (k.startsWith("GDS")) {
				continue;
			}
			// make sure keys do not include platform specific part
			if (k.contains("GPL")) {
				k = k.split("-")[0];
			}
			// check for duplicates and add if unique
			if (! inputIDs.contains(k)) {				
				inputIDs.add(k);
			}
		}
		logger.info("Organism specific IDs: " + inputIDs.size());
		
		// Create log file with input IDs
		String inputLog = OsPath.join(stageLogDir, "input");
		try {
			FSUtils.writeTextFile(inputLog, inputIDs.toArray(new String[inputIDs.size()]));
			logFiles.add(inputLog);
		} catch (IOException e1) {
			logger.warn("Could not create log file: " + inputLog, e1);					
		}
		logger.info("Input IDs: " + inputIDs.size());
		
		/*
		 * Compare lists to find new datasets to load
		 */
		ArrayList<String> newIDs = new ArrayList<String>();
		for (String i: inputIDs) {
			// Debug
			//if (! oldIDs.contains(i) && currentIDList.contains(i)) {
			if (! oldIDs.contains(i)) {
				newIDs.add(i);
			}
		}
		logger.info("New IDs: " + newIDs.size());

		// Create log file with new files to be downloaded
		String newLog = OsPath.join(stageLogDir, "new");
		try {			
			FSUtils.writeTextFile(newLog, newIDs);
			logFiles.add(newLog);
		} catch (IOException e1) {
			logger.warn("Could not create log file: " + newLog, e1);
		}
		
		/*
		 * Connect to GEO FTP server
		 */
		FTPClient ftp = new FTPClient();		
	    try {
	    	if (connectFTP(ftp) == false) {
	    		throw new StageException("Could not connect to GEO FTP server");
	    	}			
		} catch (SocketException e) {
			logger.fatal("Could not connect to GEO FTP server: ", e);
			throw new StageException("Could not connect to GEO FTP server");
		} catch (IOException e) {
			logger.fatal("Could not connect to GEO FTP server: ", e);
			throw new StageException("Could not connect to GEO FTP server");
		}
		
		/*
		 * Download new files from FTP server, one at a time, save the file in NFS,
		 * and then delete the file on the local FS
		 */
		ArrayList<String> outputFiles = new ArrayList<String>();
		ArrayList<String> outputIDs = new ArrayList<String>();
		for (String i: newIDs) {
			String ftpFilename = OsPath.join(ftpDir, i + "/" + i + "_RAW.tar");
			String outputFilename = OsPath.join(stageTmpDir, i + "_RAW.tar");

			// Download raw file
			if (GeoRawMirror.downloadRawFile(ftp, ftpFilename, outputFilename, logger) == false) {
				// File could not be downloaded
				// Log messages already written 
				continue;
			}				

			// Upload file to NFS
			String nfsFilename = tfs.putLocalFile(outputFilename, tfsOutputDir, stageTmpDir, stageLogDir, compressionFormat, timestamp);				
			if (nfsFilename != null) {
				outputFiles.add(nfsFilename);
				outputIDs.add(i);
			}
			else {
				logger.fatal("Could not copy downloaded file to NFS");
				OsPath.delete(outputFilename);
				throw new StageException("Could not copy downloaded file to NFS");
			}

			// Update metafile with new downloaded file ID
			// This is done after each file to avoid re-downloading these in case of a crash
			try {
				// Create list with single entry since it is used as input to the appendTextFile 
				// method
				ArrayList<String> newIDlist = new ArrayList<String>();
				newIDlist.add(i);
				FSUtils.appendTextFile(OsPath.join(stageMetaDir, metaFilename), newIDlist);
			} catch (IOException e) {
				logger.fatal("Could not update metadata file: ", e);
				throw new StageException("Could not update metadata file: " + e.getMessage());
			}
			
			// Delete downloaded file
			OsPath.delete(outputFilename);			
		}	
		
		try {
			ftp.disconnect();
		} catch (IOException e1) {
			logger.warn("IOException during disconnet: ", e1);
		}

		// Create log file with a list of downloaded files
		String downloadedLog = OsPath.join(stageLogDir, "downloaded");
		try {
			FSUtils.writeTextFile(downloadedLog, outputIDs);
			logFiles.add(downloadedLog);
		} catch (IOException e1) {
			logger.warn("Could not create log file: " + downloadedLog, e1);
		}
		
		logger.info("Output IDs: " + outputIDs.size());
		return outputFiles;
	}
	
	/**
	 * Read in a list of filenames from the meta file with basename "metaFilename"
	 * 
	 * @param metaFiles list of meta files
	 * @param filename basename of the meta file to read from. This file must be in the 
	 * metaFiles list
	 * @return list of series IDs read from the meta file.
	 * @throws StageException
	 */
	protected HashSet<String> readMetaFile(ArrayList<String> metaFiles,
			String filename) throws StageException {
		HashSet<String> oldIDs = new HashSet<String>();
		if (metaFiles.size() == 1) { // There should only be one entry in the list
			String metaFile = metaFiles.get(0);
			String basename = OsPath.basename(metaFile);
			 if (! filename.equals(basename)) {
				logger.fatal("Metadata filename do not match: expected: " + filename + ", received: " + basename);
				throw new StageException("Local filename not specified for metadata file");
			}
			String[] lines;
			try {
				lines = FSUtils.readTextFile(metaFile);
			} catch (IOException e) {
				logger.fatal("Could not read from metadata file: ", e);
				throw new StageException("Could not read from metadata file: " + e.getMessage());
			}
			for (String l: lines) {
				oldIDs.add(l);
			}
		}
		else {
			logger.warn("Creating new metadata file: " + filename);			
			metaFiles.add(OsPath.join(stageMetaDir, filename));
			// The file will be updated after each file download
		}
		
		return oldIDs;
	}
	
}
