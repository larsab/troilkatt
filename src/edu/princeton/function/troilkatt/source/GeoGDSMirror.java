package edu.princeton.function.troilkatt.source;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.tools.FilenameUtils;

/**
 * Mirror all GEO GDS (dataset) files.
 */
public class GeoGDSMirror extends TFSSource {
	protected final String ftpServer = "ftp.ncbi.nih.gov";	
	public static final String GDSftpDir = "/pub/geo/DATA/SOFT/GDS";
	
	protected String ftpDir = null;    
	protected String adminEmail = null;    
    
    // Number of times to try FTP listing (these often fail on the GEO FTP server)
    protected final int FTP_LS_ATTEMPTS = 5;
    // Initial time to wait until retrying an FTP listing
	protected final int INITIAL_WAIT_TIME = 900000; // 15 minutes in milliseconds
	
    /**
	 * Constructor called in SourceFactory.
	 * 
 	 * See superclass for arguments.
	 */
	public GeoGDSMirror(String name, String arguments, String outputDir,
			String compressionFormat, int storageTime, 
			String localRootDir, String tfsStageMetaDir, String tfsStageTmpDir,
			Pipeline pipeline)
			throws TroilkattPropertiesException, StageInitException {
		this(name, arguments, outputDir, compressionFormat, storageTime, localRootDir, tfsStageMetaDir, tfsStageTmpDir, pipeline, GDSftpDir);
	}
	
	/**
	 * Constructor called by default constructors.
	 * 
	 * Note that a special constructor is needed since the other GEO mirrors inherit
	 * from this class. 
	 * 
	 * @param dir FTP directory where SOFT files are downloaded from
	 * 
	 * See superclass for additional arguments.
	 */
	public GeoGDSMirror(String name, String arguments, String outputDir,
			String compressionFormat, int storageTime, 
			String localRootDir, String tfsStageMetaDir, String tfsStageTmpDir,
			Pipeline pipeline,	String dir)
			throws TroilkattPropertiesException, StageInitException {
		super(name, arguments, outputDir, compressionFormat, storageTime, localRootDir, tfsStageMetaDir, tfsStageTmpDir, pipeline);
		
		this.ftpDir = dir;
		adminEmail = troilkattProperties.get("troilkatt.admin.email");		
	}

	/**
	 * Helper function to connect, login, and change directory on the FTP server.
	 * 
	 * @param ftp iniaitlized ftp client
	 * @param return true on success, false on failure
	 * @throws IOException 
	 * @throws SocketException 
	 */
	protected boolean connectFTP(FTPClient ftp) throws SocketException, IOException {
		ftp.connect(ftpServer);
		if (! ftp.login("anonymous", adminEmail)) {
			logger.fatal("Could not login to GEO FTP server");
			return false;
		}
		
		// FTP Passive mode must be used to get through many firewalls
		ftp.enterLocalPassiveMode();
		
		if (! ftp.changeWorkingDirectory(ftpDir)) {
			logger.fatal("Could not change working directory to: " + ftpDir);
			return false;
		}
		return true;
	}
	
	/**
	 * Download new files from the GEO FTP server and save these in tfs.
	 * 
	 * @param metaFiles ignored
	 * @param logFiles list of log files which includes a file with the files new, and 
	 * downloaded files for each iteration.
	 * @return list of output files in tfs
	 * @throws StageException 
	 */
	@Override
	protected ArrayList<String> retrieve(ArrayList<String> metaFiles, 
			ArrayList<String> logFiles, long timestamp) throws StageException {
		logger.info("Retrieve");
		
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
	
		// Get list of files that have not already been downloaded
		HashSet<String> oldIDs = getOldIDs(tfsOutputDir);		
		// Create log file with old IDs
		String oldLog = OsPath.join(stageLogDir, "old");
		try {
			FSUtils.writeTextFile(oldLog, oldIDs.toArray(new String[oldIDs.size()]));
			logFiles.add(oldLog);
		} catch (IOException e1) {
			logger.warn("Could not create log file: " + oldLog, e1);			
		}
		
		// Get list of files with ID not in oldIDs
		ArrayList<String> newFiles = getNewFiles(ftp, oldIDs);
		// Create log file with new files to be downloaded
		String newLog = OsPath.join(stageLogDir, "new");
		try {			
			FSUtils.writeTextFile(newLog, newFiles);
			logFiles.add(newLog);
		} catch (IOException e1) {
			logger.warn("Could not create log file: " + newLog, e1);			
		}
		
		// Download new files from FTP server, one at a time, save the file in tfs,
		// and then delete the file on the local FS
		ArrayList<String> outputFiles = new ArrayList<String>();
		ArrayList<String> outputIDs = new ArrayList<String>();
		for (String n: newFiles) {
			String tfsFilename = downloadFile(ftp, n, timestamp);
			if (tfsFilename == null) {
				logger.warn("File with ID: " + n + " not downloaded");
				// Sleep a while before continuing
				try {
					Thread.sleep(300000);
				} catch (InterruptedException e) {
					// Do nothing if sleep was interrupted
				} // 5 minutes
				
				try {
					ftp.disconnect();
				} catch (IOException e1) {
					logger.warn("IOException during disconnet: ", e1);
				}
				
				ftp = new FTPClient();				
				try {
					if (connectFTP(ftp) == false) {
						logger.error("Could not reconnect to FTP server");
						throw new StageException("Could not reconnect to FTP server");
					}
				} catch (SocketException e) {
					logger.warn("Could not reconnect to FTP server: ", e);
					throw new StageException("Could not reconnect to FTP server");
				} catch (IOException e) {
					logger.warn("Could not reconnect to FTP server: ", e);
					throw new StageException("Could not reconnect to FTP server");
				}
				
				continue;
			}
			String id = OsPath.basename(n).split("\\.")[0];
			outputFiles.add(tfsFilename);
			outputIDs.add(id);
		}	
		
		try {
			ftp.disconnect();
		} catch (IOException e1) {
			logger.warn("IOException during disconnet: ", e1);
		}
		
		// Create log file with list of downloaded fiels
		String downloadedLog = OsPath.join(stageLogDir, "downloaded");
		try {
			FSUtils.writeTextFile(downloadedLog, outputIDs);
			logFiles.add(downloadedLog);
		} catch (IOException e1) {
			logger.warn("Could not create log file:" + downloadedLog, e1);
			logger.warn(e1.toString());
		}
		
		
		return outputFiles;
	}
	
	/**
	 * Helper function to create a hash set with the IDs of previously downloaded datasets.
	 * 
	 * @param tfsOutputDir directory with previously downloaded datasets	
	 * @return list with datasets IDs in tfsOutputDir
	 * @throws StageException if tfsOutputDir could not be listed
	 */
	protected HashSet<String> getOldIDs(String tfsOutputDir) throws StageException {		
		
		// Get list of files already downloaded
		ArrayList<String> oldFiles;
		try {
			oldFiles = tfs.listdirR(tfsOutputDir);
		} catch (IOException e2) {
			logger.fatal("Could not list output directory: ", e2);
			throw new StageException("Could not list output directory: " + e2);
		}
		if (oldFiles == null) {
			logger.fatal("Could not list output directory: " + tfsOutputDir);
			throw new StageException("Could not list output directory");
		}
		HashSet<String> oldIDs = new HashSet<String>();
		for (String s: oldFiles) {
			String id = FilenameUtils.getDsetID(s);
			oldIDs.add(id);
		}
		
		logger.debug("Found " + oldIDs.size() + " previously downloaded files.");
		
		return oldIDs;
	}

	/**
	 * Helper function to compare the dataset IDs on the FTP server to the IDs of 
	 * previously downloaded datasets
	 * 
	 * @param oldIDs hash set with previously downloaded IDs
	 * @return list of files on the FTP server that have an ID that is not in oldIDs
	 * @throws StageException 
	 */
	protected ArrayList<String> getNewFiles(FTPClient ftp, HashSet<String> oldIDs) throws StageException {
		ArrayList<String> newFiles = new ArrayList<String>(); 
		ArrayList<String> newIDs = new ArrayList<String>();
		
		logger.debug("List files");
		
		// Get list of files on the FTP server
		String[] ftpFiles = listFTPDir(ftp);
		if (ftpFiles == null) {
			logger.fatal("Could not list FTP directory: null value returned");
			throw new StageException("Could not list FTP directory");
		}
		
		logger.debug("Received list of " + ftpFiles.length + " filenames");
					
		// Compare list of files on FTP server with files already downloaded
		for (String f: ftpFiles) {
			String id = FilenameUtils.getDsetID(f);
			if (! oldIDs.contains(id)) {
				newFiles.add(f);
				newIDs.add(id);
			}
		}	
		
		logger.debug("Of " + ftpFiles.length + " files on FTP server: " + newFiles.size() + " are new.");
		
		return newFiles;
	}
	
	/**
	 * Helper function to list the current FTP working directory.
	 * 
	 * @return array of files in the directory
	 * @throws StageException 
	 */
	protected String[] listFTPDir(FTPClient ftp) throws StageException {
		String[] ftpFiles = null;
		
		/*
		 * The list operation frequently fails on the GEO FTP server, especially
		 * for the series directory. We there make multiple attempts to retrieve 
		 * the directory listing, each with a very long wait between
		 */		
		int waitTime = INITIAL_WAIT_TIME;
		for (int i = 0; i < FTP_LS_ATTEMPTS; i++) {		
			try {
				// cwd = GDS directory 
				ftpFiles = ftp.listNames();
			} catch (IOException e) {
				logger.fatal("Could not list FTP directory: ", e);
				throw new StageException("Could not list FTP directory");
			}
			
			if (ftpFiles == null) {
				int waitTimeInMin = waitTime / (60 * 1000);			
				logger.warn("Could not list FTP directory...retrying in " + waitTimeInMin + " minutes");
				try {
					Thread.sleep(waitTime);
				} catch (InterruptedException e) {
					logger.warn("Interrupted during sleep.", e);				
				}
				waitTime = waitTime * 2; // wait longer before next attempt
			}
		}
		
		return ftpFiles;
	}

	/**
	 * Helper function to download a file, unpack it, and save the unpacked files in tfs.
	 * 
	 * @param filename file to download. The file should be in the ftpDir
	 * @return tfs filename or null if file could not be downlaoded
	 * @throws StageException if an exception occurs during download, unpacking or tfs save
	 */
	protected String downloadFile(FTPClient ftp, String filename, long timestamp) throws StageException {
	
		logger.debug("Download: " + filename);
		
		// Make sure inputDir is empty before downloading, since it is later used to
		// unpack the files that are output of this stage
		OsPath.deleteAll(stageInputDir);
		OsPath.mkdir(stageInputDir);
	
		// Download file
		String outputFilename = OsPath.join(stageInputDir, OsPath.basename(filename));
		FileOutputStream fp = null;
		try {
			fp = new FileOutputStream(new File(outputFilename));
		} catch (FileNotFoundException e) {
			logger.fatal("Could not open output file: " + e.toString() + ": ", e);
			throw new StageException("Could not open output file: " + e);
		}
		
		try {
			if (ftp.setFileType(FTP.BINARY_FILE_TYPE) == false) {
				logger.warn("Could not set filetype to binary");
				fp.close();
				return null;
			}
			if (ftp.retrieveFile(filename, fp) == false) {
				logger.warn("Could not download file: " + filename);
				OsPath.delete(outputFilename);
				return null;
			}
			fp.close();
		} catch (IOException e) {
			// This exception can be thrown if server closes connectin
			logger.fatal("Could not download file: ", e);
			OsPath.delete(outputFilename);
			return null;
		}
		 
		boolean fileUnpacked = unpackAll(stageInputDir, stageOutputDir);
		OsPath.delete(outputFilename);
		if (fileUnpacked == false) {
			logger.warn("Could not unpack file: " + outputFilename);			
			return null;
		}
		String[] unpackedFiles = OsPath.listdir(stageOutputDir, logger);
	
		for (String u: unpackedFiles) {
			if (u.endsWith(".SOFT") || u.endsWith(".soft")) {
				String tfsFilename = tfs.putLocalFile(u, tfsOutputDir, stageTmpDir, stageLogDir, compressionFormat, timestamp);				
				OsPath.delete(u);
				if (tfsFilename != null) {
					// File was successfully downloaded
					return tfsFilename;
				}
				else {
					logger.fatal("Could not copy downloaded file to tfs");					
					throw new StageException("Could not copy downloaded file to tfs");
				}
			}
		}		
		
		// File could not be downloaded
		return null;
	}

	/**
	 * Uncompress and unpack all files in a directory. The source file may or may not be deleted,
	 * depending on the program used to unpack the file.
	 * 
	 * @param srcDir directory with files to uncompress/unpack 
	 * @param dstDir directory where files are uncompressed
	 * @return true if at least one file was successfully unpacked/uncompressed
	 */	
	protected boolean unpackAll(String srcDir, String dstDir) {
		logger.info("Unpack all files in :" + srcDir);

		String[] files = OsPath.listdirR(srcDir, logger);

		for (String f: files) {
			String basename = OsPath.basename(f);
			String[] parts = basename.split("\\.");			
			if (parts[parts.length - 2].equals("tar")) {				
				ArrayList<String> uncompressedFiles = tfs.uncompressDirectory(f, dstDir, stageLogDir);
				if (uncompressedFiles.size() > 0) {
					return true;
				}
				else {
					return false;
				}
			}	
			else {
				String compression = parts[parts.length - 1];
				String uncompressedName = OsPath.join(dstDir, basename.replace("." + compression, ""));
				if (tfs.uncompressFile(f, uncompressedName, stageLogDir)) { // one or more file unpacked
					return true;
				}
			}
		}
				
		return false; // no files where uncompressed
	}
}
