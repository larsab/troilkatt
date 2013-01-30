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
import org.apache.log4j.Logger;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.tools.FilenameUtils;

/**
 * Mirror GEO raw files
 *
 */
public class GeoRawMirror extends GeoGDSMirror {
	public static final String rawFtpDir = "/pub/geo/DATA/supplementary/series";
	
	private HashSet<String> currentIDList;
	
	public GeoRawMirror(String name, String arguments, String outputDir,
			String compressionFormat, int storageTime, 
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline)
			throws TroilkattPropertiesException, StageInitException {
		super(name, arguments, outputDir, compressionFormat, storageTime,
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline, rawFtpDir);		
		
		// Debug
		/*String[] argsParts = splitArgs(this.args);
		if (argsParts.length != 1) {
			logger.fatal("Debug mode requires second argument that specifies files to download");
			throw new StageInitException("Missing ID file");
		}
		else { 
			String[] lines = null;
			try {
				lines = FSUtils.readTextFile(argsParts[0]);
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
	
		// Get list of files that have not already been downloaded
		HashSet<String> oldIDs = getOldIDs(hdfsOutputDir);		
		// Create log file with old IDs
		String oldLog = OsPath.join(stageLogDir, "old");
		try {
			FSUtils.writeTextFile(oldLog, oldIDs.toArray(new String[oldIDs.size()]));
			logFiles.add(oldLog);
		} catch (IOException e1) {
			logger.warn("Could not create log file: " + oldLog);
			logger.warn(e1.toString());
		}
	
		
		FTPClient ftp = new FTPClient();		
		try {
			if (connectFTP(ftp) == false) {
				throw new StageException("Could not connect to GEO FTP server");
			}			
		} catch (SocketException e) {
			logger.fatal("Could not connect to GEO FTP server: " + e);
			throw new StageException("Could not connect to GEO FTP server");
		} catch (IOException e) {
			logger.fatal("Could not connect to GEO FTP server: " + e);
			throw new StageException("Could not connect to GEO FTP server");
		}
		
		//DEBUG ArrayList<String> newFiles = getNewFiles(ftp, oldIDs);
		ArrayList<String> newFiles = new ArrayList<String>();		
		for (String i: currentIDList) {
			if (! oldIDs.contains(i)) {
				newFiles.add(OsPath.join(ftpDir, i + "/" + i + "_RAW.tar"));
			}
		}
		
		// Create log file with new files to be downloaded
		String newLog = OsPath.join(stageLogDir, "new");
		try {			
			FSUtils.writeTextFile(newLog, newFiles);
			logFiles.add(newLog);
		} catch (IOException e1) {
			logger.warn("Could not create log file: " + newLog);
			logger.warn(e1.toString());
		}
		
		// Download new files from FTP server, one at a time, save the file in HDFS,
		// and then delete the file on the local FS
		ArrayList<String> outputFiles = new ArrayList<String>();
		ArrayList<String> outputIDs = new ArrayList<String>();
		for (String n: newFiles) {						
			// Download file
			String outputFilename = OsPath.join(stageTmpDir, OsPath.basename(n));
			if (downloadRawFile(ftp, n, outputFilename, logger) == false) {
				// File could not be downloaded
				// Log messages already written 
				continue;
			}				

			// Upload file to HDFS
			String hdfsFilename = tfs.putLocalFile(outputFilename, hdfsOutputDir, stageTmpDir, stageLogDir, compressionFormat, timestamp);
			String id = FilenameUtils.getDsetID(n, false);
			if (hdfsFilename != null) {
				outputFiles.add(hdfsFilename);
				outputIDs.add(id);
			}
			else {
				logger.fatal("Could not copy downloaded file to HDFS");
				OsPath.delete(outputFilename);
				throw new StageException("Could not copy downloaded file to HDFS");
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
			logger.warn("Could not create log file: " + downloadedLog);
			logger.warn(e1.toString());
		}
		return outputFiles;
	}
	
	/**
	 * Download a raw file from the GEO FTP server
	 * 
	 * @param ftpClient FTP handle connected to the server
	 * @param ftpFilename file to download (absolute name)
	 * @param outputFilename destination filename
	 * @paral sLogger initialized logger instance
	 */
	public static boolean downloadRawFile(FTPClient ftpClient, String ftpFilename, String outputFilename, 
			Logger sLogger) {		
			
		try {
			// Download raw file				
			FileOutputStream fp;
			try {
				fp = new FileOutputStream(new File(outputFilename));
			} catch (FileNotFoundException e) {
				sLogger.fatal("Could not open output file: " + outputFilename, e);
				OsPath.delete(outputFilename);
				return false;
			}
			if (ftpClient.setFileType(FTP.BINARY_FILE_TYPE) == false) {
				sLogger.warn("Could not set filetype to binary");
				OsPath.delete(outputFilename);
				return false;
			}
			if (ftpClient.retrieveFile(ftpFilename, fp) == false) {
				sLogger.warn("Could not download file: " + ftpFilename);
				OsPath.delete(outputFilename);
				return false;
			}
			fp.close();
		} catch (IOException e) {
			sLogger.warn("Could not download file: " + ftpFilename, e);
			OsPath.delete(outputFilename);
			return false;
		}
		return true;

	}
}
