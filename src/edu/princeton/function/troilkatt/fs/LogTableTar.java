package edu.princeton.function.troilkatt.fs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import edu.princeton.function.troilkatt.PipelineException;
import edu.princeton.function.troilkatt.pipeline.StageException;

/**
 * LogTable implement log file archiving in a tarball.
 * 
 * The architecture is as follows:
 * -Each pipeline has a separate directory. This makes it easier to administer the system by for
 * example deleting directories for debug and test pipelines.
 * -Each dierctory is named using the pipeline name. 
 * -There is one tarball per stage instance execution.
 * -The tarbal name is: stagenum-stagename.timestamp 
 * -There is sub-directory in the tarball per SGE task.
 * -The row key for SGE tasks is: stagenum-stagename.timestamp.taskid
 * -There are four column families: output, error, log, and other. Files are distributed
 * among column families based on their file suffix. 
 * -Just a single version of each cell is kept.
 */
public class LogTableTar extends LogTable {	
	private final String compressionFormat = "tar.bz2";
	
	protected String pipelineLogDir;
	protected TroilkattFS tfs;	
	protected String myLogDir;
	protected String tmpDir;
	
	/**
	 * Constructor.
	 * 
	 * Note! creating a Hbase Table is expensive, so it should only be done once. In addition,
	 * there should be one instance per thread.
	 * 
	 * @param tfs TroilkattFS handle
	 * @param logRooteDir root directory on NFS for log files. This is where log files will be put).
	 * @param pipelineName
	 * @param logDir logfile directory to be used by this class
	 * @param tmpDir directory for temporary files needed while saving packing log files
	 * 
	 * @throws PipelineException 
	 * 
	 */
	public LogTableTar(String pipelineName, TroilkattFS tfs, String logRootDir, String logDir, String tmpDir) throws PipelineException {	
		super(pipelineName);
		this.tfs = tfs;
		myLogDir = logDir;
		this.tmpDir = tmpDir;
		pipelineLogDir = OsPath.join(logRootDir, pipelineName);
		
		try {
			if (! tfs.isdir(logRootDir)) {
				throw new PipelineException("Invalid global log dir: " + logRootDir);
			}
			if (! tfs.isdir(pipelineLogDir)) {
				tfs.mkdir(pipelineLogDir);				
			}
		} catch (IOException e) {
			logger.fatal("Could not check or create pipelien logdir: " + e);
			throw new PipelineException("Failed to initialize log directory");
		}
	}
	

	/**
	 * Destructor
	 */
	public void finalize() {
		// nothing to do
	}

	/**
	 * Save logfiles to Hbase
	 * 
	 * @param stageName stage name used for the row ID
	 * @param timestamp Troilkatt timestamp used for the row ID
	 * @param logFiles log files to save
	 * 
	 * @throws StageException if file content could not be save in Hbase
	 * @return number of files saved, or -1 if an error occired
	 */
	@Override
	public int putLogFiles(String stageName, long timestamp, ArrayList<String> localFiles) throws StageException {		
		if (localFiles.isEmpty()) {
			logger.warn("No log files to save");
			return 0;
		}
		
		//	First make a temporary directory
		String tmpSubdir = OsPath.join(tmpDir, String.valueOf(timestamp));
		if (! OsPath.mkdir(tmpSubdir)) {
			logger.fatal("Could not make tmp directory: " + tmpDir);
		}
		
		String stageDir = OsPath.join(pipelineLogDir, stageName);
		
		if (! OsPath.isdir(stageDir)) {
			if (! OsPath.mkdir(stageDir)) {
				logger.fatal("Could not make stage directory: " + stageDir);
				return -1;				
			}
		}

		System.out.println("Copy files to " + tmpSubdir);
		int fileCnt = 0;
		for (String f: localFiles) {
			String tmpFilename = OsPath.join(tmpSubdir, OsPath.basename(f));				
			if (! OsPath.copy(f, tmpFilename)) {
				logger.fatal("Could not move file to tmp directory: " + f);
				return -1;
			}	
			fileCnt++;
		}			
					
		// Compress the tmp directory
		String compressedDir = tfs.compressDirectory(tmpSubdir, 
				getSubdirNameNoCompression(stageName, timestamp), // compression will be added
				myLogDir, compressionFormat); 
		if (compressedDir == null) {
			logger.fatal("Could not compress directory: " + tmpSubdir);						
			return -1;
		}
		
		return fileCnt;
	}
	
	/**
	 * Retrieve all logfiles for a stage to the local FS
	 * 
	 * @param stageName stage name used for the row ID
	 * @param timestamp Troilkatt timestamp used in the row ID
	 * @param localDir directory where all log-files are saved
	 * @return list of absolute filenames for all log files retrieved for stage
	 * @throws StageException  if a retrieved file cannot be written to the local FS, or
	 * the row could not be read from Hbase 
	 */
	@Override
	public ArrayList<String> getLogFiles(String stageName, long timestamp, String localDir) throws StageException {
		if (! OsPath.isdir(localDir)) {
			if (! OsPath.mkdir(localDir)) {
				logger.fatal("Could not create output directory: " + localDir);
				throw new StageException("mkdir failed for: " + localDir);
			}
		}
		
		String compressedDir = getSubdirName(stageName, timestamp);
		if (! OsPath.isfile(compressedDir)) {
			logger.fatal("There is no logfile archive for stage " + stageName + " at timestamp " + timestamp);
			return null;
		}
		
		// Unpack directory content		
		ArrayList<String> logFiles = tfs.uncompressDirectory(compressedDir, localDir, myLogDir);
		
		if (logFiles == null) {
			logger.fatal("Could not unpack log files archive");
			throw new StageException("Could not unpack logfiles");
		}
		return logFiles;
	}
	
	/**
	 * Check if a logfile exists. This function is mostly used for testing and debugging.
	 * 
	 * @param stageName stage name used or the row ID
	 * @param timestamp Troilkatt timestamp used in the row ID
	 * @param logFilename file to check
	 * @return true if a logfile exists, false otherwise
	 * @throws StageException 
	 */
	@Override
	public boolean containsFile(String stageName, long timestamp, String logFilename) throws StageException {
		String subDir = getSubdirName(stageName, timestamp);
			
		InputStream is;
		try {
			is = new FileInputStream(subDir);
		} catch (FileNotFoundException e1) {
			logger.fatal("Could not open: " + subDir + ": " + e1);
			return false;
		}
		CompressorInputStream cin = null;
		ArchiveInputStream ain = null;
		
		try {
			cin = new CompressorStreamFactory().createCompressorInputStream("bzip2", is);
		} catch (CompressorException e) { // This is expected, for example for the "none" format
			logger.fatal("Unknwon compression format");
			throw new RuntimeException("Unknwon compression format");
		}
		
		try {
			ain = new ArchiveStreamFactory().createArchiveInputStream("tar", cin);
		} catch (ArchiveException e) { // This is expected, for example for the "none" format
			logger.warn("Unknwon archive format");
			throw new RuntimeException("Unknwon archive format");
		}
			
		try {
			while (true) { // for all files in archive
				ArchiveEntry ae = ain.getNextEntry();
				if (ae == null) { // no more entries
					break;
				}
				
				String filename = ae.getName();
				if (filename.equals(logFilename)) { // file found 
					return true;
				}
				
			}
						
		} catch (IOException e1) {
			logger.error("Could not unpack archive entry: " + e1);
			try {
				ain.close();
			} catch (IOException e2) {
				logger.warn("Could not close archive in exception clause: " + e2);
			}
			return false;
		}
		return false;
	}
	
	/**
	 * 
	 * 
	 * @param stageName
	 * @param timestamp
	 * @return row key based on stageNum, stageName and timestamp
	 */
	private String getSubdirName(String stageName, long timestamp) {
		return OsPath.join(pipelineLogDir, String.format("%s/%d.%s", stageName, timestamp, compressionFormat)); 
	}
	
	/**
	 * 
	 * 
	 * 
	 * @param stageName
	 * @param timestamp
	 * @return row key based on stageNum, stageName and timestamp
	 */
	private String getSubdirNameNoCompression(String stageName, long timestamp) {
		return OsPath.join(pipelineLogDir, String.format("%s/%d", stageName, timestamp)); 
	}
}
