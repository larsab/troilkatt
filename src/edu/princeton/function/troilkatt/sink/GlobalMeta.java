package edu.princeton.function.troilkatt.sink;

import java.util.ArrayList;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * Sink for writing processed data to the global meta directory.
 */
public class GlobalMeta extends Sink {
	protected String downloadDir;
	
	
	/**
	 * Constructor. See superclass for arguments
	 * 
	 * @param args: sub-directory on global meta-dir
	 */
	public GlobalMeta(int stageNum, String sinkName, String args,
			String localRootDir, String tfsStageMetaDir,
			String tfsStageTmpDir, Pipeline pipeline)
			throws TroilkattPropertiesException, StageInitException {
		super(stageNum, sinkName, args, localRootDir, tfsStageMetaDir,
				tfsStageTmpDir, pipeline);		
		
		if (args != null) {
			downloadDir = OsPath.join(this.globalMetaDir, this.args);
		}
		else {
			downloadDir = this.globalMetaDir;
		}
		if (! OsPath.isdir(downloadDir)) {
			logger.info("Creating download directory: " + downloadDir);
			if (OsPath.mkdir(downloadDir) == false) {
				throw new StageInitException("Could not create subdirectory: " + downloadDir);
			}
		}
	}

	/**
	 * Function called to download files from TFS to the global meta directory 
	 * 
	 * @param inputFiles list of TFS input files to sink.
	 * @param metaFiles list of meta files.
	 * @param logFiles list for storing log files.
	 * @return list of output files.
	 * @throws StageException thrown if stage cannot be executed.
	 */
	protected void sink(ArrayList<String> inputFiles,
			ArrayList<String> metaFiles,
			ArrayList<String> logFiles,
			long timestamp) throws StageException {
		ArrayList<String> localFiles = downloadInputFiles(inputFiles);
		for (String f: localFiles) {
			// The localFiles are in the stageInputDirectory
			// The replace is to find files that are in globalMetaDir subdirectories 			
			String globalName = OsPath.join(downloadDir,  f.replace(stageInputDir, ""));
			if (! OsPath.isfile(globalName)) {
				logger.warn("File to sink does not exists in the global meta-directory: " + globalName);				
			}
			
			if (OsPath.rename(f, globalName)) {
				logger.info("Updated: " + globalName);
			}
			else {
				throw new StageException("Could not update global meta-file: " + globalName);
			}
		}		
	}
}
