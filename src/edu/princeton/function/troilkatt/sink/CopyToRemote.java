package edu.princeton.function.troilkatt.sink;

import java.io.IOException;
import java.util.ArrayList;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.pipeline.Stage;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * 
 */
public class CopyToRemote extends Sink {
	protected String script = null;
	
	/**
	 * Constructor
	 * 
	 * @param args script to run in order to do the remote copy. This script should
	 * take as argument the file to copy. For example: "python runScp.py inputfile". The 
	 * script is run once for each input file using the filename as the argument.
	 * See superclass for description of other arguments.
	 */
	public CopyToRemote(int stageNum, String sinkName, String args,
			String localRootDir, String tfsStageMetaDir, String tfsStageTmpDir,
			Pipeline pipeline)
			throws TroilkattPropertiesException, StageInitException {
		super(stageNum, sinkName, args, localRootDir, tfsStageMetaDir, tfsStageTmpDir, pipeline);
		script = args;
	}
	
	/**
	 * Function called to copy data.    
	 * 
	 * @param inputFiles list of input files to sink.
	 * @param metaFiles list of meta files.
	 * @param logFiles list for storing log files.
	 * @return list of output files.	
	 * @throws StageException thrown if stage cannot be executed.
	 */
	@Override
	protected void sink(ArrayList<String> inputFiles,
			ArrayList<String> metaFiles,
			ArrayList<String> logFiles, long timestamp) throws StageException {
		logger.fatal("Copy input files to local FS");
				
		for (String f: inputFiles) {
			String localFilename;
			try {
				localFilename = tfs.getFile(f, stageTmpDir, stageTmpDir, stageLogDir);
				if (localFilename == null) {
					logger.fatal("Could not copy file to local FS: " + f);
					throw new StageException("Could not copy file to local FS");
				}				
			} catch (IOException e) {
				logger.fatal("Could not copy file to local FS: ", e);
				throw new StageException("Could not copy file to local FS: I/O Exception" + e);
			}
			
			String cmd = script + " " + localFilename;
			if (Stage.executeCmd(cmd, logger) != 0) {
				logger.fatal("Failed to copy file to remote FS");
				throw new StageException("Failed to copy file to remote FS");
			}
			else {
				logger.info("Copied file " + localFilename + " to remote FS");
			}
		}
		
		logger.info("Copied " + inputFiles.size() + " files to remote FS");
	}
}
