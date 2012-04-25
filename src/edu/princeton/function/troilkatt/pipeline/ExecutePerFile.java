package edu.princeton.function.troilkatt.pipeline;

import java.util.ArrayList;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;

/**
 * Execute an external program that takes as input and outputs a file.
 */
public class ExecutePerFile extends Stage {
	/* Command to execute per file*/
	protected String cmd;

	/**
	 * Constructor.
	 * 
	 * @param args specify the program to be run and its arguments. This string
	 * may include the usual Troilkatt symbols.
	 * @param for_other_arguments see description for super-class 
	 */
	public ExecutePerFile(int stageNum, String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		super(stageNum, name, args, 
				outputDirectory, compressionFormat, storageTime, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		this.cmd = this.args;
	}
	
	/**
	 * Execute an command that processes all files in the input directory
	 * 
	 * @param inputFiles list of input files to process
	 * @param metaFiles list of meta files
	 * @param logFiles list for storing log files
	 * @return list of output files
	 */
	@Override
	public ArrayList<String> process(ArrayList<String> inputFiles, 
			ArrayList<String> metaFiles, ArrayList<String> logFiles,
			long timestamp) throws StageException {	   
		logger.debug("Execute cmd to process all files in input directory");

		// Execute command per file in newFiles list        
		boolean cmdFailed = false;
		for (String tf: inputFiles) {            
			String fileCmd = setTroilkattFilenameSymbols(cmd, tf);
			fileCmd = setTroilkattTimestampSymbols(fileCmd, timestamp);
			/* During the execution this thread is blocked
			 * Note that the output and error messages are not logged unless specified by the
			 * arguments string */
			if (Stage.executeCmd(fileCmd, logger) != 0) {
				logger.fatal("Failed to execute program for file: " + tf);
				// Execution is thrown when log files have been saved
				cmdFailed = true;
				break;
			}
			
			// Clear tmp directory content between runs
			OsPath.deleteAll(stageTmpDir);
			OsPath.mkdir(stageTmpDir);
		}
		
		// Always uodate log file list
		updateLogFiles(logFiles);
		
		if (cmdFailed) {
			// log files saved so exception can be thrown
			throw new StageException("Failed to execute program for input file");
		}

		// Get list of output files
		ArrayList<String> outputFiles = getOutputFiles();
		// Update list of meta files 
		updateMetaFiles(metaFiles);		
		
		logger.debug(String.format("Returning (#output, #meta, #log) files: (%d, %d, %d)", 
				outputFiles.size(), metaFiles.size(), logFiles.size()));
		
		return outputFiles;
	} 
}