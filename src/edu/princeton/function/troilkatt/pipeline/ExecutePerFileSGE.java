package edu.princeton.function.troilkatt.pipeline;

import java.util.ArrayList;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;

/**
 * Execute an external program that takes as input and outputs a file. This is
 * a special version intended for SGE jobs,
 */
public class ExecutePerFileSGE extends Stage {
	/* Command to execute per file*/
	protected String cmd;
	
	// Troilkatt container binary
	protected String containerBin;
	
	/*
	 * MapReduce task specific arguments 
	 */
	// troilkatt container command including arguments, but not the command to be run
	protected String containerCmd;

	/**
	 * Constructor.
	 * 
	 * @param args specify the program to be run and its arguments. This string
	 * may include the usual Troilkatt symbols.
	 * @param for_other_arguments see description for super-class 
	 */
	public ExecutePerFileSGE(int stageNum, String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String nfsStageMetaDir, String nfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		super(stageNum, name, args, 
				outputDirectory, compressionFormat, storageTime, 
				localRootDir, nfsStageMetaDir, nfsStageTmpDir,
				pipeline);
		this.cmd = this.args;
		
		this.containerBin = this.troilkattProperties.get("troilkatt.container.bin");
	}
	
	/**
	 * Function called to connect this stage to the SGE task. This is 
	 * necessary to get access to parameters used to set up the container 
	 * in which the stage is run.
	 * 
	 * @param maxProcs maximum number of containers than can be run simultaneously.
	 * @param maxHeapSize maximum virtual memory size for the container in bytes.
	 * @param jobID unique identifier for a job. This values is typically set to 
	 * the SHE job ID.
	 */
	public void registerSGE(int maxProcs, long maxHeapSize, String jobID) {
		// Convert maxHeapSize to GB
		double d = Math.ceil((double) maxHeapSize / (1024*1024*1024));
		int maxVMSize = (int) d;		
		
		containerCmd = String.format("%s %d -1 %d %s ", 
				containerBin, maxVMSize, maxProcs, jobID);
		logger.info("Container cmd = " + containerCmd);		
	}
	
	/**
	 * Execute an command that processes all files in the input directory.
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
		int counter = 0;
		int total = inputFiles.size();
		// Execute command per file in newFiles list        
		boolean cmdFailed = false;
		for (String tf: inputFiles) {            
			String fileCmd = setTroilkattFilenameSymbols(cmd, tf);
			fileCmd = setTroilkattTimestampSymbols(fileCmd, timestamp);
			/* During the execution this thread is blocked
			 * 
			 * Note that the output and error messages are not logged unless specified by the
			 * arguments string */
			if (Stage.executeCmd(containerCmd + fileCmd, logger) != 0) {
				logger.fatal("Failed to execute program for file: " + tf);
				// Execution is thrown when log files have been saved
				cmdFailed = true;
				break;
			}
			if ((++counter%10) == 0) 
				logger.info("Executed: "+counter+" commands [" + counter*100/total+" %]");

		}
		
		// Always update log file list
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