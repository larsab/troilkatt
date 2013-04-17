package edu.princeton.function.troilkatt.pipeline;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;

/**
 * Execute an external program that takes as input and outputs a file. This is
 * a special version intended for MapReduce jobs,
 */
public class ExecutePerFileMR extends Stage {
	/* Command to execute per file*/
	protected String cmd;
	
	// Troilkatt container binary
	protected String containerBin;
	
	/*
	 * MapReduce task specific arguments 
	 */
	// troilkatt container command including arguments, but not the command to be run
	protected String containerCmd;
	
	// Hack to allow long runnning scripts to be used with MapReduce		
	public TaskAttemptContext mrContext = null;

	/**
	 * Constructor.
	 * 
	 * @param args specify the program to be run and its arguments. This string
	 * may include the usual Troilkatt symbols.
	 * @param for_other_arguments see description for super-class 
	 */
	public ExecutePerFileMR(int stageNum, String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		super(stageNum, name, args, 
				outputDirectory, compressionFormat, storageTime, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		this.cmd = this.args;
		
		this.containerBin = this.troilkattProperties.get("troilkatt.container.bin");
	}
	
	/**
	 * Function called to connect this stage to the MapReduce task. This is 
	 * necessary to get access to parameters used to set up the container 
	 * in which the script is run, and to ensure that progress is reported
	 * such that the MapReduce task is not killed.
	 * 
	 * @param maxProcs maximum number of containers than can be run simultaneously.
	 * This value is typically set to the max mapper/reducer tasks.  
	 * @param maxHeapSize maximum virtual memory size for the container in bytes.
	 * This value is typically set to the maximum heap size of a mapper/reducer task.
	 * @param jobID unique identifier for a job. This values is typically set to 
	 * the MapReduce job ID.
	 * @param context MapReduce context obect
	 */
	public void registerMR(int maxProcs, long maxHeapSize, String jobID,
			TaskAttemptContext context) {
		// Convert maxHeapSize to GB
		double d = Math.ceil((double) maxHeapSize / (1024*1024*1024));
		int maxVMSize = (int) d;		
		
		containerCmd = String.format("%s %d -1 %d %s ", 
				containerBin, maxVMSize, maxProcs, jobID);
		logger.info("Container cmd = " + containerCmd);
		mrContext = context;
	}
	
	/**
	 * Execute an command that processes all files in the input directory.
	 * 
	 * Note! this function is called from the MapReduce mapper directly,
	 * and not via process2.
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
			 * 
			 * Note that the output and error messages are not logged unless specified by the
			 * arguments string */
			if (executeCmdMR(fileCmd) != 0) {
				logger.fatal("Failed to execute program for file: " + tf);
				// Execution is thrown when log files have been saved
				cmdFailed = true;
				break;
			}
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
	
	/**
	 * Execute a command (*nix only). This version is intended to be used from a MapReduce
	 * job. The comamnd is executed within a troilkatt container.
	 * 
	 * @param cmd command to execute 
	 * @return child program exit value, or -1 if child program could not be executed.
	 */	
	public int executeCmdMR(String cmd) {		
		/* 
		 * It is necessary to execute the command using a shell in order to (esily) redirect
		 * stdout and stderr to a user specified file.
		 */
		String cmdV[] = {"/bin/bash", "-ic", containerCmd + cmd};
		System.out.printf("Execute: %s %s %s", cmdV[0], cmdV[1], cmdV[2]);
		logger.info(String.format("Execute: %s %s %s", cmdV[0], cmdV[1], cmdV[2]));		
		Process child;
		try {
			child = Runtime.getRuntime().exec(cmdV);
			int exitValue = 0;
				
			/* Mapreduce tasks need to report their progress periodically */				
			logger.debug("Waiting for command to complete");
			
			/*
			 * The wait must be unblocked periodically to report progress
			 * to MapReduce 
			 */
			final int waitTime = 5; // time between unblocks in seconds
			int running = 0;        // time the job has been running in seconds 
			boolean childDone = false;			
			mrContext.setStatus("Waiting for script to complete");
			while (! childDone) {					
				Thread.sleep(waitTime * 1000); // Argument is in ms				
				try {
					// This will throw an exception if the child is still running
					exitValue = child.exitValue();
					childDone = true;
				} catch (IllegalThreadStateException e) {
					// Child is not yet done
					running += waitTime;
					mrContext.setStatus("Child has run for: " + running + " seconds");
				}															
			}
										
			if (exitValue != 0) {
				logger.warn("Exit value for the executed command was: " + exitValue);					
			}
			
			return exitValue;
		} catch (IOException e) {
			logger.warn("IOExcpetion: ", e);
			logger.warn(String.format("Failed to execute: %s %s %s", cmdV[0], cmdV[1], cmdV[2]));			
			// User scripts may fail, but the processing should continue
			//throw new RuntimeException("Failed to execute command");			
		} catch (InterruptedException e) {
			logger.warn("Interrupted: ", e);
			logger.warn("Wait for child to complete was interrupted");		
		}
		
		return -1;
	}
}