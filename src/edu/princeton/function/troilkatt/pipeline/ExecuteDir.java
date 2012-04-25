package edu.princeton.function.troilkatt.pipeline;

import java.util.ArrayList;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;

/** 
 * Class that can be used to run a program that takes as input a directory 
 * and writes output files to another directory.
 *
 * This class overwrites the process() function of the superclass.
 */
public class ExecuteDir extends Stage {
	// The command to execute
	protected String cmd; 
	
	/**
	 * The args parameter specify the program to be run and its arguments. This string
	 * may include the usual Troilkatt symbols. But note that TROILKATT.FILE is illegal 
	 * for this stage.
	 *
	 * @param: see description for super-class
	 */
	public ExecuteDir(int stageNum, String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		super(stageNum, name, args, 
				outputDirectory, compressionFormat, storageTime, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		if (args.contains("TROILKATT.FILE")) {
            logger.fatal("Invalid argument: TROILKATT.FILE");
            throw new StageInitException("Invalid argument: TROILKATT.FILE");
		}
        if (args.contains("TROILKATT.FILE_NOEXT")) {
            logger.fatal("Invalid argument: TROILKATT.FILE_NOEXT");
            throw new StageInitException("Invalid argument: TROILKATT.FILE_NOEXT");
        }
        cmd = this.args;
	}

	
	/**
	 * Execute an command that processes all files in the input directory
	 * 
	 * @param inputFiles ignored by this stage
	 * @param metaFiles list of meta files
	 * @param logFiles list for storing log files
	 * @return list of output files
	 * @throws StageException 
	 */
	@Override
	public ArrayList<String> process(ArrayList<String> inputFiles, 
			ArrayList<String> metaFiles, ArrayList<String> logFiles,
			long timestamp) throws StageException {		   
		logger.debug("Execute cmd to process all files in input directory");

		/* During the execution this thread is blocked
		 * Note that the output and error messages are not logged unless specified by the
		 * arguments string */
		String curCmd = setTroilkattTimestampSymbols(cmd, timestamp);
		int rv = Stage.executeCmd(curCmd, logger);
		// Always save log files
		updateLogFiles(logFiles);
		
		if (rv != 0) {
			logger.fatal("Failed to execute program");
			throw new StageException("Failed to execute program");
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
        
        
