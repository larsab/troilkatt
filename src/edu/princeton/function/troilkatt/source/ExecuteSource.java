package edu.princeton.function.troilkatt.source;

import java.util.ArrayList;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.pipeline.Stage;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/** 
 * Class that can be used to run a program that takes as input a directory 
 * and writes output files to another directory.
 *
 * This class overwrites the process() function of the superclass.
 */
public class ExecuteSource extends Source {
	// The command to execute
	protected String cmd; 
	
	/**
	 * Constructor.
	 * 
	 * The args parameter specify the program to be run and its arguments. This string
	 * may include the usual Troilkatt symbols. But note that TROILKATT.FILE is illegal 
	 * for this stage.
	 *
	 * @param args command to execute
	 * @param other see description for super-class
	 */
	public ExecuteSource(String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		super(name, args, 
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
        cmd = setTroilkattSymbols(args);
	}

	
	/**
	 * Execute a command that retrieves a list of files to the output directory.
	 * 
	 * @param metaFiles list of meta files that have been copied to the local FS meta file
	 * directory.
	 * @param logFiles list for storing log files produced by the executed program.
	 * @return list of output files in HDFS
	 */
	@Override
	protected ArrayList<String> retrieve(ArrayList<String> metaFiles, ArrayList<String> logFiles,
			long timestamp) throws StageException {	   
		logger.debug("Execute cmd to retrieve a set of new files directory");

		/* During the execution this thread is blocked
		 * Note that the output and error messages are not logged unless specified by the
		 * arguments string */
		int rv = Stage.executeCmd(cmd, logger);
		// Always save logfiles
		updateLogFiles(logFiles);
		
		if (rv != 0) {
			logger.fatal("Retrieve program failed");
			throw new StageException("Retrieve program failed");
		}

		// Get list of output files and save these in HDFS
		ArrayList<String> outputFiles = getOutputFiles();
		ArrayList<String> hdfsOutputFiles = saveOutputFiles(outputFiles, timestamp);

		// Update list of meta files 
		updateMetaFiles(metaFiles);
		
		logger.debug(String.format("Returning (#output, #meta, #log) files: (%d, %d, %d)", 
				hdfsOutputFiles.size(), metaFiles.size(), logFiles.size()));
		
		return hdfsOutputFiles;
	}   
}
        
        
