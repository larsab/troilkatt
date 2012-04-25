package edu.princeton.function.troilkatt.source;

import java.util.ArrayList;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * Class for executing a crawler implemented as a Troilkatt Python script.
 */
public class ScriptSource extends ExecuteSource {
	
	/**
	 * Constructor.
	 * 
	 * @param args script to execute and script specific arguments. Note that the 
	 *  input, output, log and object directories should not be specified.
	 * 
	 * For other arguments refer do documentation for Stage constructor. 
	 */
	public ScriptSource(String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		super(name, args, outputDirectory, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
		
		String[] cmdParts = args.split(" ");
		String newCmd = null;
		
		if (args.isEmpty()) {
			logger.fatal("Troilkatt script to execute could not be parsed: " + args);
			throw new StageInitException("Troilkatt script to execute is not specified: " + args);
		}
		
		/* Make sure the first word in the command is python */
		/* Make sure the first word in the command is python */
		int firstArgument;
		if (cmdParts[0].startsWith("python")) {
			throw new StageInitException("The python binary should be spesified using the full absolute path");
		}
		if (cmdParts[0].indexOf("python") != -1) {
			newCmd = cmdParts[0] + " " + cmdParts[1];
			firstArgument = 2;
		}
		else {
			newCmd = "/usr/bin/python " + cmdParts[0];
			firstArgument = 1;
		}
		
		// Add absolute path for configuration file
		newCmd = newCmd + " -c " + troilkattProperties.getConfigFile();
		// Add logging level
		newCmd = newCmd + " -l " + "warning";
		// Add timestamp holder
		newCmd = newCmd + " -t " + "TROILKATT.TIMESTAMP";
		
		/* Add directories */
		newCmd = newCmd + " TROILKATT.TMP_DIR TROILKATT.OUTPUT_DIR TROILKATT.META_DIR TROILKATT.LOG_DIR TROILKATT.TMP_DIR ";
		
		/* Add script specific arguments */
		for (int i = firstArgument; i < cmdParts.length; i++) {
			newCmd = newCmd + " " + cmdParts[i];
		}
		
		cmd = setTroilkattSymbols(newCmd);
	}
	
	/**
	 * Execute a Troilkatt script that retrieves files and copies these to the output directory
	 * 
	 * @param metaFiles list of meta files that have been copied to the local FS meta file
	 * directory.
	 * @param logFiles list for storing log files produced by the executed program.
	 * @return list of output files in HDFS
	 */
	@Override
	protected ArrayList<String> retrieve(ArrayList<String> metaFiles, ArrayList<String> logFiles,
			long timestamp) throws StageException {	   

		// Replace TROILKATT.TIMESTAMP symbol with current timestamp and excecute the script
		String oldCmd = this.cmd;
		this.cmd = this.cmd.replace("TROILKATT.TIMESTAMP", String.valueOf((timestamp)));
		ArrayList<String> outputFiles = super.retrieve(metaFiles, logFiles, timestamp);
		
		// Revert cmd to string with symbol again
		this.cmd = oldCmd;

		return outputFiles;
	}
}
