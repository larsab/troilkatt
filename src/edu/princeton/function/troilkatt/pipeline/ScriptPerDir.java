package edu.princeton.function.troilkatt.pipeline;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;

/**
 * Execute one _instance_ of a Troilkatt script. Such a script typically process
 * all files in the input directory. 
 */
public class ScriptPerDir extends ExecuteDir {
	
	/**
	 * Constructor
	 * 
	 * @param args script to execute and script specific arguments. Note that the 
	 *  input, output, log and object directories should not be specified.
	 * @param for_other_arguments see description for super-class  
	 */
	public ScriptPerDir(int stageNum, String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String tfsStageMetaDir, String tfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		super(stageNum, name, args, 
				outputDirectory, compressionFormat, storageTime, 
				localRootDir, tfsStageMetaDir, tfsStageTmpDir,
				pipeline);
	
		if (args.isEmpty()) {
			logger.fatal("Troilkatt script to execute is not specified: " + args);
			throw new StageInitException("Troilkatt script to execute is not specified: " + args);
		}		
		String[] cmdParts = args.split(" ");
		String newCmd = null;
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
		newCmd = newCmd + " " + stageInputDir + " " + stageOutputDir + " " + stageMetaDir;
		newCmd = newCmd + " " + stageLogDir + " " + stageTmpDir;
		
		/* Add script specific arguments */
		for (int i = firstArgument; i < cmdParts.length; i++) {
			newCmd = newCmd + " " + cmdParts[i];
		}
		
		cmd = setTroilkattSymbols(newCmd);
	}
}