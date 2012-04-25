package edu.princeton.function.troilkatt.pipeline;

import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.TroilkattFS;

/**
 * Execute a Troilkatt script once per file in the input directory
 */
public class ScriptPerFile extends ExecutePerFile {
	protected boolean saveObjects = false;
	
	/**
	 *  Constructor
	 * 
	 * @param args specifies the script including arguments, including the
	 * usual TROILKATT symbols. Note that the input, output, log and object directories 
	 * should not be specified.
	 * @param for_other_arguments see description for super-class
	 */
	public ScriptPerFile(int stageNum, String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String pipelineName, Stage prevStage, 			
			TroilkattProperties troilkattProperties, TroilkattFS tfs) throws TroilkattPropertiesException, StageInitException {
		super(stageNum, name, args, 
				outputDirectory, compressionFormat, storageTime, 
				pipelineName, prevStage,
				troilkattProperties, tfs);
		
		String[] cmdParts = args.split(" ");
		String newCmd = null;
		
		if (cmdParts.length < 1) {
			logger.fatal("Troilkatt script to execute could not be parsed: " + args);
			throw new StageInitException("Troilkatt script to execute is not specified: " + args);
		}
		
		/* Make sure the first word in the command is python */
		int firstArgument;
		if (cmdParts[0].indexOf("python") != -1) {
			newCmd = cmdParts[0] + " " + cmdParts[1];
			firstArgument = 2;
		}
		else {
			newCmd = "python " + cmdParts[0];
			firstArgument = 1;
		}
		
		// Add absolute path for configuration file
		newCmd = newCmd + " -c " + troilkattProperties.getConfigFile();
		
		/* Add directories */
		newCmd = newCmd + "TROILKATT.INPUT_DIR/TROILKATT.FILE TROILKATT.OUTPUT_DIR TROILKATT.META_DIR TROILKATT.GLOBALMETA_DIR TROILKATT.LOG_DIR";
		
		/* Add script specific arguments */
		for (int i = firstArgument; i < cmdParts.length; i++) {
			newCmd = newCmd + " " + cmdParts[i];
		}
		
		cmd = newCmd;			
	}
}
