package edu.princeton.function.troilkatt.pipeline;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;

public class ScriptPerFileMR extends ExecutePerFileMR {

	public ScriptPerFileMR(int stageNum, String name, String args,
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String hdfsStageMetaDir,
			String hdfsStageTmpDir, Pipeline pipeline)
			throws TroilkattPropertiesException, StageInitException {
		super(stageNum, name, args, outputDirectory, compressionFormat,
				storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		String[] cmdParts = args.split(" ");
		String newCmd = null;
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
		// Add logging level
		newCmd = newCmd + " -l " + "warning";
		// Add timestamp holder
		newCmd = newCmd + " -t " + "TROILKATT.TIMESTAMP";
		// Add input file holder
		newCmd = newCmd + " -f " + "TROILKATT.FILE";
		
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
