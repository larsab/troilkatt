package edu.princeton.function.troilkatt.source;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.Stage;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * Troilkatt script to execute a set of commands specifies in a file. These commands may include
 * Troilkatt specific symbols that will be substituted before the commands are executed. This 
 * allows easily writing scripts to download and parse meta-data and gold standard files.
 * 
 * The command file has the following format:
 * -One command per line
 * -# specifies comments
 * 
 * The commands will be executed using the runtime exec methdo, and each individual command must 
 * take care of logging by for example redirecting stdout and stderr to files in TROILKATT.LOG_DIR.
 * 
 * This source uses the fail-stop failure model. That is, if one of the commands fails the subsequent
 * commands will not be executed. The log files of already executed commands is saved, but the meta
 * and output files will not be saved. Any files added to the global meta directory or other directoreis
 * will also be saved.
 */
public class OsCmdsSource extends Source {
	// Commands to execute in each iteration
	protected String[] cmds; 
	/**
	 * Constructor.
	 * 
	 * The args parameter specify the program to be run and its arguments. This string
	 * may include the usual Troilkatt symbols. But note that TROILKATT.FILE is illegal 
	 * for this stage.
	 *
	 * @param args file with OS commands to execute
	 * @param other see description for super-class
	 */
	public OsCmdsSource(String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String tfsStageMetaDir, String tfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		super(name, args, 
				outputDirectory, compressionFormat, storageTime, 
				localRootDir, tfsStageMetaDir, tfsStageTmpDir, 
				pipeline);
		
		if (! OsPath.isfile(this.args)) {
			throw new StageInitException("File specified in the arguments was not found: " + this.args);
		}
		try {
			cmds = readCommandsFile(this.args);
		} catch (IOException e) {
			logger.fatal("Could not read commands file: ", e);
			throw new StageInitException("Could not read commands from file: " + this.args);			
		}
	}

	/**
	 * Helper method to read set of commands to execute from a file and replace Troilkatt
	 * symbols in the commands.
	 * 
	 * @param filename commands filename
	 * @return list of commands to execute
	 * @throws TroilkattPropertiesException 
	 * @throws IOException 
	 */
	private String[] readCommandsFile(String filename) throws TroilkattPropertiesException, IOException {
		ArrayList<String> cmds = new ArrayList<String>();
		
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line = null;
		while ((line = br.readLine()) != null) {
			if (line.startsWith("#")) { // is a comment
				continue;
			}
			if (line.isEmpty()) {
				continue;
			}
			cmds.add( this.setTroilkattSymbols(line) ); // add line with symbols replaced
		}
		br.close();
		
		return cmds.toArray(new String[cmds.size()]);
	}
	
	/**
	 * Execute a set of commands.
	 * 
	 * @param metaFiles Not used by this source.
	 * @param logFiles list for storing log files produced by the executed program.
	 * @return list of output files in TFS
	 */
	@Override
	protected ArrayList<String> retrieve(ArrayList<String> metaFiles, ArrayList<String> logFiles,
			long timestamp) throws StageException {	   
		logger.debug("Execute cmd to retrieve a set of new files directory");

		try {
			FSUtils.writeTextFile(OsPath.join(stageLogDir, "cmds.log"), cmds);
		} catch (IOException e) {
			logger.error("Could not create commands log file", e);
			throw new StageException("Problems with log directory");
		}
		
		for (String cmd: cmds) {
			/* During the execution this thread is blocked
			 * Note that the output and error messages are not logged unless specified by the
			 * arguments string */			
			int rv = Stage.executeCmd(cmd, logger);
			
			if (rv != 0) {
				// Always save logfiles
				updateLogFiles(logFiles);
				logger.fatal("Command failed: " + cmd);
				throw new StageException("Command failed: " + cmd);
			}
		}
			
		// Save files written to the log directory
		updateLogFiles(logFiles);
		
		// Get list of output files and save these in TFS
		ArrayList<String> outputFiles = getOutputFiles();
		ArrayList<String> tfsOutputFiles = saveOutputFiles(outputFiles, timestamp);

		// Update list of meta files 
		updateMetaFiles(metaFiles);
		
		logger.debug(String.format("Returning (#output, #meta, #log) files: (%d, %d, %d)", 
				tfsOutputFiles.size(), metaFiles.size(), logFiles.size()));
		
		return tfsOutputFiles;
	}   
}
