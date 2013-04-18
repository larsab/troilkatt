package edu.princeton.function.troilkatt.source;

import java.io.IOException;
import java.util.ArrayList;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * Retrieve a list of filenames by reading a file on the local FS.
 */
public class FileSource extends Source {
	protected String inputFile;

	/**
	 * Constructor.
	 *     
	 * @param arguments file with list of output files
	 */
	public FileSource(String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String tfsStageMetaDir, String tfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		
		super(name, args, 
				outputDirectory, compressionFormat, storageTime, 
				localRootDir, tfsStageMetaDir, tfsStageTmpDir, 
				pipeline);

		logger.debug("Initializing module");

		inputFile = args;			
		if (! OsPath.isfile(inputFile)) {
			logger.fatal("Not a file: " + inputFile);
			throw new StageInitException("Crawler initialization error: not a file");
		}

		logger.debug("FileSource initialized, reading from file: " + inputFile);        			
	}

	/**
	 * Read a list of TFS filenames from a file on the local FS.
	 * 
	 * @param metaFiles ignored
	 * @param logFiles input file is saved as a log file
	 * @return list of output files
	 * @throws StageException 
	 */
	@Override
	protected ArrayList<String> retrieve(ArrayList<String> metaFiles, ArrayList<String> logFiles,
			long timestamp) throws StageException {
		logger.info("Retrieve at " + timestamp);

		// Read the list of filenames from the input file
		String[] filenames = null;
		IOException eThrown = null; // set in case of exception
		try {
			filenames = FSUtils.readTextFile(inputFile);			
		} catch (IOException e) {
			logger.fatal("Could not open input file: " + inputFile, e);			
			// Excpetion is thrown when log files have been saved
			eThrown = e; 			
		}		
		
		// The input file is saved as a log file
		logFiles.add(inputFile);
		
		if (eThrown != null) {
			throw new StageException("Could not open input file: " + inputFile);
		}

		ArrayList<String> outputFiles = new ArrayList<String>();
		for (String fn: filenames) {								
			outputFiles.add(fn);
		}	    			    		
		
		return outputFiles;
	}
}
