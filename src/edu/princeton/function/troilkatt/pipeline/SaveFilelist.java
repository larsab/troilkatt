package edu.princeton.function.troilkatt.pipeline;

import java.io.IOException;
import java.util.ArrayList;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;

/**
* Save the list of input files retrieved by this stage. Also output the input files
* without any changes.
*/
public class SaveFilelist extends Stage {
	protected String listFilename;
	
	/**
	 * The argument specifies the file in where the input file list is written. 
	 *    
	 * @param see description for super-class
	 */
	public SaveFilelist(int stageNum, String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		super(stageNum, name, args, 
				outputDirectory, compressionFormat, storageTime, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		if ((outputDirectory != null) && (! outputDirectory.isEmpty())) {
			logger.warn("The output directory will be ignored since this stage does not save any output files");
		}
		
		listFilename = this.args;
		String parentDir = OsPath.dirname(listFilename);
		if (! OsPath.isdir(parentDir)) {
			if (OsPath.mkdir(parentDir) == false) {
				logger.warn("Could not create directory for input file: " + listFilename);
				throw new StageInitException("Could not create directory: " + parentDir);
			}
		} 
	}
	
	/**
	 * The process2 function is overriden since files to be processed are not downloaded, and there are no
	 * meta nor logfiles
	 */
	@Override
	public ArrayList<String> process2(ArrayList<String> inputHDFSFiles, long timestamp) throws StageException {
		logger.debug("Start process2() at " + timestamp);
		
		// Do processing (i.e. write input filenames to a file)
		try {
			logger.info("Write to file: " + listFilename);
			FSUtils.writeTextFile(listFilename, inputHDFSFiles);
		} catch (IOException e) {
			logger.error("Could not write to file: " + listFilename, e);
			throw new StageException("Could not write to file: " + listFilename);
		}
		
		// And return the input files
		logger.debug("Process2() done at " + timestamp);
		return inputHDFSFiles;
	}
	
	/**
	 * Recover from a crashed iteration. The default recovery just re-processes all files. 
	 * If needed, subclasses should implement stage specific recovery functions. 
	 * 
	 * @param inputFiles list of input files to process.
	 * @param timestamp timestamp added to output files.
	 * @return list of output files
	 * @throws StageException 	 
	 */
	@Override
	public  ArrayList<String> recover(ArrayList<String> inputFiles, long timestamp) throws StageException {
		return process2(inputFiles, timestamp);
	}
}