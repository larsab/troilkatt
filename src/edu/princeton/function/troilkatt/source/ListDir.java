package edu.princeton.function.troilkatt.source;

import java.io.IOException;
import java.util.ArrayList;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * Return a list of files in a HDFS directory. Note! that the listing is recursive 
 */
public class ListDir extends HDFSSource {
	protected String listDir;

	/**    
	 * Constructor. See superclass description for arguments.
	 * @param arguments directory to list.
	 */
	public ListDir(String name, String arguments, String outputDir, String compressionFormat, int storageTime,
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {

		super(name, arguments, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
		logger.debug("Initializing module");
		
		listDir = arguments;
		if (listDir.startsWith("hdfs://")) {
			listDir = listDir.substring(7);
		}
		
		try {
			if (! tfs.isdir(listDir)) {
				logger.fatal("ListDir source initialization error: not a directory");
				throw new StageInitException("ListDir source initialization error: not a directory");
			}
		} catch (IOException e) {
			logger.fatal("ListDir source initialization error: " + e.getMessage());
			throw new StageInitException("ListDir source initialization error: " + e.getMessage());
		}		

		logger.debug("ListDir initialized, reading from: " + listDir);
	}

	/**
	 * List directory content recursively.
	 * 
	 * @param inputFiles list of input files to process.
	 * @param metaFiles meta data files
	 * @param logFiles list for storing log files.
	 * @return list of files in HDFS.
	 * @throws StageException thrown if stage cannot be executed.
	 */
	@Override
	protected ArrayList<String> retrieve(ArrayList<String> metaFiles, 
			ArrayList<String> logFiles, long timestamp) throws StageException {
		logger.info("List dir at: " + timestamp);
		 
		ArrayList<String> outputFiles;
		try {
			outputFiles = tfs.listdirN(listDir);
			if (outputFiles == null) {
				logger.fatal("Could not list directory: " + listDir);
				throw new StageException("Could not list directory");
			}
		} catch (IOException e) {
			logger.fatal("Could not list directory: " + e.toString());
			throw new StageException("Could not list directory");
		}
		
		logger.info("Returning " + outputFiles.size() + " files");
		return outputFiles;
	}
}
