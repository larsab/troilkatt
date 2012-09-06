package edu.princeton.function.troilkatt.source;

import java.util.ArrayList;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * Superclass for sources that read the files from HDFS, and therefore
 * does not need to get/put files to local FS.
 */
public class HDFSSource extends Source {

	/**
	 * Constructor. 
	 * 
	 * See superclass for parameters.
	 */
	public HDFSSource(String name, String arguments, String outputDir,
			String compressionFormat, int storageTime, 
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline)
			throws TroilkattPropertiesException, StageInitException {
		super(name, arguments, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
	}

	/**
	 * Override to avoid reading/writing data files to local FS.
	 *
	 * @param timestamp timestamp added to output files.
	 * @return list of output filenames (HDFS filenames)
	 * @throws StageException if files could not be retrieved
	 */
	public ArrayList<String> retrieve2(long timestamp) throws StageException {
		logger.debug("Crawl at :  + " + timestamp);


		// Download meta data
		ArrayList<String> metaFiles = downloadMetaFiles();
		ArrayList<String> logFiles = new ArrayList<String>();
		
		// Retrieve new files	
		ArrayList<String> outputFiles = null;
		StageException eThrown = null;
		try {
			outputFiles = retrieve(metaFiles, logFiles, timestamp);
			saveMetaFiles(metaFiles, timestamp);
		} catch (StageException e) {
			// Delay exception until log files have been saved
			eThrown = e;
			logger.warn("Warning: retrieve failed");
		}
		
		// Always save logfiles and do cleanup
		saveLogFiles(logFiles, timestamp);
		cleanupLocalDirs();
		cleanupHDFSDirs();
		
		// Can now throw exception since log files are saved
		if (eThrown != null) {
			throw eThrown;
		}
		
 		logger.debug("Retrive done at " + timestamp);		
		return outputFiles;
	}
	
	/**
	 * HDFS sources have no output files to save
	 */
	@Override
	public ArrayList<String> saveOutputFiles(ArrayList<String> localFiles, long timestamp) throws StageException {
		throw new RuntimeException("saveOutputFiles() called by HDFS source");
	}
}
