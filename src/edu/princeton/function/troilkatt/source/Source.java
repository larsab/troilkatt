package edu.princeton.function.troilkatt.source;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.log4j.Logger;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.pipeline.Stage;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * Generic source class. Different soure implementations inherit from this class.
 *
 * A source is a special case of a pipeline stage.
 *
 * Subclasses of Source must implement the following functions:
 * -retrieve()
 * -recover(), if some special action is required for recovery.
 */
public class Source extends Stage {
	/**
	 * Constructor. 
	 *
	 * @param name name of the stage.
	 * @param args stage specific arguments.
	 * @param outputDirectory stage output directory in HDFS
	 * @param compressionFormat compression to use for output files
	 * @param storageTime persistent storage time for output files in days. If -1 files
	 * are stored forever.
	 * @param localRootDir directory on local FS used as root for saving temporal files
	 * @param hdfsStageMetaDir meta file directory for this stage in HDFS.
	 * @param hdfsStageTmpDir tmp directory for this stage in HDFS  (can be null).
	 * @param pipeline pipeline this stage belongs to.	
	 * @throws TroilkattPropertiesException if there is an error in the Troilkatt configuration file
	 * @throws StageInitException if the stage cannot be initialized
	 */
	public Source(String name, String arguments, String outputDir, String compressionFormat, int storageTime,
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {

		super(0, name, arguments, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);		
		this.logger = Logger.getLogger("troilkatt.source-" + name);	          			
	}

	/**
	 * When updating a dataset this function is called to initiate the crawling. This
	 * function does the following:
	 * 
	 * 1. Download meta-data files from HDFS
	 * 2. Call retrieve() to retrieve data (function must be implemented by a subclass)
	 * 3. Save all files in the log and meta directories to HDFS.
	 * 
	 * Note! files in the output directory are not automatically saved since it is
	 * assumed that each subclass will implement its own file save or just use the 
	 * provided saveOutputFiles method.
	 *
	 * @param timestamp timestamp added to output files.
	 * @return list of output filenames (HDFS filenames)
	 * @throws StageException if files could not be retrieved 
	 */
	public ArrayList<String> retrieve2(long timestamp) throws StageException {
		logger.debug("Retrieve at :  + " + timestamp);
		
		// Download meta data
		ArrayList<String> metaFiles = downloadMetaFiles();
		ArrayList<String> logFiles = new ArrayList<String>();
		
		// Retrieve new files	
		ArrayList<String> hdfsOutputFiles = null;
		StageException eThrown = null;
		try {
			hdfsOutputFiles = retrieve(metaFiles, logFiles, timestamp);
			logger.info("Retrive returned: " + hdfsOutputFiles.size());
			saveMetaFiles(metaFiles, timestamp);
		} catch (StageException e) {
			// Do not throw exception until log files have been saved
			logger.warn("Retrieve failed");
			eThrown = e;
		}
		
		// Save log files to BigTable and do cleanup
		saveLogFiles(logFiles, timestamp);		
		cleanup();
		
		if (eThrown != null) {			
			throw eThrown;
		}
				
		logger.debug("Retrive done at " + timestamp);
		
		return hdfsOutputFiles;
	}

	/**
	 * Recover state from a previous iteration. The default recovery just re-retrieves all files. 
	 * If needed, subclasses should implement stage specific recovery functions. 
	 *
	 * @param timestamp timestamp added to output files.
	 * @return list of output files
	 * @throws StageException 	 
	 */
	public  ArrayList<String> recover(long timestamp) throws StageException {
		logger.debug("Recover retrieve at " + timestamp);

		if ((hdfsOutputDir != null) && (! hdfsOutputDir.isEmpty())) {
			// Read files from previous iteration
			try {
				return tfs.listdirT(hdfsOutputDir, timestamp);
			} catch (IOException e) {
				logger.fatal("Could not list files in outputdirectory: " + hdfsOutputDir, e);
				throw new StageException("Could not recover files from last iteration");
			}
		}
		else {
			// Must re-do operation
			return retrieve2(timestamp);
		}
	}

	/**
	 * Retrieve a set of files to be processed by a pipeline. This function is periodically 
	 * called from the main loop.
	 * 
	 * @param metaFiles list of meta filenames that have been downloaded to the meta directory.
	 * Any new meta files are added to tis list
	 * @param logFiles list for storing log filenames.
	 * @param timestamp of Troilkatt iteration.
	 * @return list of output files in HDFS.
	 * @throws StageException thrown if stage cannot be executed.
	 */
	protected ArrayList<String> retrieve(ArrayList<String> metaFiles, 
			ArrayList<String> logFiles, long timestamp) throws StageException {
		throw new RuntimeException("retrive() should be implemented by subclass");
	}

	/**
	 * @return true if this is a crawler. Subclasses of Crawler are always crawlers.
	 */
	@Override
	public boolean isSource(){
		return true;
	}

	/**
	 * Note! that source classes should use the retrieve2() method instead
	 */
	@Override
	public ArrayList<String> process2(ArrayList<String> inputFiles, long timestamp) throws StageException {
		logger.fatal("Process2() called for source");
		throw new RuntimeException("Process2() called for source");
	}

	/**
	 * Note! that source classes should use the recover(long) method instead
	 */
	@Override
	public  ArrayList<String> recover( ArrayList<String> inputFiles, long timestamp) throws StageException {
		logger.fatal("Recover(inputFiles, timestamp) called for source.");
		throw new RuntimeException("Recover(inputFiles, timestamp) called for source.");
	}
	
	/**
	 * Note! source functions should use the retrieve() method instead. 
	 */
	@Override
	public ArrayList<String> process(ArrayList<String> inputFiles, 
			ArrayList<String> metaFiles, ArrayList<String> logFiles,
			long timestamp) throws StageException {
		logger.fatal("Process() called for source");
		throw new RuntimeException("Process() called for source");
	}
	
	/**
	 * Sources do not download input files
	 */
	@Override
	public ArrayList<String> downloadInputFiles(ArrayList<String> hdfsFiles) throws StageException {
		logger.fatal("downloadInputFiles() called for source");
		throw new RuntimeException("downloadInputFiles() called for source");
	}
}
