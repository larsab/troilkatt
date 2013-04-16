package edu.princeton.function.troilkatt.sink;

import java.util.ArrayList;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.pipeline.Stage;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class Sink extends Stage {

	/**
	 * Constructor 
	 *
	 * @param stageNum stage number in pipeline.
	 * @param name name of the sink.
	 * @param args stage specific arguments.
	 * @param pipeline pipeline this stage belongs to.
	 * @throws TroilkattPropertiesException if there is an error in the Troilkatt configuration file
	 * @throws StageInitException if the stage cannot be initialized
	 */
	public Sink(int stageNum, String sinkName,
			String args, 
			String localRootDir, String tfsStageMetaDir, String tfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		super(stageNum, sinkName, args, localRootDir, tfsStageMetaDir, tfsStageTmpDir, pipeline);				
	}

	/**
	 * Sink function called in main loop.
	 * 
	 * @param inputFiles list of TFS filename of files to sink
	 * @param timestamp of iteration
	 * @throws StageException 
	 */
	public void sink2(ArrayList<String> inputFiles, long timestamp) throws StageException {
		logger.debug("Start sink2() at " + timestamp);

		// Download meta files		
		ArrayList<String> metaFiles = downloadMetaFiles();
		ArrayList<String> logFiles = new  ArrayList<String>();
		
		// Do processing	
		StageException eThrown = null;
		try {
			sink(inputFiles, metaFiles, logFiles, timestamp);
		} catch (StageException e) {
			// Do not throw exception until log files have been saved
			eThrown = e;
		}
		
		// Save log files to BigTable and do cleanup
		saveLogFiles(logFiles, timestamp);
		cleanupLocalDirs();
		cleanupTFSDirs();
		
		if (eThrown != null) {			
			throw eThrown;
		}
		
		logger.debug("Process2() done at " + timestamp);
	}
	
	/**
	 * Function called to process data. Sub-classes must implement this function.    
	 * 
	 * @param inputFiles list of TFS input files to sink.
	 * @param metaFiles list of meta files.
	 * @param logFiles list for storing log files.
	 * @return list of output files.
	 * @throws StageException thrown if stage cannot be executed.
	 */
	protected void sink(ArrayList<String> inputFiles,
			ArrayList<String> metaFiles,
			ArrayList<String> logFiles,
			long timestamp) throws StageException {
		logger.fatal("Super-class sink() called");
		throw new RuntimeException("sink() not implemented in subclass");
	}

	/**
	 * Note! that source classes should use the retrieve2() method instead
	 */
	@Override
	public ArrayList<String> process2(ArrayList<String> inputFiles, long timestamp) throws StageException {
		logger.fatal("Process2() called for sink");
		throw new RuntimeException("Process2() called for sink");
	}
	
	
	/**
	 * Note! sink classes should use the recover(long) method instead
	 */
	@Override
	public  ArrayList<String> recover(ArrayList<String> inputFiles, long timestamp) throws StageException {
		logger.fatal("Recover(inputFiles, timestamp) called for sink.");
		throw new RuntimeException("Recover(inputFiles, timestamp) called for sink.");
	}
	
	/**
	 * Note! sink functions should use the retrieve() method instead. 
	 */
	@Override
	public ArrayList<String> process(ArrayList<String> inputFiles, 
			ArrayList<String> metaFiles,
			ArrayList<String> logFiles, long timestamp) throws StageException {
		logger.fatal("Process() called for sink");
		throw new RuntimeException("Process() called for sink");
	}
	
	/**
	 * Sink classes should not save output files
	 */
	protected ArrayList<String> saveOutputFiles(ArrayList<String> localFiles) throws StageException {
		logger.fatal("saveOutputFiles() called for sink");
		throw new RuntimeException("saveOutputFiles() called for sink");
	}
}
