package edu.princeton.function.troilkatt.source;

import java.util.ArrayList;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * A source that does not do anything.
 */
public class NullSource extends Source {

	public NullSource(String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String tfsStageMetaDir, String tfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		
		super(name, args, 
				outputDirectory, compressionFormat, storageTime, 
				localRootDir, tfsStageMetaDir, tfsStageTmpDir,
				pipeline);
	}
	
	/**
	 * Overriden to do nothing 
	 */
	@Override
	public ArrayList<String> retrieve2(long timestamp) throws StageException {
		logger.info("Retrieve at: " + timestamp);		
		return new ArrayList<String>();
	}
}
