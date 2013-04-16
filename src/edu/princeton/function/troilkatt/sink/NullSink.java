package edu.princeton.function.troilkatt.sink;

import java.util.ArrayList;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * Sink that does not do anything. Can be used either for debugging or
 * for a pipeline that does not need to sink files.
 */
public class NullSink extends Sink {

	/**
	 * Constructor.
	 * 
	 * See superclass for parameters.
	 */
	public NullSink(int stageNum, String sinkName, String args,
			String localRootDir, String tfsStageMetaDir, String tfsStageTmpDir,
			Pipeline pipeline)
			throws TroilkattPropertiesException, StageInitException {
		super(stageNum, sinkName, args, localRootDir, tfsStageMetaDir, tfsStageTmpDir, pipeline);	
	}

	/**
	 * Do nothing
	 */
	@Override
	protected void sink(ArrayList<String> inputFiles,
			ArrayList<String> metaFiles,
			ArrayList<String> logFiles, long timestamp) throws StageException {
		return;
	}
	
	/**
	 * Nothing to recover.
	 */
	@Override
	public  ArrayList<String> recover( ArrayList<String> inputFiles, long timestamp) throws StageException {
		return null;
	}
}
