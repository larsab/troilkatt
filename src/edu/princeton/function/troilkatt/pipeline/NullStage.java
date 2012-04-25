package edu.princeton.function.troilkatt.pipeline;

import java.util.ArrayList;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;

/**
* Stage that just returns the input files as output files.
*/
public class NullStage extends Stage {
	
	/**
	 * Constructor.
	 */
	public NullStage(int stageNum, String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		super(stageNum, name, args, 
				outputDirectory, compressionFormat, storageTime, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
	}
	
	/**
	 * The function is overriden since the stage does not do anything
	 * 
	 * @param inputHDFSFiles list of input files
	 * @return list of input files
	 */
	public ArrayList<String> process2(ArrayList<String> inputHDFSFiles, long timestamp) throws StageException {		
		return inputHDFSFiles;
	}
}
