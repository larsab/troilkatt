package edu.princeton.function.troilkatt.source;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class ArrayExpressMirror extends Source {

	public ArrayExpressMirror(String name, String arguments, String outputDir,
			String compressionFormat, int storageTime, 
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline)
			throws TroilkattPropertiesException, StageInitException {
		super(name, arguments, outputDir, compressionFormat, storageTime, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
	}

}
