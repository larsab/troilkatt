package edu.princeton.function.troilkatt.sink;

import java.io.IOException;
import java.util.ArrayList;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class CopyToLocalFS extends Sink {
	protected String outputDir = null;
	
	/**
	 * Constructor
	 * 
	 * @param args directory where outputfiles should be copied to
	 * See superclass for description of other arguments.
	 */
	public CopyToLocalFS(int stageNum, String sinkName, String args,
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline)
			throws TroilkattPropertiesException, StageInitException {
		
		super(stageNum, sinkName, args, localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
		outputDir = args;
		
		if (! OsPath.isdir(outputDir)) {
			if (OsPath.mkdir(outputDir) == false) {
				logger.fatal("Could not create output directory: " + outputDir);
				throw new StageInitException("Could not create output directory");
			}
		}
	}

	/**
	 * Function called to copy data.    
	 * 
	 * @param inputFiles list of input files in HDFS to sink.
	 * @param metaFiles ignored by this sink
	 * @param logFiles ignored by this sink
	 * @return list of output files.
	 * @throws StageException thrown if stage cannot be executed.
	 */
	@Override
	protected void sink(ArrayList<String> inputFiles,
			ArrayList<String> metaFiles,
			ArrayList<String> logFiles, long timestamp) throws StageException {
		logger.fatal("Copy input files to local FS");
		
		for (String f: inputFiles) {			
			try {
				if (tfs.fileSize(f) == 0) {
					logger.warn("Ignoring empty file: " + f);
				}
				String localFilename = tfs.getFile(f, outputDir, stageTmpDir, stageLogDir);
				if (localFilename == null) {
					logger.fatal("Could not copy file " + f + " to local FS directory: " + outputDir);
					throw new StageException("Could not copy file to local FS");
				}
				else {
					logger.info("Copied file to local FS: " + localFilename);
				}
			} catch (IOException e) {
				logger.fatal("Could not copy file to local FS: " + e.toString());
				throw new StageException("Could not copy file to local FS: I/O Exception");
			}
		}
		
		logger.info("Copied " + inputFiles.size() + " files to " + outputDir);
	}
}
