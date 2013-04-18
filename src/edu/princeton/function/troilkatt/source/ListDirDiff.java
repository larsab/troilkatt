package edu.princeton.function.troilkatt.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.tools.FilenameUtils;

/**
 * Compare the content of two directories A and B, and return a list of files that are 
 * only in directory A. For the comparison the filename extensions are removed. 
 */
public class ListDirDiff extends TFSSource {
	protected String srcDir;
	protected String dstDir;
	
	/**    
	 * Constructor. See superclass description for arguments.
	 * @param arguments directories A and B separated
	 */
	public ListDirDiff(String name, String arguments, String outputDir, String compressionFormat, int storageTime,
			String localRootDir, String tfsStageMetaDir, String tfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		super(name, arguments, outputDir, compressionFormat, storageTime, localRootDir, tfsStageMetaDir, tfsStageTmpDir, pipeline);

		logger.debug("Initializing module");

		/*
		 * Initialize global data structures
		 * 
		 * Note! that input and output directory is the same for this crawler        
		 */
		String[] dirs = arguments.split(" ");
		if (dirs.length != 2) {
			throw new StageInitException("Invalid argument length: expected 2, got " + dirs.length + " arguments");
			
		}
		srcDir = dirs[0];
		dstDir = dirs[1];
		if (srcDir.startsWith("hdfs://")) {
			srcDir = srcDir.substring(7);
		}
		if (dstDir.startsWith("hdfs://")) {
			dstDir = dstDir.substring(7);
		}
		
		try {
			if (! tfs.isdir(srcDir)) {
				logger.fatal("Not a directory: " + srcDir);
				throw new StageInitException("Not a directory: " + srcDir);
			}
			if (! tfs.isdir(dstDir)) {
				logger.fatal("Not a directory: " + dstDir);
				throw new StageInitException("Not a directory: " + dstDir);
			}
		} catch (IOException e) {
			logger.fatal("ListDirDiff initialization error: ", e);
			throw new StageInitException("ListDirDiff initialization error: " + e);
		}		

		logger.debug("ListDir initialized comparing dir: " + srcDir + " with " + dstDir);
	}

	/**
	 * Compare directory content of A and B, and return files that are only in A.
	 * 
	 * @param inputFiles list of input files to process.
	 * @param metaFiles ignored by ListDor.
	 * @param logFiles list for storing log files.
	 * @return list of output files.
	 * @throws StageException thrown if stage cannot be executed.
	 */
	@Override
	protected ArrayList<String> retrieve(ArrayList<String> metaFiles, ArrayList<String> logFiles,
			long timestamp) throws StageException {
		logger.info("List dir at: " + timestamp);
		 
		// Get directory listings
		ArrayList<String> filesA = null;
		ArrayList<String> filesB = null;	
		try {
			filesA = tfs.listdirR(srcDir);
			filesB = tfs.listdirR(dstDir);
			if (filesA == null) {
				logger.fatal("Could not list directory: " + srcDir);
				throw new StageException("Could not list directory");
			}
			if (filesB == null) {
				logger.fatal("Could not list directory: " + dstDir);
				throw new StageException("Could not list directory");
			}
		} catch (IOException e) {
			logger.fatal("Could not list directory: ", e);
			throw new StageException("Could not list directory" + e);
		}
		
		HashSet<String> setB = new HashSet<String>();
		for (String s: filesB) {
			String basename = OsPath.basename(s);
			//String noExts = basename.split("\\.")[0];
			String noExts = FilenameUtils.getDsetID(basename, false);
			setB.add(noExts);
		}
		
		// Compare content of directories
		ArrayList<String> outputFiles = new ArrayList<String>();
		for (String s: filesA) {
			String basename = OsPath.basename(s);
			//String noExts = basename.split("\\.")[0];
			String noExts = FilenameUtils.getDsetID(basename, false);
			if (! setB.contains(noExts)) {
				outputFiles.add(s);
			}
		}
			
		logger.info("Returning " + outputFiles.size() + " files");
		return outputFiles;
	}
}
