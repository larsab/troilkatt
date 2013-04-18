package edu.princeton.function.troilkatt.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * Return a list of files in a TFS directory that have been added since the last iteration. 
 * Note! that the listing is recursive.
 */
public class ListDirNew extends ListDir {
	// Text file with the list of files that were returned by the retrieve() method in the last iteration
	protected final String metaFilename = "filelist";
	
	/**    
	 * Constructor. See superclass description for arguments.
	 * @param arguments directory to list.
	 */
	public ListDirNew(String name, String arguments, String outputDir, String compressionFormat, int storageTime,
			String localRootDir, String tfsStageMetaDir, String tfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		super(name, arguments, outputDir, compressionFormat, storageTime, localRootDir, tfsStageMetaDir, tfsStageTmpDir, pipeline);	
	}

	/**
	 * List directory content and return a list of files added since the last listing.
	 * 
	 * @param inputFiles list of input files to process.
	 * @param metaFiles array list with a single filename, which is the list of previously
	 * returned filenames. The file is updated after the retrieve.
	 * @param logFiles list for storing log files.
	 * @return list of output files in TFS.
	 * @throws StageException thrown if source cannot be executed.
	 */
	@Override
	protected ArrayList<String> retrieve(ArrayList<String> metaFiles, ArrayList<String> logFiles,
			long timestamp) throws StageException {
		logger.info("List dir at: " + timestamp);
		 
		// Read set of previous files
		HashSet<String> prevFiles = readMetaFile(metaFiles, metaFilename, listDir);
		
		// Get set of current files
		ArrayList<String> curFiles = absolute2relative(listDir,
				super.retrieve(metaFiles, logFiles, timestamp));		
		
		// Compare set of previous and current files, and add new files to output list
		ArrayList<String> outputFiles = OsPath.relative2absolute(listDir,
				compareSets(prevFiles, curFiles));
		
		try {
			FSUtils.appendTextFile(OsPath.join(stageMetaDir, metaFilename), outputFiles);
		} catch (IOException e) {
			logger.fatal("Could not update metadata file: ", e);
			throw new StageException("Could not update metadata file: " + e);
		}
		
		logger.info("Returning " + outputFiles.size() + " files");
		return outputFiles;
	}
	
	/**
	 * Read in a list of filenames from the meta file with basename "metaFilename"
	 * 
	 * @param metaFiles list of meta files
	 * @param filename basename of the meta file to read from. This file must be in the 
	 * metaFiles list
	 * @param rootDir root directory for the files listed in the meta file
	 * @return list of filenames read from the meta file. The filenames are relative to
	 * the listed directory (as specified by the listDir global variable).
	 * @throws StageException
	 */
	protected HashSet<String> readMetaFile(ArrayList<String> metaFiles,
			String filename, String rootDir) throws StageException {
		HashSet<String> prevFiles = new HashSet<String>();
		if (metaFiles.size() == 1) { // There should only be one entry in the list
			String metaFile = metaFiles.get(0);
			String basename = OsPath.basename(metaFile);
			 if (! filename.equals(basename)) {
				logger.fatal("Metadata filename do not match: expected: " + filename + ", received: " + basename);
				throw new StageException("Local filename not specified for metadata file");
			}
			String[] lines;
			try {
				lines = FSUtils.readTextFile(metaFile);
			} catch (IOException e) {
				logger.fatal("Could not read from metadata file: ", e);
				throw new StageException("Could not read from metadata file: " + e);
			}
			for (String l: lines) {
				String relativeName = OsPath.absolute2relative(l, rootDir);
				if (relativeName == null) {
					logger.fatal("Previous file not in listed directory: " + l);
					throw new StageException("Previous file not in listed directory: " + l);
				}
				prevFiles.add(relativeName);
			}
		}
		else {
			logger.warn("Creating new metadata file: " + filename);			
		}
		
		return prevFiles;
	}
	
	/**
	 * Convert a list of filenames to a list of filenames relative to a root directory
	 * 
	 * @param rootDir root directory
	 * @param absoluteFilenames list of filenames to convert
	 * @return filenames relative to rootDir
	 * @throws StageException if a file in absolute filenames is not relative to rootDir
	 */
	public static ArrayList<String> absolute2relative(String rootDir, ArrayList<String> absoluteFilenames) throws StageException {
		ArrayList<String> relativeFilenames = new ArrayList<String>();
		
		for (String f: absoluteFilenames) {			
			String relativeName = OsPath.absolute2relative(f, rootDir);
			if (relativeName == null) {				
				throw new StageException("Listed file not in listed directory: " + f);
			}			
			relativeFilenames.add(relativeName);
		}
		
		return relativeFilenames;		
	}
	
	/**
	 * Compare a set A and set B of relative filenames and return the files only in set B.
	 * 
	 * @param setA first set of relative filenames
	 * @param setB second set of relative filenames
	 * @return set of files only in setB
	 */
	public static ArrayList<String> compareSets(HashSet<String> setA, ArrayList<String> setB) {
		ArrayList<String> outputSet = new ArrayList<String>();
		for (String f: setB) {						
			if (! setA.contains(f)) {
				outputSet.add(f);				
			}
		}
		return outputSet;
	}
}
