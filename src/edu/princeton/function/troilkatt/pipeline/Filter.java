package edu.princeton.function.troilkatt.pipeline;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;

/**
* Filter: copy a selection of files from the input directory to the output directory, but
* do not copy any files.
*/
public class Filter extends Stage {
	protected Pattern pattern;
	
	/**
    * The arguments specify a regular expression used to match files to copy.
    *
    * Some useful regexp's are:
    *    .*\.foo = *.foo in the shell (copy all files with .foo extension) 
    *    
    * @param see description for super-class
    */
	public Filter(int stageNum, String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String tfsStageMetaDir, String tfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		super(stageNum, name, args, 
				outputDirectory, compressionFormat, storageTime, 
				localRootDir, tfsStageMetaDir, tfsStageTmpDir,
				pipeline);
		
		try {
			this.pattern = Pattern.compile(this.args);
		} catch (PatternSyntaxException e) {
			logger.fatal("Invalid filter pattern: " + args);
			throw new StageInitException("Invalid filter pattern: " + args);
		}
	}
	
	/**
	 * The process2 function is overriden since files to be processed are not downloaded, and there are no
	 * meta nor logfiles
	 */
	@Override
	public ArrayList<String> process2(ArrayList<String> inputTFSFiles, long timestamp) throws StageException {
		logger.debug("Start process2() at " + timestamp);
		
		// Do processing		
		ArrayList<String> tfsOutputFiles = process(inputTFSFiles, null, null, timestamp);
		
		logger.debug("Process2() done at " + timestamp);
		return tfsOutputFiles;
	}
		   
    
	/**
	 *  Filter all files produced by the previous step. The filtering is done on both
	 *  the local FS name (if given) and the TFS filename (if given). A file passes
	 *  the filter if one of these match the pattern specified as an argument in the
	 *  pipeline configuration file.   
	 * 
	 * @param inputFiles files to filter 
	 * @param metaFiles list of meta files
	 * @param logFiles list for storing log files
	 * @return list of output files that pass the filter
	 */
	@Override
	public ArrayList<String> process(ArrayList<String> inputFiles, 
			ArrayList<String> metaFiles, ArrayList<String> logFiles,
			long timestamp) throws StageException {
		ArrayList<String> outputFiles = new ArrayList<String>();
		
		logger.info("Filter: " + inputFiles.size() + " input files");
		for (String f: inputFiles) {
			Matcher matcher = pattern.matcher(f);
			if (matcher.find()) {									
				outputFiles.add(f);
			}        				
        }
		return outputFiles;
	}
}