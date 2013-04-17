package edu.princeton.function.troilkatt.pipeline;

import java.io.IOException;
import java.util.ArrayList;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;

/**
 * Execute many stage instances in parallel using MapReduce.
 */
public class MapReduceStage extends MapReduce {
	// The stage to execute
	protected String stageToExecute = null;
	
	/**
	 * Constructor.
	 * 
	 * @param args MapReduce stage (in edu.princeton.function.troilkatt.mapreduce) to run
	 * @param see description for ExecutePerFile class
	 */
	public MapReduceStage(int stageNum, String name, String args,
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline)
			throws TroilkattPropertiesException, StageInitException {
		super(stageNum, name, args, outputDirectory, compressionFormat,
				storageTime, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
				
	}
	
	/**
	 * Helper function to parse MapReduce specific arguments.
	 * 
     * @throws StageInitException
	 * @throws TroilkattPropertiesException 
	 */
	public void parseMapReduceArgs(String args) throws StageInitException, TroilkattPropertiesException {
		String[] argsParts = args.split(" ");
        if (argsParts.length < 3) {
        	logger.fatal("Invalid arguments: " + args);
        	logger.fatal("Usage: stageType maxTroilkattMem maxVirtualMem");
        	throw new StageInitException("Too few arguments: " + args);
        }
		
        jarFile = troilkattProperties.get("troilkatt.jar");
        mainClass = "edu.princeton.function.troilkatt.mapreduce.ExecuteStage";
		stageToExecute = argsParts[0];
		if (! StageFactory.isValidStageName(stageToExecute)) {
			throw new StageInitException("Invalid stage to execute: " + stageToExecute);
		}
		
		try {
			troilkattMaxVMem = Long.valueOf(argsParts[1]);
			taskMaxVMem = Long.valueOf(argsParts[2]);			
		} catch (NumberFormatException e) {
			logger.fatal("Invalid max memory size argument: ", e);
			logger.fatal("Args; " + args);
			throw new StageInitException("Invalid number for maximum troilkatt or task vmem: " + argsParts[1] + ", " + argsParts[2]);
		}
		
		stageArgs = stageToExecute;
        for (int i = 3; i < argsParts.length; i++) {
        	String p = argsParts[i];
        	if (stageToExecute.equals("execute_per_file") || stageToExecute.equals("execute_per_dir") ||
        			stageToExecute.equals("script_per_dir") || stageToExecute.equals("script_per_file")) {
        		if (p.equals(">")) {
        			p = "TROILKATT.REDIRECT_OUTPUT";
        		}
        		else if (p.equals("2>")) {
        			p = "TROILKATT.REDIRECT_ERROR";
        		}
        		else if (p.equals("<")) {
        			p = "TROILKATT.REDIRECT_INPUT";
        		}
        		
        		p = p.replace(";", "TROILKATT.SEPERATE_COMMAND");
        	}
        			        	
        	stageArgs = stageArgs + " " + p;        	
        }   
                
        // Override        
		String libJars = troilkattProperties.get("troilkatt.libjars");
		argsFilename = OsPath.join(stageTmpDir, "input.args");
		mapReduceCmd = "hadoop jar " + jarFile + " " + mainClass + " -libjars " + libJars + " " + argsFilename;
	}
	
	/**
	 * Handle MapReduceExecute stage output files. These are stored in the stage's HDFS tmp directory,
	 * and it may contain named files, reduce output files (part-r-*), and other hadoop
	 * specific files.
	 * 
	 * Note! This function assumes that all output files are already timestamped and compressed.
	 * 
	 * This function ignores map output files (part-m-*), reduce output files (part-r-*), and 
	 * all hadoop internal files such as * _logs/* or _SUCESS.
	 * 
	 * This function moves all files in the HDFS tmp directory to the DFS output directory.
	 * 
	 * @param hdfsTmpOutputDir output directory for MapReduce job
	 * @param timestamp timestamp to add to output files
	 * @return list of output files saved in the HDFS output directory.
	 * @throws StageException 
	 */
	@Override
	protected ArrayList<String> moveMapReduceOutputFiles(
			String hdfsTmpOutputDir, long timestamp) throws StageException {	
		ArrayList<String> tmpFiles;
		ArrayList<String> outputFiles = new ArrayList<String>();
		try {
			tmpFiles = tfs.listdir(hdfsTmpOutputDir);		
		} catch (IOException e) {
			logger.fatal("Could not read list of outputfiles in HDFS: ", e);
			throw new StageException("Could not read list of outputfiles in HDFS");
		}
		
		if (tmpFiles == null) {
			return outputFiles;
		}
		
		for (String f: tmpFiles) {
			String basename = OsPath.basename(f);
			if (OsPath.fileInList(hadoopInternalOutputFiles, basename, false)) {
				continue;
			}		
			if (basename.startsWith("part-m-")) {
				continue;
			}
			if (basename.startsWith("part-r-")) {
				continue;
			}
			
			try {
				String dstName = OsPath.join(tfsOutputDir, basename);
				if (tfs.renameFile(f, dstName) == false) {			
					logger.warn("Could not move file: " + basename + ": to: " + tfsOutputDir);
				}			
				else {
					outputFiles.add(dstName);
				}
			} catch (IOException e) {
				logger.warn("Could not move file: " + basename + ": IOException: ", e);
			}
		}
		
		return outputFiles;
	}
}
