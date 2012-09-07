package edu.princeton.function.troilkatt.pipeline;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import org.apache.log4j.Level;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;

import edu.princeton.function.troilkatt.fs.OsPath;

/** 
 * Superclass to execute a stage as a MapReduce job. The MapReduce job to execute should be 
 * a subclass of edu.princeton.function.troilkatt.mapreduce.TroilkattMapReduce
 * 
 * The subclass MapReduceStage can be used to execute a stage in parallel using MapReduce.
 */
public class MapReduce extends Stage {
	// List of Hadoop internal output files
	public static final String[] hadoopInternalOutputFiles = {
		"_logs", "_SUCCESS"};
	
	// The MapReduce jar file to execute
	protected String jarFile;
	// Main class in jar file
	protected String mainClass;
	// Maximum virtual memory size in megabytes. This value is used for the Java heap size and the ulimit for
	// external programs or scripts started from this stage
	// Note! This value should be smaller than taskMacVMemSize since the latter also must include the JVM code, etc
	protected long troilkattMaxVMem;
	// Maximum virtual memory size for the MapReduce task. This value is used as the ulimit for the mapper,
	// and for job distribution by the MapReduce framework. In megabytes.
	// Note! This value should be larger than troilkattMaxVMemSize 
	protected long taskMaxVMem;
	// The stage arguments
	protected String stageArgs; 
	// File with input arguments to MapReduce program
	public String argsFilename;
	// Command used to run MapReduce job
	protected String mapReduceCmd;
	
	
	/**
	 * Constructor.
	 * 
	 * @param args [0] jar file to run, [1] main class, [2...] stage arguments
	 * @param see description for ExecutePerFile class
	 */
	public MapReduce(int stageNum, String name, String args, 
			String outputDirectory, String compressionFormat, int storageTime,
			String localRootDir, String hdfsStageMetaDir, String hdfsStageTmpDir,
			Pipeline pipeline) throws TroilkattPropertiesException, StageInitException {
		super(stageNum, name, args,
				outputDirectory, compressionFormat, storageTime, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		parseMapReduceArgs(args);
		
	}
	
	/**
	 * Helper function to parse Mapreduce specific arguments
     * @throws StageInitException
	 * @throws TroilkattPropertiesException 
	 */
	public void parseMapReduceArgs(String args) throws StageInitException, TroilkattPropertiesException {
		String[] argsParts = args.split(" ");
        if (argsParts.length < 4) {
        	logger.fatal("Invalid arguments: " + args);
        	logger.fatal("Usage: jarFile mainClass maxTroilkattMem maxVirtualMem");
        	throw new StageInitException("Too few arguments: " + args);
        }
		
		jarFile = argsParts[0];
		if (jarFile.equals("TROILKATT.JAR")) {
			jarFile = troilkattProperties.get("troilkatt.jar");
		}
		mainClass = argsParts[1];
		
		try {
			troilkattMaxVMem = Long.valueOf(argsParts[2]);
			taskMaxVMem = Long.valueOf(argsParts[3]);			
		} catch (NumberFormatException e) {
			logger.fatal("Invalid max memory size argument: ", e);
			throw new StageInitException("Invalid number for maximum troilkatt or task vmem: " + argsParts[2] + ", " + argsParts[3]);
		}
		
        for (int i = 4; i < argsParts.length; i++) {
        	String p = argsParts[i];
        	if (stageArgs == null) {
        		stageArgs = p;
        	}
        	else {
        		stageArgs = stageArgs + " " + p;
        	}
        }        
        
		argsFilename = OsPath.join(stageTmpDir, "input.args");
		String libJars = troilkattProperties.get("troilkatt.libjars");
		mapReduceCmd = "hadoop jar " + jarFile + " " + mainClass + " -libjars " + libJars + " " + argsFilename;
	}
	
	/**
	 * The downloadInputFiles is overridden such that the input files are not downloaded
	 */
	@Override
	public ArrayList<String> downloadInputFiles(ArrayList<String> hdfsFiles) throws StageException {
		return hdfsFiles;
	}
	
	/**
	 * The save output files is overridden such that no files are saved
	 */
	@Override
	public ArrayList<String> saveOutputFiles(ArrayList<String> hdfsFiles, long timestamp) throws StageException {
		return hdfsFiles;
	}
	
	/**
	 * Helper function to create an arguments file. The filename is given by the argsFilename global
	 * variable.
	 * 
	 * edu.princeton.function.troilkatt.mapreduce.TroilkattMapreduce.readMapReduceArgsfile is the corresponding
	 * to read the arguments file
	 * 
	 * @param inputFiles of input files to process
	 * @param hdfsTmpOutputDir output directory for MapReduce job
	 * @param timstamp for this iteration
	 * @return none
	 * @throws StageException if file could not be created
	 */
	public void writeMapReduceArgsFile(ArrayList<String> inputFiles, 
			String hdfsTmpOutputDir, long timestamp) throws StageException {
		// Note! write order of lines must match read order in TroilkattMapReduce.readMapReduceArgsFile
		try {
			PrintWriter out = new PrintWriter(new FileWriter(argsFilename));
			out.println("configuration.file = " + troilkattProperties.getConfigFile());
			out.println("pipeline.name = " + pipelineName);			
			out.println("stage.name = " + stageName);
			out.println("stage.args = " + stageArgs);
			out.println("hdfs.output.dir = " + hdfsTmpOutputDir);
			out.println("compression.format = " + compressionFormat);
			out.println("storage.time = " + storageTime);
			//out.println("hdfs.log.dir = " + hdfsLogDir);			
			out.println("hdfs.meta.dir = " + hdfsMetaDir);
			String mapreduceDir;
			try {
				mapreduceDir = troilkattProperties.get("troilkatt.localfs.mapreduce.dir");
			} catch (TroilkattPropertiesException e) {
				logger.fatal("Invalid properies file: " + e.getMessage());
				throw new StageException("Could not create input arguments file");
			}
			out.println("mapred.input.dir = " + OsPath.join(mapreduceDir, "input"));
			out.println("mapred.output.dir = " + OsPath.join(mapreduceDir, "output"));
			out.println("mapred.meta.dir = " + OsPath.join(mapreduceDir, "meta"));
			out.println("mapred.global-meta.dir = " + OsPath.join(mapreduceDir, "global-meta"));
			out.println("mapred.tmp.dir = " + OsPath.join(mapreduceDir, "tmp"));
			
			out.println("jobclient.input.dir = " + stageInputDir);
			out.println("jobclient.output.dir = " + stageOutputDir);
			out.println("jobclient.meta.dir = " + stageMetaDir);
			out.println("jobclient.global-meta.dir = " + globalMetaDir);
			out.println("jobclient.log.dir = " + stageLogDir);
			out.println("jobclient.tmp.dir = " + stageTmpDir);
			
			//Logger lp = logger;
			Level level = logger.getLevel(); //null;			
			//while (lp != null) {
			//	level = lp.getLevel();
			//	if (level != null) {
			//		break;
			//	}
			//	else {
			//		lp = lp.getParent();
			//	}
			//}			
			String logLevelName;
			if (level == null) { // could not find level: set to info as fallback
				logger.warn("Could not find logging level");
				logLevelName = "info";
			}
			else {
				logLevelName = level.toString();
			}
			
			out.println("logging.level = " + logLevelName);			
			out.println("timestamp = " + timestamp);
			
			out.println("soft.max.memory.mb = " + troilkattMaxVMem);
			out.println("hard.max.memory.mb = " + taskMaxVMem);
			
			out.println("input.files.start");
			for (String f: inputFiles) {
				out.println(f);
			}
			out.println("input.files.end");
			out.close();
		} catch (IOException e1) {
			logger.fatal("Could not create input arguments file: " + e1.getMessage());
			throw new StageException("Could not create input arguments file");
		}
	}
	
	/**
	 * Handle MapReduce job output files. These are stored in the stage's HDFS tmp directory,
	 * and it may contain named files, reduce output files (part-r-*), and other hadoop
	 * specific files.
	 * 
	 * This function ignores map output files (part-m-*) and hadoop internal files such as
	 * _logs/* or _SUCESS.
	 * 
	 * This function timstamps, moves, and potentially compress all files in the HDFS tmp directory 
	 * to the DFS output directory, except the Hadoop internal files specified in the global variable 
	 *  hadoopInternalOutputFiles
	 * 
	 * It is possible to overwrite this function in order to for example merge and rename the
	 * reduce output files, or to save map output files 
	 * 
	 * @param hdfsTmpOutputDir output directory for MapReduce job
	 * @param timestamp timestamp to add to output files
	 * @return list of output files saved in the HDFS output directory.
	 * @throws StageException 
	 */
	protected ArrayList<String> moveMapReduceOutputFiles(
			String hdfsTmpOutputDir, long timestamp) throws StageException {	
		ArrayList<String> tmpFiles;
		ArrayList<String> outputFiles = new ArrayList<String>();
		try {
			tmpFiles = tfs.listdir(hdfsTmpOutputDir);		
		} catch (IOException e) {
			logger.fatal("Could not read list of outputfiles in HDFS: " + e.toString());
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
			
			String dstName = null;
			try {
				dstName = tfs.putHDFSFile(f, hdfsOutputDir, stageTmpDir, stageLogDir, compressionFormat, timestamp);
			} catch (IOException e) {
				logger.warn("Could not move file: " + f + ": IOException: " + e.getMessage());
			}
			if (dstName == null) {
				logger.warn("Could not move file: " + f);
			}
			else {
				outputFiles.add(dstName);
			}
		}
		
		return outputFiles;
	}

	/**
	 * Run the MapReduce program and update file lists.
	 * 
	 * @param inputFiles list of HDFS input files to process
	 * @param metaFiles list of meta files
	 * @param logFiles list for storing log files
	 * @return list of output files
	 * @throws StageException if stage cannot be executed
	 */
	@Override
	public ArrayList<String> process(ArrayList<String> inputFiles, 
			ArrayList<String> metaFiles, ArrayList<String> logFiles,
			long timestamp) throws StageException {						
		
		// Temporary MapReduce output directory
		String hdfsTmpOutputDir = OsPath.join(hdfsTmpDir, getStageID() + "-" + timestamp);
		
		// Create arguments file for MapReduce Job-task
		writeMapReduceArgsFile(inputFiles, hdfsTmpOutputDir, timestamp);
		
		// Redirect output and execute MapReduce job
		String outputLogfile = OsPath.join(stageLogDir, "mapreduce.output");
		String errorLogfile = OsPath.join(stageLogDir, "mapreduce.error");
		int rv = Stage.executeCmd(mapReduceCmd + " > " + outputLogfile + " 2> " + errorLogfile, logger);
		//int rv = Stage.executeCmd(mapReduceCmd, logger);
		
		//int rv = executeMapReduceCmd(hdftTmpOutputDir); 
		// Always update log files even if job crashes
		updateLogFiles(logFiles);
		if (rv != 0) {
			logger.warn("Mapreduce job failed with error code: " + rv);
			throw new StageException("Mapreduce job failed");
		}
		
		// Comrpess, timestamp and move MapReduce output files to output directory
		ArrayList<String> outputFiles = moveMapReduceOutputFiles(hdfsTmpOutputDir, timestamp);
		
		// Update list of meta and log files 
		updateMetaFiles(metaFiles);		
		
		return outputFiles;
	}
}
        
        
