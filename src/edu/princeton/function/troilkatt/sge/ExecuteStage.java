package edu.princeton.function.troilkatt.sge;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import edu.princeton.function.troilkatt.PipelineException;
import edu.princeton.function.troilkatt.PipelinePlaceholder;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattNFS;
// for some statics functions that check and parse key-entry values
import edu.princeton.function.troilkatt.mapreduce.TroilkattMapReduce;
import edu.princeton.function.troilkatt.pipeline.Stage;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageFactory;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * Execute a Troilkatt stage in parallel as a MapReduce job.
 * 
 *
 * Mapper class that gets as input a filename and outputs a filename. For each file
 * it does the following:
 * 1. Copy a file to the local filesystem (the files can be tens of gigabytes in size)
 * 2. Execute a command that takes the local file as argument
 * 3. Copy the resulting file to NFS
 */
public class ExecuteStage {	
	
	protected String configurationFile;
	protected String pipelineName;				
	protected String stageName;
	protected String args;
	protected String taskID;
	protected String taskStageName;	
	protected String nfsOutputDir;
	//protected String taskNFSOutputDir;
	protected String compressionFormat;
	protected int storageTime;			
	protected String nfsMetaDir;	
	protected String nfsLogDir;
	protected String localFSPipelineDir;
	//protected String localFSInputDir;
	//protected String localFSOutputDir;
	//protected String localFSMetaDir;
	//protected String globalFSGlobalMetaDir;	
	
	protected String localFSTmpDir;								
	protected String loggingLevel;
	protected long timestamp;
	//protected long softMaxMemoryMb;
	//protected long hardMaxMemoryMb;
		
	// Initialized in constructor()
	protected Stage stage;	
		
	protected Logger logger;
	protected TroilkattProperties troilkattProperties;
	protected TroilkattNFS tfs;
	
	/**
	 * Constructor
	 * @throws StageInitException 
	 *  
	 * @throws PipelineException 
	 */		
	public ExecuteStage(String argsFilename, String taskID) throws StageInitException {
		logger = Logger.getLogger("troilkatt" + "-sge-" + taskID);
		
		// initialize stage arguments
		readSGEArgsFile(argsFilename);
		
		String[] argsSplit = args.split(" ");
		String stageType = argsSplit[0];
		String stageArgs = "";
		if (argsSplit.length > 1) {
			stageArgs = argsSplit[1];
			for (int i = 2; i < argsSplit.length; i++) {
				stageArgs = stageArgs + " " + argsSplit[i]; 
			}
		}
		
		this.taskID = taskID;		
		
		// ExecutePerFile and Script per file are replaced with special stages
		// that take into account the requirements of MapReduce tasks
		//if (stageType.equals("execute_per_file")) {
		//	stageType = "execute_per_file_mr";
		//}
		//else if (stageType.equals("script_per_file")) {
		//	stageType = "script_per_file_mr";
		//}
		// These stages should not be run as MapReduce jobs
		if (stageType.equals("execute_dir") ||
				stageType.equals("script_per_dir") ||
				stageType.equals("filter")) {
			throw new StageInitException("Stage type: " + stageType + " is not supported");
		}
		
		logger.info("Execute stage type: " + stageType);
		logger.info("Execute stage args: " + stageArgs);	

		// Get stage number from stageName
		String parts[] = stageName.split("-");
		if (parts.length < 2) {
			logger.fatal("Invalid stagename: " + stageName);
			throw new StageInitException("Invalid stagename: " + stageName);
		}
		int stageNum = 0;
		try {
			stageNum = Integer.valueOf(parts[0]);
		} catch (NumberFormatException e) {
			logger.fatal("Invalid number in stagename " + stageName + ": " + parts[0]);
			throw new RuntimeException("Invalid number in stagename " + stageName + ": " + parts[0]);
		}		
		// The stage name contains the task id, but not the stage number 
		taskStageName = parts[1] + "-" + taskID;	
				
		try {
			troilkattProperties = new TroilkattProperties(configurationFile);
		} catch (TroilkattPropertiesException e) {
			logger.fatal("Could not create troilkatt properties object from file: " + configurationFile, e);				
			throw new StageInitException("Failed to create troilkatt properties object from file: " + configurationFile);
		}
		
		/* Setup TFS/ NFS */		
		tfs = new TroilkattNFS();
					
		try {
			PipelinePlaceholder pipeline = new PipelinePlaceholder(pipelineName, 
				troilkattProperties, tfs);
			stage = StageFactory.newStage(stageType,
					stageNum,
					taskStageName, 
					stageArgs, 
					nfsOutputDir, 
					compressionFormat,
					storageTime,
					localFSPipelineDir,
					nfsMetaDir,
					null,
					pipeline,
					logger);				
		} catch (PipelineException e) {
			logger.fatal("Setup failed for task: " + taskStageName, e);				
			throw new StageInitException("Pipeline exception: " + e.getMessage());			
		} catch (TroilkattPropertiesException e) {
			logger.fatal("Setup failed for task: " + taskStageName, e);				
			throw new StageInitException("TroilkattPropertiesException exception: " + e.getMessage());
		} 		
	}
		
	/**
	 * Do the mapping: execute the stage on the input file. The stage specific
	 * code will take care of downloading input files and saving output and logfiles.
	 * 
	 * @param key HDFS soft filename
	 * @param value always null since the SOFT files can be very large	 
	 * @throws StageException 
	 */		
	public void run(String filename) throws StageException {					
		// Download meta Files 
		// Note! these are not saved
		ArrayList<String> metaFiles = stage.downloadMetaFiles();			

		// Prepare input filelist
		ArrayList<String> inputNFSFiles = new ArrayList<String>();
		inputNFSFiles.add(filename);

		/*
		 * This stage overrides the process2 function to better control which files are
		 * downloaded and saved where.
		 */
		logger.debug("Start process2() at " + timestamp);
		ArrayList<String> logFiles = new ArrayList<String>();
		//StageException eThrown = null; // set to true in case of excpeption in process()

		try {
			ArrayList<String> inputFiles = stage.downloadInputFiles(inputNFSFiles);				

			// Meta-data files where downloaded in setup()

			// Do the processing					
			ArrayList<String> outputFiles = stage.process(inputFiles, metaFiles, logFiles, timestamp);				

			// Save output, log, and meta files in NFS						
			stage.saveOutputFiles(outputFiles, timestamp);				
		} catch (StageException e) {
			logger.warn("Stage exceution failed: ", e);				
			// Do not throw exception until log files have been saved and local directories 
			// has been cleaned
			//eThrown = e;

			// Accept some subtasks to fail
		}

		// Move all log files to task specific log directory, these are then saved in cleanup
		for (String src: logFiles) {
			String dst = OsPath.join(nfsLogDir, taskID + "/" + OsPath.basename(src));
			if (OsPath.rename(src, dst) == false) {
				logger.warn("Could not move log file to task specific output directory");
			}
		}

		// Cleanup after each map
		stage.cleanupLocalDirs();			

		//if (eThrown != null) {
		//	doCleanup();
		// Log files saved and cleanup done, so can throw exception
		//	throw new IOException(eThrown);
		//}

		logger.debug("Process2() done at " + timestamp);			

		// All the output files will be in the task specific output directory. 
		// They will be moved automatically id the task succeeds.		
	}


	/**
	 * Helper function to initialize global variables by reading the arguments file.
	 * 
	 * edu.princeton.function.troilkatt.mapreduce.TroilkattMapReduce has the corresponding
	 * read arguments file.
	 * 
	 * Note! Logger has not yet been initialized since the log directory and logging level are
	 * specified in the arguments file (to be read by this function). 
	 * 
	 * @param filename arguments file created by the MapReduce pipeline stage
	 * @return none
	 * @throws StageInitException if arguments file could not be read or parsed
	 */
	protected void readSGEArgsFile(String filename) throws StageInitException {		
		try {
			BufferedReader ib = new BufferedReader(new FileReader(filename));
			
			// Note! read order of lines must match write order in MapReduce.writeMapReduceArgsFile
			configurationFile = TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "configuration.file");
			pipelineName = TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "pipeline.name");			
			stageName = TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "stage.name");
			args = TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "stage.args");	
			nfsOutputDir = TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "nfs.output.dir");			
			compressionFormat = TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "compression.format");
			storageTime = Integer.valueOf(TroilkattMapReduce.checkKeyGetValLong(ib.readLine(), "storage.time"));			
			nfsLogDir = TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "nfs.log.dir");
			nfsMetaDir = TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "nfs.meta.dir");	
			localFSPipelineDir = TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "sge.pipeline.dir");
			//localFSInputDir = TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "sge.input.dir");
			//localFSOutputDir = TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "sge.output.dir");
			//localFSMetaDir = TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "sge.meta.dir");
			//globalFSGlobalMetaDir = TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "sge.global-meta.dir");			
			
			localFSTmpDir = TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "sge.tmp.dir");								
			loggingLevel = TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "logging.level");
			timestamp = Long.valueOf(TroilkattMapReduce.checkKeyGetValLong(ib.readLine(), "timestamp"));
			//softMaxMemoryMb = Long.valueOf(TroilkattMapReduce.checkKeyGetValLong(ib.readLine(), "soft.max.memory.mb"));
			//hardMaxMemoryMb = Long.valueOf(TroilkattMapReduce.checkKeyGetValLong(ib.readLine(), "hard.max.memory.mb"));
		} catch (IOException e1) {			
			throw new StageInitException("Could not read arguments file: " + e1.getMessage());
		}
	}

	/**
	 * Arguments: [0] sgeArgsFilename
	 *            [2] input filename
	 *            [3] taskID         
	 */
	public static void main(String[] args) {		
		if (args.length < 4) {
			System.err.println("Usage: sgeArgsFilename inputFilename taskID");
			System.exit(-2);
		}
				
		String argsFilename = args[0];
		String inputFilename = args[1];
		String taskID = args[2];
						
		try {
			ExecuteStage o = new ExecuteStage(argsFilename, taskID);
			o.run(inputFilename);
		} catch (StageInitException e1) {
			System.err.println("Could not initialize stage");
			e1.printStackTrace();
			System.exit(-1);
		} catch (StageException e) {
			System.err.println("Could not execute stage");
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
