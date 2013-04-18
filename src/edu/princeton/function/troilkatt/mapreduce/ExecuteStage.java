package edu.princeton.function.troilkatt.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.princeton.function.troilkatt.PipelineException;
import edu.princeton.function.troilkatt.PipelinePlaceholder;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.ExecutePerFileMR;
import edu.princeton.function.troilkatt.pipeline.ScriptPerFileMR;
import edu.princeton.function.troilkatt.pipeline.Stage;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageFactory;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * Execute a Troilkatt stage in parallel as a MapReduce job.
 * 
 */
public class ExecuteStage extends PerFile {	
	enum ExecCounters {
		// Input file read from HDFS
		FILES_READ,
		// Output files written to HDFS, can be > FILES_READ
		FILES_WRITTEN,
		// Files for which mapper did not fail
		FILES_PROCESSED,
		// Number of cmds executed that did not fail.
		CMDS_EXECUTED,
		// Number of cmds that failed
		CMDS_FAILED				
	}
	
	/**
	 * Mapper class that gets as input a filename and outputs a filename. For each file
	 * it does the following:
	 * 1. Copy a file to the local filesystem (the files can be tens of gigabytes in size)
	 * 2. Execute a command that takes the local file as argument
	 * 3. Copy the resulting file to HDFS
	 * 4. Output the HDFS filename to the reducer
	 */
	public static class ExecutePerFileMapper extends PerFileMapper {	
		// Counters
		protected Counter filesRead;
		protected Counter filesWritten;
		protected Counter filesProcessed;
		protected Counter cmdsExecuted;
		protected Counter cmdsFailed;
		
		protected String stageType;
		protected String stageArgs;
		protected String taskStageName;
		protected String taskMapredOutputDir;
		protected long timestamp;
		
		// Initialized in setup()
		protected Stage stage;
		// Initialized in setup()
		protected ArrayList<String> metaFiles;
		
		/**
		 * Setup global variables. This function is called before map()
		 * @throws IOException 
		 * @throws PipelineException 
		 */
		@Override
		public void setup(Context context) throws IOException {
			super.setup(context);

			filesRead = context.getCounter(ExecCounters.FILES_READ);
			filesWritten = context.getCounter(ExecCounters.FILES_WRITTEN);
			filesProcessed = context.getCounter(ExecCounters.FILES_PROCESSED);
			cmdsExecuted = context.getCounter(ExecCounters.CMDS_EXECUTED);
			cmdsFailed = context.getCounter(ExecCounters.CMDS_FAILED);
			
			String[] args = TroilkattMapReduce.confEget(conf, "troilkatt.stage.args").split(" ");
			stageType = args[0];
			String stageArgs = "";
			if (args.length > 1) {
				stageArgs = args[1];
				for (int i = 2; i < args.length; i++) {
					stageArgs = stageArgs + " " + args[i]; 
				}
			}	
			
			// ExecutePerFile and Script per file are replaced with special stages
			// that take into account the requirements of MapReduce tasks
			if (stageType.equals("execute_per_file")) {
				stageType = "execute_per_file_mr";
			}
			else if (stageType.equals("script_per_file")) {
				stageType = "script_per_file_mr";
			}
			// These stages should not be run as MapReduce jobs
			else if (stageType.equals("execute_dir") ||
					stageType.equals("script_per_dir") ||
					stageType.equals("filter")) {
				throw new IOException("Stage type: " + stageType + " is not supported");
			}
			
			mapLogger.info("Execute stage type: " + stageType);
			mapLogger.info("Execute stage args: " + stageArgs);
			
			// The stage name contains the task id
			String stageName = confEget(conf, "troilkatt.stage.name");
			taskStageName = stageName + "-" + confEget(conf, "mapred.task.id");

			// Must use task specific output directory in case of speculative execution			
			taskMapredOutputDir = confEget(conf, "mapred.work.output.dir");
			if (taskMapredOutputDir == null) {
				mapLogger.fatal("Task specific output directory is not specified");
				throw new IOException("Task specific output directory is not specified");
			}
			timestamp = Long.valueOf(confEget(conf, "troilkatt.timestamp"));

			try {
				String localRootDir = troilkattProperties.get("troilkatt.localfs.mapreduce.dir");
				PipelinePlaceholder pipeline = new PipelinePlaceholder(confEget(conf, "troilkatt.pipeline.name"), 
						troilkattProperties, tfs);

				String hdfsStageMetaDir = confEget(conf, "troilkatt.hdfs.meta.dir");
				
				// Get stage number from stageName
				String parts[] = stageName.split("-");
				if (parts.length < 2) {
					mapLogger.fatal("Invalid stagename: " + stageName);
					throw new RuntimeException("Invalid stagename: " + stageName);
				}
				int stageNum = 0;
				try {
					stageNum = Integer.valueOf(parts[0]);
				} catch (NumberFormatException e) {
					mapLogger.fatal("Invalid number in stagename " + stageName + ": " + parts[0], e);
					throw new RuntimeException("Invalid number in stagename " + stageName + ": " + parts[0]);
				}
				
				stage = StageFactory.newStage(stageType,
						stageNum,
						taskStageName, 
						stageArgs, 
						taskMapredOutputDir, // localRootDir
						confEget(conf, "troilkatt.compression.format"),
						Integer.valueOf(confEget(conf, "troilkatt.storage.time")),
						localRootDir,
						hdfsStageMetaDir,
						null,
						pipeline,
						mapLogger);
				
				if (stageType.equals("execute_per_file_mr")) {
					ExecutePerFileMR mrStage = (ExecutePerFileMR) stage;
					int maxMappers = Integer.valueOf(conf.get("mapred.tasktracker.map.tasks.maximum"));
					
					String val = conf.get("troilkatt.soft.max.memory.mb");
					long heapMaxSize = Runtime.getRuntime().maxMemory(); // use JVM heap size
					if (val != null) {	
						// use job specific value - JVM heap size
						heapMaxSize = (Long.valueOf(val) * 1024 * 1024) - heapMaxSize;  
					}
					
					mrStage.registerMR(maxMappers, heapMaxSize, jobID, context);
				}	
				else if (stageType.equals("script_per_file_mr")) {
					ScriptPerFileMR mrStage = (ScriptPerFileMR) stage;
					int maxMappers = Integer.valueOf(conf.get("mapred.tasktracker.map.tasks.maximum"));

					String val = conf.get("troilkatt.soft.max.memory.mb");
					long heapMaxSize = Runtime.getRuntime().maxMemory(); // use JVM heap size
					if (val != null) {	
						// use job specific value - JVM heap size
						heapMaxSize = (Long.valueOf(val) * 1024 * 1024) - heapMaxSize;  
					}
					
					mrStage.registerMR(maxMappers, heapMaxSize, jobID, context);
				}	
					
				// Download meta-data files from the stage specific HDFS directory to a task specific
				// directory
				metaFiles = downloadMetaFiles(tfs, hdfsStageMetaDir, stage.stageMetaDir, stage.stageTmpDir, stage.stageLogDir);
			} catch (PipelineException e) {
				mapLogger.fatal("Setup failed for task: " + taskStageName, e);				
				throw new IOException("Pipeline exception: " + e.getMessage());			
			} catch (TroilkattPropertiesException e) {
				mapLogger.fatal("Setup failed for task: " + taskStageName, e);				
				throw new IOException("TroilkattPropertiesException exception: " + e.getMessage());
			} catch (StageInitException e) {
				mapLogger.fatal("Setup failed for task: " + taskStageName, e);				
				throw new IOException("StageInitException exception: " + e.getMessage());	
			} catch (StageException e) {
				mapLogger.fatal("Could not download metadata for stage: " + taskStageName, e);				
				throw new IOException("Could not download metadata for stage: " + taskStageName);
			}
		}	
		
		
		/**
		 * Do the mapping: execute the stage on the input file. The stage specific
		 * code will take care of downloading input files and saving output and logfiles.
		 * 
		 * @param key HDFS soft filename
		 * @param value always null since the SOFT files can be very large	 
		 */
		@Override
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {		
			// Get filename that is stored as a property in the configuration
			String filename = key.toString();

			// Prepare input filelist
			ArrayList<String> inputHDFSFiles = new ArrayList<String>();
			inputHDFSFiles.add(filename);
			
			/*
			 * This stage overrides the process2 function to better control which files are
			 * downloaded and saved where.
			 */
			mapLogger.debug("Start process2() at " + timestamp);
			ArrayList<String> logFiles = new ArrayList<String>();
			//StageException eThrown = null; // set to true in case of excpeption in process()
			
			try {
				ArrayList<String> inputFiles = stage.downloadInputFiles(inputHDFSFiles);
				filesRead.increment(1);

				// Meta-data files where downloaded in setup()

				// Do the processing					
				ArrayList<String> outputFiles = stage.process(inputFiles, metaFiles, logFiles, timestamp);
				cmdsExecuted.increment(1);
								
				// Save output, log, and meta files in HDFS						
				stage.saveOutputFiles(outputFiles, timestamp);
				filesWritten.increment(outputFiles.size());
			} catch (StageException e) {
				mapLogger.warn("Stage exceution failed: ", e);
				cmdsFailed.increment(1);
				// Do not throw exception until log files have been saved and local directories 
				// has been cleaned
				//eThrown = e;
				
				// Accept some subtasks to fail
			}
			
			// Move all log files to task specific log directory, these are then saved in cleanup
			for (String src: logFiles) {
				String dst = OsPath.join(taskLogDir, OsPath.basename(src));
				if (OsPath.rename(src, dst) == false) {
					mapLogger.warn("Could not move log file to task specific output directory");
				}
			}

			// Cleanup after each map
			String[] dirs = {stage.stageLogDir, stage.stageInputDir, stage.stageMetaDir, stage.stageOutputDir, stage.stageTmpDir, taskMapredOutputDir};			
			// Delete and then re-create directory
			for (String d: dirs) {
				if (! OsPath.isdir(d)) {
					mapLogger.warn("Stage directory: " + d + " does not exist");				
				}
				else {
					if (OsPath.deleteAll(d) == false) {
						mapLogger.warn("Could not delete directory: " + d);
						throw new IOException("Cleanup failed: could not delete directory: " + d);
					}
				}
				OsPath.mkdir(d);
			}	
			
			//if (eThrown != null) {
			//	doCleanup();
				// Log files saved and cleanup done, so can throw exception
			//	throw new IOException(eThrown);
			//}
			
			mapLogger.debug("Process2() done at " + timestamp);
			filesProcessed.increment(1);
			
			// All the output files will be in the task specific output directory. 
			// They will be moved automatically id the task succeeds.		
		}
	}

	/**
	 * Create and execute MapReduce job
	 * 
	 * @param cargs command line arguments
	 * @return 0 on success, -1 of failure
	 */
	public int run(String[] cargs)  {
		Configuration conf = new Configuration();		
		String[] remainingArgs;
		try {
			remainingArgs = new GenericOptionsParser(conf, cargs).getRemainingArgs();
		} catch (IOException e2) {
			e2.printStackTrace();
			System.err.println("Could not parse arguments: " + e2);
			return -1;
		}
		
		if (parseArgs(conf, remainingArgs) == false) {			
			System.err.println("Invalid arguments " + cargs);
			return -1;
		}
		
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e1) {		
			jobLogger.fatal("Could not create FileSystem object: ", e1);			
			return -1;
		}
		
		/*
		 * Setup job
		 */						
		Job job;
		try {
			// Set memory limits
			// Note! must be done before creating job
			long maxTroilkattVMem = -1;
			long maxMapredVMem = -1;
			try {
				maxTroilkattVMem = Long.valueOf(confEget(conf, "troilkatt.soft.max.memory.mb"));
				maxMapredVMem = Long.valueOf(confEget(conf, "troilkatt.hard.max.memory.mb"));
			} catch (NumberFormatException e) {
				jobLogger.fatal("Could not read memory limits from configuration: ", e);
				return -1;
			} catch (IOException e) {
				jobLogger.fatal("Could not read memory limits from configuration: ", e);
				return -1;
			}
			
			setMemoryLimits(conf, maxMapredVMem - maxTroilkattVMem - 512, maxMapredVMem);
			
			job = new Job(conf, progName);
			
			job.setJarByClass(ExecuteStage.class);
			
			/* Setup mapper: use the Compress class*/		
			job.setMapperClass(ExecutePerFileMapper.class);

			/* Specify that no reducer should be used */
			job.setNumReduceTasks(0);
		   
		    // Do per file job configuration
		    perFileConfInit(conf, job);
		    
		    // Set input and output paths
		    if (setInputPaths(job) == 0) { // No input files
		    	return 0;
		    }
		    setOutputPath(hdfs, job);
		} catch (IOException e1) {
			jobLogger.fatal("Job setup failed: ", e1);
			return -1;
		} catch (StageInitException e) {
			jobLogger.fatal("Could not set output path: ", e);
			return -1;
		}
		
		
	    // Execute job and wait for completion
		try {
			return job.waitForCompletion(true) ? 0: -1;
		} catch (InterruptedException e) {
			jobLogger.fatal("Job execution failed: ", e);
			return -1;
		} catch (ClassNotFoundException e) {
			jobLogger.fatal("Job execution failed: ", e);
			return -1;
		} catch (IOException e) {
			jobLogger.fatal("Job execution failed: ", e);
			return -1;
		}	
	}

	/**
	 * Arguments: see documentation for run
	 */
	public static void main(String[] args) {			
		// Hadoop configuration (core-default.xml and core-site.xml must be in classpath)
		ExecuteStage o = new ExecuteStage();
		int exitCode = o.run(args);		
		System.exit(exitCode);	    	    
	}
}
