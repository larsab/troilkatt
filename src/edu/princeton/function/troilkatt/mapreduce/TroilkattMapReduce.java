package edu.princeton.function.troilkatt.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.LogTable;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import edu.princeton.function.troilkatt.pipeline.Stage;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.utils.Utils;

/**
 * Troilkatt MapReduce super-class: reads arguments from the arguments file and starts
 * the job.
 *  
 * Use PerFile as super-class for MapReduce programs that needs to process one file 
 * at a time.
 */
public class TroilkattMapReduce {			
	protected static Logger jobLogger;
	
	/*
	 * Arguments sent using the configuration file. These are all set in readMapReduceArgsFile().
	 */
	protected String loggingLevel;	
	protected ArrayList<String> inputFiles;
	protected String hdfsOutputDir;
	protected String progName; // stageNum-stageName
	
	/**
	 * Set the input files for the MapReduce job
	 * 
	 * @param job
	 * @return number of input files
	 * @throws IOException 
	 */
	public int setInputPaths(Job job) throws IOException {
		jobLogger.debug(String.format("%d input files", inputFiles.size()));		
		
		/* Setup HDFS input paths */		
		for (String tf: inputFiles) {
			FileInputFormat.addInputPath(job, new Path(tf));			
		}		
		
		return inputFiles.size();
	}
	
	/**
	 * Set MapReduce output path
	 * 
	 * @param hdfs
	 * @param job
	 * @throws IOException 
	 */
	public void setOutputPath(FileSystem hdfs, Job job) throws StageInitException, IOException {
		Path outputPath = new Path(hdfsOutputDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		if (hdfs.exists(outputPath)) {			
			jobLogger.fatal("Output path already exist: " + outputPath.toString());
			throw new StageInitException("Output path already exist: " + outputPath.toString());
		}
	}
	
	/**
	 * Set memory limits for tasks.
	 * 
	 * The memory limits are read from the configuration object: "troilkatt.soft.max.memory.mb" and
	 * "troilkatt.hard.max.memory.mb".
	 * 
	 * troilkatt.soft.max.memory.mb: maximum virtual memory in megabytes. If a task attempts to allocate more 
	 * than maxTroilkattVMem of memory it will be killed by troilkatt. The task will not be reported as 
	 * killed or failed to the mapreduce framework hence the job will not fail.
	 * troilkatt.hard.max.memory.mb: maximum virutal memory in megabytes. If maxMapredVmem < maxTroilkattVMem and
	 * a task attempts to allocate more than maxMapredVmem it will be killed by the mapreduce framework. 
	 * The job will fail if the same task is killed three times.
	 * 
	 * @param conf Hadoop configuration 
	 * @throws StageInitException 
	 */
	public void setMemoryLimits(Configuration conf) throws StageInitException {
		long maxTroilkattVMem = -1;
		long maxMapredVmem = -1;
		try {
			maxTroilkattVMem = Long.valueOf(confEget(conf, "troilkatt.soft.max.memory.mb"));
			maxMapredVmem = Long.valueOf(confEget(conf, "troilkatt.hard.max.memory.mb"));
		} catch (NumberFormatException e) {
			jobLogger.fatal("Could not read memory limits from configuration: ", e);
			throw new StageInitException("Could not read memory limits from configuration");
		} catch (IOException e) {
			jobLogger.fatal("Could not read memory limits from configuration: ", e);
			throw new StageInitException("Could not read memory limits from configuration");
		}
		
		// maximum Virtual Memory task-limit for each task of the job 
		conf.setLong("mapred.job.map.memory.mb", maxMapredVmem); // in MB		
		conf.setLong("mapred.job.reduce.memory.mb", maxMapredVmem); // in MB				
		conf.set("mapred.child.java.opts", "-Xmx" + maxTroilkattVMem + "m -XX:MaxPermSize=256m"); // in MB
		
		// Also set ulimit to kill tasks that use too much virtual memory
		//conf.setLong("mapred.child.ulimit", maxMapredVmem * 1024); // ulimit is in kilobytes
		//System.out.println("mapred.child.ulimit: " + maxMapredVmem * 1024 + "KB");	
		
		jobLogger.info("Soft memory limit: " + maxTroilkattVMem + "mb, hard: " + maxMapredVmem + "mb, heapsize: " + maxTroilkattVMem);
		System.out.println("Soft memory limit: " + maxTroilkattVMem + ", hard: " + maxMapredVmem + " heapsize: " + maxTroilkattVMem);
	}
	
	/**
	 * Parse common arguments
	 * 
	 * @param conf Hadoop configuration
	 * @param args list of command line arguments (see below)
	 *   args[0]: arguments file that was written in pipeline.Mapreduce.writeMapReduceArgsFile
	 * @return true if arguments where successfully parsed, false otherwise 
	 */
	protected boolean parseArgs(Configuration conf, String[] args) {
		if (args.length != 1) {
			return false;
		} 		
		
		try {
			readMapReduceArgsFile(conf, args[0]);
		} catch (StageInitException e) {
			System.err.println("Could not parse arguments file: " + e.getMessage());
			return false;
		}
				
		jobLogger = Logger.getLogger("troilkatt." + progName + "-jobclient");
		
		return true;
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
	protected void readMapReduceArgsFile(Configuration conf, String filename) throws StageInitException {
		inputFiles = new ArrayList<String>(); 
		try {
			BufferedReader ib = new BufferedReader(new FileReader(filename));
			
			// Note! read order of lines must match write order in MapReduce.writeMapReduceArgsFile
			conf.set("troilkatt.configuration.file", checkKeyGetVal(ib.readLine(), "configuration.file"));
			conf.set("troilkatt.pipeline.name", checkKeyGetVal(ib.readLine(), "pipeline.name"));			
			String stageName = checkKeyGetVal(ib.readLine(), "stage.name");
			progName =  stageName + "-mr";
			conf.set("troilkatt.stage.name", stageName);
			conf.set("troilkatt.stage.args", checkKeyGetVal(ib.readLine(), "stage.args"));
			hdfsOutputDir = checkKeyGetVal(ib.readLine(), "hdfs.output.dir");
			conf.set("troilkatt.hdfs.output.dir", hdfsOutputDir);
			conf.set("troilkatt.compression.format", checkKeyGetVal(ib.readLine(), "compression.format"));
			conf.set("troilkatt.storage.time", checkKeyGetValLong(ib.readLine(), "storage.time"));			
			conf.set("troilkatt.hdfs.meta.dir", checkKeyGetVal(ib.readLine(), "hdfs.meta.dir"));	
			conf.set("troilkatt.localfs.input.dir", checkKeyGetVal(ib.readLine(), "mapred.input.dir"));
			conf.set("troilkatt.localfs.output.dir", checkKeyGetVal(ib.readLine(), "mapred.output.dir"));
			conf.set("troilkatt.localfs.meta.dir", checkKeyGetVal(ib.readLine(), "mapred.meta.dir"));
			conf.set("troilkatt.globalfs.global-meta.dir", checkKeyGetVal(ib.readLine(), "mapred.global-meta.dir"));
			conf.set("troilkatt.localfs.tmp.dir", checkKeyGetVal(ib.readLine(), "mapred.tmp.dir"));								
			conf.set("troilkatt.jobclient.input.dir", checkKeyGetVal(ib.readLine(), "jobclient.input.dir"));
			conf.set("troilkatt.jobclient.output.dir", checkKeyGetVal(ib.readLine(), "jobclient.output.dir"));
			conf.set("troilkatt.jobclient.meta.dir", checkKeyGetVal(ib.readLine(), "jobclient.meta.dir"));
			conf.set("troilkatt.jobclient.global-meta.dir", checkKeyGetVal(ib.readLine(), "jobclient.global-meta.dir"));
			conf.set("troilkatt.jobclient.log.dir", checkKeyGetVal(ib.readLine(), "jobclient.log.dir"));
			conf.set("troilkatt.jobclient.tmp.dir", checkKeyGetVal(ib.readLine(), "jobclient.tmp.dir"));
			loggingLevel = checkKeyGetVal(ib.readLine(), "logging.level");
			conf.set("troilkatt.logging.level", loggingLevel);
			conf.set("troilkatt.timestamp", checkKeyGetValLong(ib.readLine(), "timestamp"));
			
			conf.set("troilkatt.soft.max.memory.mb", checkKeyGetValLong(ib.readLine(), "soft.max.memory.mb"));
			conf.set("troilkatt.hard.max.memory.mb", checkKeyGetValLong(ib.readLine(), "hard.max.memory.mb"));
			
			if (! "input.files.start".equals(ib.readLine())) {
				throw new StageInitException("input.files.start not found in arguments file");
			}
			
			while (true) {
				String str = ib.readLine();
				if (str == null) {
					throw new StageInitException("input.files.end not found in arguments file");
				}
				if (str.equals("input.files.end")) {
					break;
				}
				inputFiles.add(str.trim());
			}
			
			ib.close();
		} catch (IOException e1) {			
			throw new StageInitException("Could not read arguments file: " + e1.getMessage());
		}
	}

	/**
	 * Helper function to parse an "key = value" string.  
	 * 
	 * @param line to parse
	 * @param key expected key
	 * @return value
	 * throws StageInitException if the line does not contain the key 'key'
	 */
	public static String checkKeyGetVal(String line, String ekey) throws StageInitException {
		String[] parts = line.split("=");
		if (parts.length != 2) {
			throw new StageInitException("Invalid line in arguments file: " + line);
		}
		
		String key = parts[0].trim();
		String val = parts[1].trim();
		
		if (! key.equals(ekey)) {
			throw new StageInitException("Invalid key in arguments file, expected: " + ekey + " got: " + key);
		}
		
		return val;
	}
	
	/**
	 * Helper function to parse an "key = value" string, and to verify that the value
	 * is an integer  
	 * 
	 * @param line to parse
	 * @param key expected key
	 * @return value
	 * throws StageInitException if the line does not contain the key 'key', or the value
	 * is not a valid integer
	 */
	public static String checkKeyGetValInt(String line, String ekey) throws StageInitException {
		String val = checkKeyGetVal(line, ekey);
		try {
			Integer.valueOf(val);
		} catch (Exception e) {
			throw new StageInitException("Value for key " + ekey + " is not a valid integer: " + val);
		}
		return val;
	}
	
	/**
	 * Helper function to parse an "key = value" string, and to verify that the value
	 * is a long  
	 * 
	 * @param line to parse
	 * @param key expected key
	 * @return value
	 * throws StageInitException if the line does not contain the key 'key', or the value
	 * is not a valid long
	 */
	public static String checkKeyGetValLong(String line, String ekey) throws StageInitException {
		String val = checkKeyGetVal(line, ekey);
		try {
			Long.valueOf(val);
		} catch (Exception e) {
			throw new StageInitException("Value for key " + ekey + " is not a valid long: " + val);
		}
		return val;
	}
	
	/**
	 * Helper function that provides an error wrapped conf.get(). 
	 * 
	 * @param key: property key
	 * @return property value
	 * @throws IOException if the property does not exists in the configuration map
	 */
	public static String confEget(Configuration c, String key) throws IOException {			
		String val = c.get(key);
		if (val == null) {			
			throw new IOException("Key not in configuration map: " + key);
		}
		return val;
	}
	
	/**
	 * Copy  meta files from HDFS to local FS.
	 *  
	 * @param tfs TroilkattFS handle
	 * @param hdfsMetaDir directory with meta data
	 * @param stageMetaDir directory where downlaoded meta data is stored
	 * @param stageTmpDir directory for temporary local files
	 * @param stageLogDir directory for local log files
	 * @return list of downloaded filenames (local FS), or an empty list if the stage
	 * does not have any metadata files.
	 * @throws StageException if metafiles could not be downloaded
	 */
	public static ArrayList<String> downloadMetaFiles(TroilkattFS tfs,
			String hdfsMetaDir,
			String stageMetaDir, String stageTmpDir, String stageLogDir) throws StageException {
		try {
			String newestMetaDir = tfs.getNewestDir(hdfsMetaDir);
			if (newestMetaDir == null) {		
				System.err.println("No metafiles for stage");
				return new ArrayList<String>();
			}
			else {
				ArrayList<String> metaFiles = tfs.getDirFiles(OsPath.join(hdfsMetaDir, newestMetaDir), stageMetaDir, stageLogDir, stageTmpDir);		
				if (metaFiles == null) {					
					throw new StageException("Could not download meta file for stage");
				}
				return metaFiles;
			}
		} catch (IOException e) {			
			throw new StageException("Could not download meta file for stage");
		}
	}
	
	/** 
	 * Helper function to setup task specific logger. The log files will be written to 
	 * the HADOOP_LOG_DIR/userlogs/<job-id>/<task-id>/syslog file.
	 * 
	 * @param context MapReduce runtime provided configuration handle with troilkatt properties
	 * @param localLogDir task specific log directory on the local FS
	 * @return initialized logger
	 */
	public static Logger getTaskLogger(Configuration conf) throws IOException {	
		String stageName = TroilkattMapReduce.confEget(conf, "troilkatt.stage.name");			
		return Logger.getLogger("troilkatt" + stageName + "-mr");
	}

	/**
	 * Return task attempt specific logfile directory. ($HADOOP_LOG_DIR/userlogs/<job-id>/<task-id>)
	 * 
	 * @param jobID MapReduce job ID
	 * @param taskID MapReduce task attempt ID
	 * @return log directory for this task attempt
	 */
	public static String getTaskLocalLogDir(String jobID, String taskID) {
		return OsPath.join(System.getenv("HADOOP_LOG_DIR"), "userlogs/" + jobID + "/" + taskID);
	}

	/**
	 * Save task attempt specific logfiles
	 * 
	 * @param conf Configuration
	 * @param taskLogDir task attempt specific logfile directory
	 * @param taskAttemptID task attempt ID
	 * @param logTable LogTable
	 * @return none
	 * @throws IOException if files could not be saved
	 */
	public static void saveTaskLogFiles(Configuration conf, String taskLogDir, String taskAttemptID, LogTable logTable) throws IOException { 
		String[] dirFiles = OsPath.listdir(taskLogDir);		
		ArrayList<String> logFiles = Utils.array2list(dirFiles);						
		String stageName = TroilkattMapReduce.confEget(conf, "troilkatt.stage.name");	
		long timestamp = Long.valueOf(TroilkattMapReduce.confEget(conf, "troilkatt.timestamp"));

		try {
			System.err.println("Save logfiles for task attempt: " + taskAttemptID);
			logTable.putMapReduceLogFiles(stageName, timestamp, taskAttemptID, logFiles);
		} catch (StageException e) {
			System.err.println("WARNING: Could not save logfiles: " + e.getMessage());
		}		
	}
	
	/**
	 * Return task attempt specific metafile directory
	 * 
	 * Note! Files updated in, or written to, this directory are not saved persistently
	 * 
	 * @param conf Configuration object with "troilkatt." properties
	 * @param jobID MapReduce job ID
	 * @param taskID MapReduce task attempt ID
	 * @return meta directory for this task attempt, or null if the 
	 * @throws IOException 
	 */
	public static String getTaskLocalMetaDir(Configuration conf, String jobID, String taskID) throws IOException {
		String localMetaDir = confEget(conf, "troilkatt.localfs.meta.dir");
		String taskMetaDir = OsPath.join(localMetaDir,jobID + "-" + taskID);
		
		if (OsPath.mkdir(taskMetaDir) == false) {
			throw new IOException("Could not create local meta directory: " + taskMetaDir);
		}
		
		return taskMetaDir;
	}
	
	/**
	 * Return task attempt specific MapReduce tmp directory
	 * 
	 * @param conf Configuration object with "troilkatt." properties
	 * @param jobID MapReduce job ID
	 * @param taskID MapReduce task attempt ID
	 * @return meta directory for this task attempt
	 * @throws IOException 
	 */
	public static String getTaskLocalTmpDir(Configuration conf, String jobID, String taskID) throws IOException {
		String localTmpDir = confEget(conf, "troilkatt.localfs.tmp.dir");
		String taskTmpDir = OsPath.join(localTmpDir, jobID + "-" + taskID);
		
		if (OsPath.mkdir(taskTmpDir) == false) {
			throw new IOException("Could not create local tmp directory: " + taskTmpDir);
		}
		
		return taskTmpDir;
	}
	
	/**
	 * Return task attempt specific MapReduce input directory
	 * 
	 * @param conf Configuration object with "troilkatt." properties
	 * @param jobID MapReduce job ID
	 * @param taskID MapReduce task attempt ID
	 * @return meta directory for this task attempt
	 * @throws IOException 
	 */
	public static String getTaskLocalInputDir(Configuration conf, String jobID, String taskID) throws IOException {
		String localInputDir = confEget(conf, "troilkatt.localfs.input.dir");
		String taskInputDir = OsPath.join(localInputDir, jobID + "-" + taskID);		
		
		if (OsPath.mkdir(taskInputDir) == false) {
			throw new IOException("Could not create local input directory: " + taskInputDir);
		}
		
		return taskInputDir;
	}
	
	/**
	 * Return task attempt specific MapReduce input directory
	 * 
	 * @param conf Configuration object with "troilkatt." properties
	 * @param jobID MapReduce job ID
	 * @param taskID MapReduce task attempt ID
	 * @return meta directory for this task attempt
	 * @throws IOException 
	 */
	public static String getTaskLocalOutputDir(Configuration conf, String jobID, String taskID) throws IOException {
		String localOutputDir = confEget(conf, "troilkatt.localfs.output.dir");
		String taskOutputDir = OsPath.join(localOutputDir, jobID + "-" + taskID);
		
		if (OsPath.mkdir(taskOutputDir) == false) {
			throw new IOException("Could not create local output directory: " + taskOutputDir);
		}
		
		return taskOutputDir;
	}
	
	/**
	 * Save task specific output files to HDFS
	 * 
	 * @param tfs TroilkattFS
	 * @param conf Configuration
	 * @param taskOutputDir local FS directory with task specific output files to save
	 * @param taskTmpDir task specific tmp directory
	 * @param taskLogDir task specific log directory
	 * @param compressionFormat compression format to use on HDFS output files
	 * @param timestamp timestamp to add to HDFS output files
	 * 
	 * @throws IOException 
	 */	
	public static ArrayList<String> saveTaskOutputFiles(TroilkattFS tfs, Configuration conf, 
			String taskOutputDir, String taskTmpDir, String taskLogDir, String compressionFormat, long timestamp) throws IOException {
		String hdfsOutputDir = getTaskHDFSOutputDir(conf);
		
		ArrayList<String> hdfsFiles= new ArrayList<String>();
		
		 if (hdfsOutputDir == null) {
			 throw new RuntimeException("saveOutputFiles called for a stage where output directory is not set");
		 }
		 
		 String[] localFiles = OsPath.listdirR(taskOutputDir);
		 if (localFiles.length > 0) {		
			 // Save output files to the output directory specified in the 
			 for (String f: localFiles) {
				 String hdfsName = tfs.putLocalFile(f, hdfsOutputDir, taskTmpDir, taskLogDir, compressionFormat); 
				 if (hdfsName == null) {
					 throw new IOException("Could not copy output file to HDFS: " + f);
				 }
				 hdfsFiles.add(hdfsName);
			 }
		 }		 
		 
		 return hdfsFiles;
	}
	
	/**
	 * Get task specific HDFS output directory. Each task should write their files to this directory.
	 * The files while then be moved automatically by the MapReduce framework upon task completion.
	 * 
	 * Note! The output directory will only be task specific if the OutputCommitter is FileOutputCommitter.
	 * Otherwise, all tasks will share the same output directory as specified by setOutputPath
	 * 
	 * @param job JobContext
	 * @return task specific output directory
	 */ 
	public static String getTaskHDFSOutputDir(JobContext job) {
		Path p = FileOutputFormat.getOutputPath(job);
		return p.toString();		
	}
	
	/**
	 * Get task specific output directory. Each task should write their files to this directory.
	 * The files while then be moved automatically by the MapReduce framework upon task completion.
	 * 
	 * Note! The output directory will only be task specific if the OutputCommitter is FileOutputCommitter.
	 * Otherwise, all tasks will share the same output directory as specified by setOutputPath
	 * 
	 * @param conf Hadoop Configuration
	 * @return task specific output directory
	 * @throws IOException 
	 */ 
	public static String getTaskHDFSOutputDir(Configuration conf) throws IOException {
		return confEget(conf, "mapred.work.output.dir");		
	}
	
	/**
	 * Helper function to execute and job and wait for completion, including logging of
	 * exceptions
	 * 
	 * @param job to execute
	 * @return 0 on success, -1 of failure
	 */
	public int waitForCompletionLogged(Job job) {
		try {
			return job.waitForCompletion(true) ? 0: -1;
		} catch (InterruptedException e) {
			jobLogger.fatal("Interrupt exception: " + e.toString());
			return -1;
		} catch (ClassNotFoundException e) {
			jobLogger.fatal("Class not found exception: " + e.toString());
			return -1;
		} catch (IOException e) {
			jobLogger.fatal("Job execution failed: IOException: " + e.toString());
			return -1;
		}
	}
	
	/**
	 * Merge and return the Hadoop configuration file with the HBase configuration file.
	 * 
	 * @return Hadoop configuration object with all Hbase entries added
	 */
	public Configuration getMergedConfiguration() {
		Configuration hbConf = HBaseConfiguration.create();
		Configuration conf = new Configuration();
		
		Iterator<Entry<String, String>> it = hbConf.iterator();
		while (it.hasNext()) {
			Entry<String, String> entry = it.next();
			String key = entry.getKey();
			String value = entry.getValue();
			conf.set(key, value);
		}
		
		return conf;
	}
	
	/**
	 * Replace TROILKATT. substrings with per process variables.
	 * 
	 * Note that TROILKAT.FILE* symbods are not replaced
	 *
	 * @param argsStr string that contains TROILKATT substrings to be replaced  
	 * @param conf Configuration object with "troilkatt." properties
	 * @param jobID MapReduce job ID
	 * @param taskAttemptID MapReduce task attempt ID
	 * @param prop TroilkattProperties
	 * @param log optional log4j logger. Set to null if errors should not be logged.
	 * @return args string with TROILKATT substrings replaced 
	 * @throws TroilkattPropertiesException 
	 * @throws IOException 
	 */
	protected static String setTroilkattSymbols(String argsStr,
			Configuration conf, String jobID, String taskAttemptID,
			TroilkattProperties prop, Logger log) 
					throws TroilkattPropertiesException, IOException {		
		// Task specific
		String newStr = argsStr.replace("TROILKATT.INPUT_DIR", 
				TroilkattMapReduce.getTaskLocalInputDir(conf, jobID, taskAttemptID));
		newStr = newStr.replace("TROILKATT.OUTPUT_DIR", 
				TroilkattMapReduce.getTaskLocalOutputDir(conf, jobID, taskAttemptID));
		newStr = newStr.replace("TROILKATT.LOG_DIR", 
				TroilkattMapReduce.getTaskLocalLogDir(jobID, taskAttemptID));
		newStr = newStr.replace("TROILKATT.META_DIR", 
				TroilkattMapReduce.getTaskLocalMetaDir(conf, jobID, taskAttemptID));
		newStr = newStr.replace("TROILKATT.TMP_DIR", 
				TroilkattMapReduce.getTaskLocalTmpDir(conf, jobID, taskAttemptID));
		
		// Set global and command line symbols
		newStr = Stage.setCommonTroilkattSymbols(newStr, prop, log);

		return newStr;
	}	  
}
