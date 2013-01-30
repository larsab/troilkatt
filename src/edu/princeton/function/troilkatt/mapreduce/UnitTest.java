package edu.princeton.function.troilkatt.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.princeton.function.troilkatt.PipelineException;
import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.LogTableHbase;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * MapReduce job used for unit testing. It is a variant of Word Count, with the exception that
 * only words provided as command line arguments are counted and that the output is written to
 * one file per input word.
 */
public class UnitTest extends TroilkattMapReduce {
	enum GeneCounters {
		LINES_READ,
		WORDS_READ,
		WORDS_FOUND
	}

	/**
	 * Mapper class that counts the number of words that match the input words.
	 */
	public static class WordMatcherMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		// Maximum line size: can be small since genes are in first column
		static final int MAX_LINE_SIZE = 65536;			 // 64KB
		private final IntWritable one = new IntWritable(1);

		/*
		 * All global variables are initialized in setup() 
		 */	
		protected Configuration conf;		
		// all in lower case
		protected String[] inputWords;

		// Counters used to report progress
		protected Counter linesRead;
		protected Counter wordsRead;
		protected Counter wordsFound;
		
		// Task specific local variables
		protected String jobID;
		protected String taskAttemptID;
		protected String taskInputDir;
		protected String taskOutputDir;
		protected String taskLogDir;
		protected String taskMetaDir;
		protected String taskTmpDir;
		
		protected Logger mapLogger;
		protected LogTableHbase logTable;

		/**
		 * Setup global variables. This function is called once per task before map()
		 */
		@Override
		public void setup(Context context) throws IOException {
			conf = context.getConfiguration();
			String pipelineName = TroilkattMapReduce.confEget(conf, "troilkatt.pipeline.name");
			try {
				logTable = new LogTableHbase(pipelineName);
			} catch (PipelineException e) {
				throw new IOException("Could not create logTable object: " + e.getMessage());
			}
			
			jobID = context.getJobID().toString();
			taskAttemptID = context.getTaskAttemptID().toString();
			taskLogDir = TroilkattMapReduce.getTaskLocalLogDir(jobID, taskAttemptID);
			taskInputDir = TroilkattMapReduce.getTaskLocalInputDir(conf, jobID, taskAttemptID);
			taskOutputDir = TroilkattMapReduce.getTaskLocalOutputDir(conf, jobID, taskAttemptID);
			mapLogger = TroilkattMapReduce.getTaskLogger(conf);			
			
			// These will be written respectively to the stdout, stderr and progName.log files in $HADOOP_LOG_DIR/userlogs/<job-id>/<task-id>/
			System.out.println("Output test: MapReduce job: System.out.println: in mapper setup");
			System.err.println("Output test: MapReduce job: System.err.println: in mapper setup");
			mapLogger.fatal("Output test: MapReduce job: logger.fatal: in mapper setup");
						
			String[] argParts = TroilkattMapReduce.confEget(conf, "troilkatt.stage.args").split(" ");
			if (argParts.length == 0) {
				throw new IOException("Could not read input words from configuration file");
			}

			inputWords = new String[argParts.length];
			for (int i = 0; i < argParts.length; i++) {
				inputWords[i] = argParts[i].trim().toLowerCase();
				System.err.println("mapper input word: " + inputWords[i]);
			}

			linesRead = context.getCounter(GeneCounters.LINES_READ);
			wordsRead = context.getCounter(GeneCounters.WORDS_READ);
			wordsFound = context.getCounter(GeneCounters.WORDS_FOUND);		
			
			// Check metafile download
			taskMetaDir = TroilkattMapReduce.getTaskLocalMetaDir(conf, jobID, taskAttemptID);
			taskTmpDir = TroilkattMapReduce.getTaskLocalTmpDir(conf, jobID, taskAttemptID);					
			FileSystem hdfs = null;
			try {
				hdfs = FileSystem.get(conf);
			} catch (IOException e1) {		
				mapLogger.fatal("Could not create FileSystem object: " + e1.toString());			
				throw new IOException("Could not create FileSystem object: " + e1.toString());
			}
			TroilkattHDFS tfs = new TroilkattHDFS(hdfs);
			try {
				ArrayList<String> metaFiles = downloadMetaFiles(tfs, 
						confEget(conf, "troilkatt.hdfs.meta.dir"),
						taskMetaDir,
						taskTmpDir,
						taskLogDir);
				boolean isCorrect = false;
				for (String m: metaFiles) {
					String basename = OsPath.basename(m);
					if (basename.equals("maptest")) {
						String[] lines = FSUtils.readTextFile(m);
						if (lines[0].contains("success")) {
							isCorrect = true;
						}
					}
				}
				
				if (! isCorrect) {
					mapLogger.fatal("Metafile content mismatch");
					throw new IOException("Metafile content mismatch");
				}
			} catch (StageException e) {
				mapLogger.fatal("Could not download meta files to: " + taskMetaDir);
				throw new IOException("Could not download meta files to: " + taskMetaDir);
			}		
			
			// Attempt to write to tmp, log, and input directories
			String[] randomLines = {"foo", "bar", "baz"};
			FSUtils.writeTextFile(OsPath.join(taskTmpDir, "foo.txt"), randomLines);
			FSUtils.writeTextFile(OsPath.join(taskLogDir, "foo.txt"), randomLines);
			FSUtils.writeTextFile(OsPath.join(taskInputDir, "foo.txt"), randomLines);
		}

		/**
		 * Deconstructor (also called once per task)
		 */
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("Output test: MapReduce job: System.out.println: in map cleanup");
			System.err.println("Output test: MapReduce job: System.err.println: in map cleanup");
			mapLogger.fatal("Output test: MapReduce job: logger.fatal: in map cleanup");
			
			OsPath.deleteAll(taskMetaDir);
			OsPath.deleteAll(taskInputDir);
			OsPath.deleteAll(taskOutputDir);
			OsPath.deleteAll(taskTmpDir);
			
			TroilkattMapReduce.saveTaskLogFiles(conf, taskLogDir, taskAttemptID, logTable);
		}
		

		/**
		 * Do the mapping: 
		 * 1. Read a line from a file in HDFS
		 * 2. For each word in the line compare with the list of inputWords, and output <word, 1> for
		 * all matches	 
		 * 
		 * @param key byte offset of line in file
		 * @param value a line
		 * @param context MapReduce context supplied by the runtime system
		 */
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Will generate thousands of lines of output (very slow)
			//System.out.println("Output test: MapReduce job: System.out.println: in mapper map");
			//System.err.println("Output test: MapReduce job: System.err.println: in mapper map");
			//mapLogger.fatal("Output test: MapReduce job: logger.fatal: in mapper setup");
						
			linesRead.increment(1);

			String line = value.toString().trim();
			line = line.replace("\t", " ");
			line = line.replace("|", " ");
			String[] words = line.split(" ");

			wordsRead.increment(words.length);

			for (String w: words) {
				w = w.toLowerCase();
				for (String y: inputWords) {
					if (w.equals(y)) {
						context.write(new Text(y), one);
						wordsFound.increment(1);
					}
				}
			}
		}
	}

	/**
	 * Reducer class that writes the number of words found to a file named 'word'.cnt
	 */
	public static class WordWritterReducer extends Reducer <Text, IntWritable, Text, IntWritable> {
		protected MultipleOutputs<Text, IntWritable> mos;
		
		/*
		 * All global variables are set in setup()
		 */
		protected Configuration conf;
		
		// Task specific local variables
		protected String jobID;
		protected String taskAttemptID;
		protected String taskInputDir;
		protected String taskOutputDir;
		protected String taskLogDir;
		protected String taskMetaDir;
		protected String taskTmpDir;
		
		protected LogTableHbase logTable;
		protected Logger reduceLogger;

		/**
		 * Setup (called once per task)
		 */
		@Override
		public void setup(Context context) throws IOException {
			conf = context.getConfiguration();
			String pipelineName = TroilkattMapReduce.confEget(conf, "troilkatt.pipeline.name");
			try {
				logTable = new LogTableHbase(pipelineName);
			} catch (PipelineException e) {
				throw new IOException("Could not create logTable object: " + e.getMessage());
			}
			taskAttemptID = context.getTaskAttemptID().toString();
			taskLogDir = TroilkattMapReduce.getTaskLocalLogDir(context.getJobID().toString(), taskAttemptID);
			reduceLogger = TroilkattMapReduce.getTaskLogger(conf);			
			
			System.out.println("Output test: MapReduce job: System.out.println: in reducer setup");
			System.err.println("Output test: MapReduce job: System.err.println: in reducer setup");
			reduceLogger.fatal("Output test: MapReduce job: logger.fatal: in reducer setup");			

			mos = new MultipleOutputs<Text, IntWritable>(context);
			
			// Check metafile download
			jobID = context.getJobID().toString();
			taskAttemptID = context.getTaskAttemptID().toString();
			taskLogDir = TroilkattMapReduce.getTaskLocalLogDir(jobID, taskAttemptID);
			taskInputDir = TroilkattMapReduce.getTaskLocalInputDir(conf, jobID, taskAttemptID);
			taskOutputDir = TroilkattMapReduce.getTaskLocalOutputDir(conf, jobID, taskAttemptID);
			taskMetaDir = TroilkattMapReduce.getTaskLocalMetaDir(conf, jobID, taskAttemptID);
			taskTmpDir = TroilkattMapReduce.getTaskLocalTmpDir(conf, jobID, taskAttemptID);					
			FileSystem hdfs = null;
			try {
				hdfs = FileSystem.get(conf);
			} catch (IOException e1) {		
				reduceLogger.fatal("Could not create FileSystem object: " + e1.toString());			
				throw new IOException("Could not create FileSystem object: " + e1.toString());
			}
			TroilkattHDFS tfs = new TroilkattHDFS(hdfs);
			try {
				ArrayList<String> metaFiles = downloadMetaFiles(tfs, 
						confEget(conf, "troilkatt.hdfs.meta.dir"),
						taskMetaDir,
						taskTmpDir,
						taskLogDir);
				boolean isCorrect = false;
				for (String m: metaFiles) {
					String basename = OsPath.basename(m);
					if (basename.equals("reducetest")) {
						String[] lines = FSUtils.readTextFile(m);
						if (lines[0].contains("success")) {
							isCorrect = true;
						}
					}
				}
				
				if (! isCorrect) {
					reduceLogger.fatal("Metafile content mismatch");
					throw new IOException("Metafile content mismatch");
				}
			} catch (StageException e) {
				reduceLogger.fatal("Could not download meta files to:" + taskMetaDir);
				throw new IOException("Could not download meta files to: " + taskMetaDir);
			}
			
			// Attempt to write to tmp, log, and input directories
			String[] randomLines = {"foo", "bar", "baz"};
			FSUtils.writeTextFile(OsPath.join(taskTmpDir, "foo.txt"), randomLines);
			FSUtils.writeTextFile(OsPath.join(taskLogDir, "foo.txt"), randomLines);
			FSUtils.writeTextFile(OsPath.join(taskInputDir, "foo.txt"), randomLines);
		}

		/**
		 * Deconstructor (also called once per task)
		 */
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("Output test: MapReduce job: System.out.println: in reducer cleanup");
			System.err.println("Output test: MapReduce job: System.err.println: in reducer cleanup");
			reduceLogger.fatal("Output test: MapReduce job: logger.fatal: in reducer cleanup");

			OsPath.deleteAll(taskMetaDir);
			OsPath.deleteAll(taskInputDir);
			OsPath.deleteAll(taskOutputDir);
			OsPath.deleteAll(taskTmpDir);
			
			mos.close();
			
			TroilkattMapReduce.saveTaskLogFiles(conf, taskLogDir, taskAttemptID, logTable);
		}

		/**
		 *  Do the reduce
		 *  
		 *  @param key gene name
		 *  @param values collection of gene counts for the gene
		 *  @param context MapReduce context supplied by the runtime system
		 */
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			System.out.println("Output test: MapReduce job: System.out.println: in reducer reduce");
			System.err.println("Output test: MapReduce job: System.err.println: in reducer reduce");
			reduceLogger.fatal("Output test: MapReduce job: logger.fatal: in reducer reduce");

			int geneCount = 0;

			for (IntWritable val: values) {
				geneCount += val.get();
			}

			String outputFile = key.toString();			
			mos.write(outputFile, key, new IntWritable(geneCount));		
		}
	}
	
	/**
	 * Return a list of files in the "filelist" metafile
	 * 
	 * @param list of meta files
	 * @return list of files in the "filelist" metafile
	 * @throws IOException 
	 */
	public static String[] getMetafileList(ArrayList<String> metaFiles) throws IOException {
		String filelistFilename = null;		
		for (String f: metaFiles) {				
			if (f.endsWith("filelist")) {
				filelistFilename = f;
				break;
			}
		}

		if (filelistFilename == null) {
			jobLogger.fatal("filelist meta-data file not found.");
			return null;
		}

		return FSUtils.readTextFile(filelistFilename);
	}
	
	public static boolean isFileInMetaList(String[] metafileList, String f) {
		boolean fileFound = false;
		String b = OsPath.basename(f);
		for (String g: metafileList) {
			if (b.equals(OsPath.basename(g))) {
				fileFound = true;
				break;
			}
		}

		return fileFound;
	}

	/**
	 * Alternative run method that can be used without the Troilkatt arguments
	 * 
	 * @param cargs command line arguments, which are: hdfsInputDir hdfsOutputDir log4j.properties word1 word2...
	 * @return 0 on success, -1 of failure
	 */
	public int run2(String[] cargs) {
		Configuration conf = new Configuration();		
		String[] remainingArgs = null;;
		try {
			remainingArgs = new GenericOptionsParser(conf, cargs).getRemainingArgs();
		} catch (IOException e2) {
			System.err.println("Could not parse arguments: IOException: " + e2.getMessage());
			return -1;
		}

		/* Check input arguments */
		if (remainingArgs.length <= 4) {
			System.err.println("At least four arguments are required (inputDir outputDir logDir word1)");
			return -1;
		}

		String inputDir = remainingArgs[0];
		String outputDir = remainingArgs[1];
		String logProperties = remainingArgs[2];
		loggingLevel = "finest";
		String[] inputWords = new String[remainingArgs.length - 3];
		String stageArgs = null;
		for (int i = 0; i < inputWords.length; i++) {
			inputWords[i] = remainingArgs[3 + i].toLowerCase();
			if (i == 0) {
				stageArgs = inputWords[i];
			}
			else {
				stageArgs = stageArgs + " " + inputWords[i];
			}
		}

		Troilkatt.setupLogging(logProperties);		
		progName = "unit-test";
		jobLogger = Logger.getLogger("troilkatt." + progName + "-jobclient");

		conf.set("troilkatt.stage.args", stageArgs);
		System.out.println("stageArgs: " + stageArgs);


		/* Setup job */		
		Job job = null;
		try {
			job = new Job(conf, progName);
			job.setJarByClass(UnitTest.class);

			FileInputFormat.setInputPaths(job, new Path(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));

		} catch (IOException e) {
			jobLogger.fatal("Job setup failed due to IOException: " + e.getMessage());
			return -1;
		}

		/* Setup mapper */
		job.setMapperClass(WordMatcherMapper.class);	    				

		/* Setup reducer */		
		job.setReducerClass(WordWritterReducer.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		/* Setup multiple named output paths */
		for (String y: inputWords) {
			MultipleOutputs.addNamedOutput(job, y.trim().toLowerCase(), TextOutputFormat.class,
					Text.class, LongWritable.class);
		}

		// Execute job and wait for completion
		try {
			return job.waitForCompletion(true) ? 0: -1;
		} catch (InterruptedException e) {
			jobLogger.fatal("Job execution failed: Interrupt exception: " + e.toString());
			return -1;
		} catch (ClassNotFoundException e) {
			jobLogger.fatal("Job execution failed: Class not found exception: " + e.toString());
			return -1;
		} catch (IOException e) {
			jobLogger.fatal("Job execution failed: IOException: " + e.toString());
			return -1;
		}
	}


	/**
	 * Create and execute the MapReduce job.
	 * 
	 * This program is run in the context of the "Troilkatt" user, while the mappers and reducers are run
	 * in the context of the "mapred" user.
	 * 
	 * @param cargs command line arguments
	 * @return 0 on success, -1 of failure. In addition the input arguments file is augmented with 
	 * return values, including the JobID of the task
	 */
	public int run(String[] cargs) {
		// Note! logger not set up yet
		Configuration conf = new Configuration();		
		String[] remainingArgs = null;;
		try {
			remainingArgs = new GenericOptionsParser(conf, cargs).getRemainingArgs();
		} catch (IOException e2) {
			System.err.println("Could not parse arguments: IOException: ");
			return -1;
		}

		// These lines go respectively to stage/log/mapreduce.output and stage/log/mapreduce.error
		System.out.println("Output test: MapReduce job: System.out.println: before setting up loger");
		System.err.println("Output test: MapReduce job: System.err.println: before setting up logger");

		if (parseArgs(conf, remainingArgs) == false) {			
			System.err.print("Invalid arguments:");
			for (String s: remainingArgs) {
				System.err.print(" " + s);
			}
			System.err.println("");
			return -1;
		}	

		// Logger is now set up
		// These lines go respectively to stage/log/mapreduce.output and stage/log/mapreduce.error
		System.out.println("Output test: MapReduce job: System.out.println: after setting up loger");
		System.err.println("Output test: MapReduce job: System.err.println: after setting up logger");
		// This line goes to mapreduce.error and 5-mapreduce-unittest-mr-job.log
		jobLogger.fatal("Output test: MapReduce job: logger.fatal: after setting up logger");

		/* Check input arguments */
		String[] inputWords;
		try {
			inputWords = TroilkattMapReduce.confEget(conf, "troilkatt.stage.args").split(" ");
		} catch (IOException e2) {
			jobLogger.fatal("Parameter troilkatt.stage.args not in configuratiuon file.");			
			return -1;
		}
		if (inputWords.length == 0) {
			jobLogger.fatal("No input words specified");
			return -1;
		}

		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e1) {		
			jobLogger.fatal("Could not create FileSystem object: " + e1.toString());			
			return -1;
		}

		/* Verify that list of input files matches list in meta-data files */
		try {
			TroilkattHDFS tfs = new TroilkattHDFS(hdfs);
			String localMetaDir = confEget(conf, "troilkatt.jobclient.meta.dir");
			System.err.println("localMetaDir = " + localMetaDir);
			ArrayList<String> metaFiles = downloadMetaFiles(tfs, 
					confEget(conf, "troilkatt.hdfs.meta.dir"),
					localMetaDir,
					confEget(conf, "troilkatt.jobclient.tmp.dir"),
					confEget(conf, "troilkatt.jobclient.log.dir"));
			String[] metafileList = getMetafileList(metaFiles);
			
			for (String f: inputFiles) {
				if (! isFileInMetaList(metafileList, f)) {
					jobLogger.fatal("Input file " + f + " not found in metafile");
					return -1;
				}				
			}
		} catch (StageException e) {
			jobLogger.fatal("Could not read arguments from configuration file: " + e.getMessage());
			return -1;
		} catch (IOException e) {
			jobLogger.fatal("Could not meta-data file: " + e.getMessage());
			return -1;
		}
		
		/* Switch local FS directories to 
		String mapreduceDir;
		try {
			mapreduceDir = troilkattProperties.get("troilkatt.localfs.mapreduce.dir");
		} catch (TroilkattPropertiesException e) {
			jobLogger.fatal("Invalid properies file: " + e.getMessage());
			throw new StageException("Could not create input arguments file");
		}

		/* Setup job */		
		Job job = null;
		try {
			job = new Job(conf, progName);
			job.setJarByClass(UnitTest.class);

			/* Setup input and output paths */
			if (setInputPaths(job) == 0) { // No input files
				jobLogger.warn("No input files");
				return 0;
			}
			setOutputPath(hdfs, job);
		} catch (IOException e) {
			jobLogger.fatal("Job setup failed due to IOException: " + e.getMessage());
			return -1;
		} catch (StageInitException e) {
			jobLogger.fatal("Could not set output path: " + e.getMessage());
			return -1;
		}

		/* Setup mapper */
		job.setMapperClass(WordMatcherMapper.class);	    				

		/* Setup reducer */		
		job.setReducerClass(WordWritterReducer.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		/* Setup multiple named output paths */
		for (String y: inputWords) {
			String w = y.trim().toLowerCase();			
			MultipleOutputs.addNamedOutput(job, w, TextOutputFormat.class, Text.class, LongWritable.class);
		}
		
		// Execute job and wait for completion
		try {		
			boolean rv = job.waitForCompletion(true);
			System.err.println("Job ID: " + job.getJobID());
			
			if (rv == true) {
				return 0;
			}
			else {
				return -1;
			}			
		} catch (InterruptedException e) {
			jobLogger.fatal("Job execution failed: Interrupt exception: " + e.toString());
			return -1;
		} catch (ClassNotFoundException e) {
			jobLogger.fatal("Job execution failed: Class not found exception: " + e.toString());
			return -1;
		} catch (IOException e) {
			jobLogger.fatal("Job execution failed: IOException: " + e.toString());
			return -1;
		}
	}

	/**
	 * Arguments: see documentation for run
	 * 
	 * To run from command line without Mapreduce:
	 * 1. Uncomment "o.run2()" line below
	 * 2. Create the troilkatt.jar file by exporting a jar
	 * 3. Run using the command: hadoop jar <PATH>/troilkatt.jar edu.princeton.function.troilkatt.mapreduce.UnitTest hdfsInputDir hdfsOutputDir localLogDir word1 word2...wordN
	 * (alternatively hadoop jar $TROILKATT_JAR edu.princeton.function.troilkatt.mapreduce.UnitTest inputDir outputDir word1 word2...wordN)
	 */
	public static void main(String[] args) {
		// These lines go respectively to stage/log/mapreduce.output and stage/log/mapreduce.error
		System.out.println("Output test: MapReduce job: System.out.println: in main function before creating UnitTest object");
		System.err.println("Output test: MapReduce job: System.err.println: in main function before creating UnitTest object");

		UnitTest o = new UnitTest();

		// These lines go respectively to stage/log/mapreduce.output and stage/log/mapreduce.error
		System.out.println("Output test: MapReduce job: System.out.println: before running MapReduce job");
		System.err.println("Output test: MapReduce job: System.err.println: before running MapReduce job");

		// For starting with Troilkatt
		int exitCode = o.run(args);		
		// For starting without Troilkatt
		//int exitCode = o.run2(args);

		// These lines go respectively to stage/log/mapreduce.output and stage/log/mapreduce.error
		System.out.println("Output test: MapReduce job: System.out.println: after running MapReduce job");
		System.err.println("Output test: MapReduce job: System.err.println: after running MapReduce job");

		System.exit(exitCode);	    	    
	}

}
