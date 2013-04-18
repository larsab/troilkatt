package edu.princeton.function.troilkatt.mapreduce;

import java.io.IOException;
import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.util.GenericOptionsParser;

import edu.princeton.function.troilkatt.PipelineException;
import edu.princeton.function.troilkatt.fs.LogTableHbase;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * For all genes count the number of files containing the gene. This is a variant of 
 * the "Hello world" word counter MapReduce application.
 * 
 * It outputs a list of (gene-id, count) tuples
 */
public class GeneCounter extends TroilkattMapReduce {
	enum GeneCounters {
		LINES_READ,
		OFFSET_ZERO,
		INVALID_LINES,
		EMPTY_GENE_NAMES,
		MISMATCHING_NAMES,
		GENE_NAMES
	}
	
	/**
	 * Mapper class that counts the number of genes
	 */
	public static class GeneCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		// Maximum line size: can be small since genes are in first column
		static final int MAX_LINE_SIZE = 1024;			
		private final IntWritable one = new IntWritable(1);
		 
		/*
		 * All global variables are initialized in setup() 
		 */	
		protected Configuration conf;
		// Global variables necessary for setting up and saving log files
		protected String taskAttemptID;
		protected String taskLogDir;
		protected Logger mapLogger;
		protected LogTableHbase logTable;
		
		// Counters
		protected Counter linesRead;
		protected Counter offsetZero;
		protected Counter invalidLines;
		protected Counter emptyGeneNames;
		protected Counter mismatchingNames;
		protected Counter geneNames;

		/**
		 * Setup global variables. This function is called once per task before map()
		 */
		@Override
		public void setup(Context context) throws IOException {
			conf = context.getConfiguration();			
			String pipelineName = TroilkattMapReduce.confEget(conf, "troilkatt.pipeline.name");
			
			String taskAttemptID = context.getTaskAttemptID().toString();
			taskLogDir = TroilkattMapReduce.getTaskLocalLogDir(context.getJobID().toString(), taskAttemptID);
			mapLogger = TroilkattMapReduce.getTaskLogger(conf);
			
			try {
				logTable = new LogTableHbase(pipelineName);
			} catch (PipelineException e) {
				mapLogger.fatal("Could not open log table: ", e);
				throw new IOException("Could not create logTable object: " + e);
			}
									
			linesRead = context.getCounter(GeneCounters.LINES_READ);
			offsetZero = context.getCounter(GeneCounters.OFFSET_ZERO);
			invalidLines = context.getCounter(GeneCounters.INVALID_LINES);
			emptyGeneNames = context.getCounter(GeneCounters.EMPTY_GENE_NAMES);
			mismatchingNames = context.getCounter(GeneCounters.MISMATCHING_NAMES);
			geneNames = context.getCounter(GeneCounters.GENE_NAMES);
		}
		
		/**
		 * Cleanup function that is called once at the end of the task
		 */
		@Override
		protected void cleanup(Context context) throws IOException {
			TroilkattMapReduce.saveTaskLogFiles(conf, taskLogDir, taskAttemptID, logTable);
		}
		
		/**
		 * Do the mapping: 
		 * 1. Read a line from a file in HDFS
		 * 2. For each row (excluding headers) output <gene-name, 1>		 
		 * 
		 * @param key byte offset of line in file
		 * @param value a line
		 * @param context MapReduce context supplied by the runtime system
		 */
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Counters used to report progress			
				
			linesRead.increment(1);
			
			String line = value.toString();
			String[] cols = line.split("\t");
		
			long fileOffset = key.get();			
			if (fileOffset == 0) { // ignore first line
				offsetZero.increment(1);
				return;
			}
					
			if (cols.length < 4) { // Invalid line format
				invalidLines.increment(1);
				return;
			}
			
			String name1 = cols[0].toUpperCase();
			String name2 = cols[1].toUpperCase();
			
			if (name1.isEmpty() && name2.isEmpty()) {
				emptyGeneNames.increment(1);
				return;
			}
			else if (name1.isEmpty() && (! name2.isEmpty())) {
				name1 = name2;
			}
			
			if (! name1.equals(name2)) {
				mismatchingNames.increment(1);				
			}
			
			geneNames.increment(1);
			// Write (gene, 1) tuple
			context.write(new Text(name1), one);
		}

		
	}
		
	/**
	 * Reducer class that counts the number of files a specific gene is in and creates 
	 * a new gene count file.
	 */
	public static class GeneCounterReducer extends Reducer <Text, IntWritable, Text, IntWritable> {
		/*
		 * All global variables are set in setup()
		 */
		protected Configuration conf;
		// Global variables necessary for setting up and saving log files
		protected String taskAttemptID;
		protected String taskLogDir;
		protected LogTableHbase logTable;
		protected Logger reduceLogger;
		
		/**
		 * This function is called once at the start of the task
		 */
		@Override
		public void setup(Context context)  throws IOException {
			conf = context.getConfiguration();
			String pipelineName = TroilkattMapReduce.confEget(conf, "troilkatt.pipeline.name");
			
			String taskAttemptID = context.getTaskAttemptID().toString();
			taskLogDir = TroilkattMapReduce.getTaskLocalLogDir(context.getJobID().toString(), taskAttemptID);
			reduceLogger = TroilkattMapReduce.getTaskLogger(conf);
			
			try {
				logTable = new LogTableHbase(pipelineName);
			} catch (PipelineException e) {
				reduceLogger.fatal("Could not create logTable object: ", e);
				throw new IOException("Could not create logTable object: " + e);
			}			
		}
		
		/**
		 * Cleanup function that is called once at the end of the task
		 */
		@Override
		protected void cleanup(Context context) throws IOException {
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
			int geneCount = 0;
			
			for (IntWritable val: values) {
				geneCount += val.get();
			}
			    			
			context.write(key, new IntWritable(geneCount));		
		}			
	}
	
	/**
	 * Create and execute MapReduce job
	 * 
	 * @param cargs command line arguments
	 * @return 0 on success, -1 of failure
	 * @throws IOException
	 * @throws StageInitException 
	 * @throws StageException 
	 */
	public int run(String[] cargs) {
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
			System.err.println("Inavlid arguments " + cargs);
			return -1;
		}			
		
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e1) {		
			jobLogger.fatal("Could not create FileSystem object: ", e1);			
			return -1;
		}
	
		/* Setup job */				
		Job job;
		try {
			job = new Job(conf, progName);
			
			job.setJarByClass(GeneCounter.class);
			
			/* Setup input and output paths */
			if (setInputPaths(job) == 0) { // No input files
		    	return 0;
		    }
		    setOutputPath(hdfs, job);
			
			/* Setup mapper */
			job.setMapperClass(GeneCounterMapper.class);	    				

			/* Setup reducer */
			job.setCombinerClass(GeneCounterReducer.class);
			job.setReducerClass(GeneCounterReducer.class);		
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
		} catch (IOException e1) {
			jobLogger.fatal("Job setup failed due to: ", e1);
			return -1;
		} catch (StageInitException e) {
			jobLogger.fatal("Could not initialize job: ", e);
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
	 * Command line arguments: none
	 */
	public static void main(String[] args) {			
		GeneCounter o = new GeneCounter();
		int exitCode = o.run(args);		
		System.exit(exitCode);	    	    
	}

}
