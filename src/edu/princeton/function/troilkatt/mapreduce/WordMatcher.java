package edu.princeton.function.troilkatt.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
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

/**
 * MapReduce job used for unit testing. It is a variant of Word Count, with the exception that
 * only words provided as command line arguments are counted and that the output is written to
 * one file per input word.
 * 
 * This job does not contain any Troilkatt code.
 */
public class WordMatcher {
	enum GeneCounters {
		LINES_READ,
		WORDS_READ,
		WORDS_FOUND
	}
	
	protected static Logger jobLogger;
	
	/*
	 * Arguments sent using the configuration file. These are all set in readMapReduceArgsFile().
	 */
	protected String loggingLevel;	
	protected ArrayList<String> inputFiles;
	protected String hdfsOutputDir;

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
		protected Logger mapLogger;

		/**
		 * Setup global variables. This function is called once per task before map()
		 */
		@Override
		public void setup(Context context) throws IOException {
			conf = context.getConfiguration();			
			jobID = context.getJobID().toString();
			taskAttemptID = context.getTaskAttemptID().toString();
			mapLogger = Logger.getLogger("troilkatt.wordmatcher-mapper");		
			
			// These will be written respectively to the stdout, stderr and progName.log files in $HADOOP_LOG_DIR/userlogs/<job-id>/<task-id>/
			System.out.println("Output test: MapReduce job: System.out.println: in mapper setup");
			System.err.println("Output test: MapReduce job: System.err.println: in mapper setup");
			
			//String logProperties = conf.get("log4j.properties.file");
			//PropertyConfigurator.configure(logProperties);   		
			mapLogger.fatal("Output test: MapReduce job: logger.fatal: in mapper setup");
						
			String[] argParts = conf.get("stage.args").split(" ");
			
			inputWords = new String[argParts.length];
			for (int i = 0; i < argParts.length; i++) {
				inputWords[i] = argParts[i].trim().toLowerCase();
				System.err.println("mapper input word: " + inputWords[i]);
			}

			linesRead = context.getCounter(GeneCounters.LINES_READ);
			wordsRead = context.getCounter(GeneCounters.WORDS_READ);
			wordsFound = context.getCounter(GeneCounters.WORDS_FOUND);
			
			System.out.println("Output test: MapReduce job: taskID = " + taskAttemptID);
			System.err.println("Output test: MapReduce job: taskID = " + taskAttemptID);
			mapLogger.fatal("Output test: MapReduce job: taskID = " + taskAttemptID);
		}

		/**
		 * Decstructor (also called once per task)
		 */
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("Output test: MapReduce job: System.out.println: in map cleanup");
			System.err.println("Output test: MapReduce job: System.err.println: in map cleanup");
			mapLogger.fatal("Output test: MapReduce job: logger.fatal: in map cleanup");
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
	
		protected Logger reduceLogger;

		/**
		 * Setup (called once per task)
		 */
		@Override
		public void setup(Context context) throws IOException {
			conf = context.getConfiguration();
			jobID = context.getJobID().toString();
			taskAttemptID = context.getTaskAttemptID().toString();	
			
			//String logProperties = conf.get("log4j.properties.file");
			//PropertyConfigurator.configure(logProperties); 
			reduceLogger = Logger.getLogger("troilkatt.wordmatcher-reducer");			
			
			System.out.println("Output test: MapReduce job: System.out.println: in reducer setup");
			System.err.println("Output test: MapReduce job: System.err.println: in reducer setup");
			reduceLogger.fatal("Output test: MapReduce job: logger.fatal: in reducer setup");			

			mos = new MultipleOutputs<Text, IntWritable>(context);
		}

		/**
		 * Destructor (also called once per task)
		 */
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("Output test: MapReduce job: System.out.println: in reducer cleanup");
			System.err.println("Output test: MapReduce job: System.err.println: in reducer cleanup");
			reduceLogger.fatal("Output test: MapReduce job: logger.fatal: in reducer cleanup");
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
			
			if (geneCount % 1 == 0) {
				System.out.println("Output test: throw");
				System.err.println("Output test: throw");
				reduceLogger.fatal("Output test: throw");
				throw new IOException("Word not found: " + key.toString());
			}

			String outputFile = key.toString();			
			mos.write(outputFile, key, new IntWritable(geneCount));		
		}
	}	

	/**                                                                                                                                                          
	 * Alternative run method that can be used without the Troilkatt arguments                                                                                   
	 *                                                                                                                                                           
	 * @param cargs command line arguments, which are: hdfsInputDir hdfsOutputDir log4j.properties word1 word2...                                                
	 * @return 0 on success, -1 of failure                                                                                                                       
	 */   
	public int run(String[] cargs) {                                                                                                                            
		Configuration conf = new Configuration();
		HBaseConfiguration.merge(conf, HBaseConfiguration.create());    
		
		String[] remainingArgs = null;;                                                                                                                      
		try {                                                                                                                                                
			remainingArgs = new GenericOptionsParser(conf, cargs).getRemainingArgs();                                                                    
		} catch (IOException e2) {                                                                                                                           
			e2.printStackTrace();                                                                                                                        
			System.err.println("Could not parse arguments: IOException: " + e2);                                                                         
			return -1;                                                                                                                                   
		}                                                                                                                                                    

		/* Check input arguments */                                                                                                                          
		if (remainingArgs.length <= 4) {                                                                                                                     
			System.err.println("At least four arguments are required (inputDir outputDir logProperties word1)");                                                
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
		conf.set("stage.args", stageArgs);
		
		//PropertyConfigurator.configure(logProperties);   
		//conf.set("log4j.properties.file", logProperties);
		jobLogger = Logger.getLogger("troilkatt.wordmatcher-jobclient");                                                                                

		conf.set("troilkatt.stage.args", stageArgs);                                                                                                         
		System.out.println("stageArgs: " + stageArgs);                                                                                                       

		/* Setup job */                                                                                                                                      
		Job job = null;                                                                                                                                      
		try {                                                                                                                                                
			job = Job.getInstance(conf, "wordmatcher");                                                                                                             
			job.setJarByClass(UnitTest.class);                                                                                                           
			FileInputFormat.setInputPaths(job, new Path(inputDir));                                                                                      
			FileOutputFormat.setOutputPath(job, new Path(outputDir));                                                                                    

		} catch (IOException e) {                                                                                                                            
			jobLogger.fatal("Job setup failed: ", e);                                                                                                    
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
			jobLogger.fatal("Job execution failed: Interrupt exception: ", e);                                                                           
			return -1;                                                                                                                                   
		} catch (ClassNotFoundException e) {                                                                                                                 
			jobLogger.fatal("Job execution failed: Class not found exception: ", e);                                                                     
			return -1;                                                                                                                                   
		} catch (IOException e) {                                                                                                                            
			jobLogger.fatal("Job execution failed: IOException: ", e);                                                                                   
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

		WordMatcher wc = new WordMatcher();

		// These lines go respectively to stage/log/mapreduce.output and stage/log/mapreduce.error
		System.out.println("Output test: MapReduce job: System.out.println: before running MapReduce job");
		System.err.println("Output test: MapReduce job: System.err.println: before running MapReduce job");

		// For starting with Troilkatt
		int exitCode = wc.run(args);		

		// These lines go respectively to stage/log/mapreduce.output and stage/log/mapreduce.error
		System.out.println("Output test: MapReduce job: System.out.println: after running MapReduce job");
		System.err.println("Output test: MapReduce job: System.err.println: after running MapReduce job");

		System.exit(exitCode);	    	    
	}
}
