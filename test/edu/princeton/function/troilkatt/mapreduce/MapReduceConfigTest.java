package edu.princeton.function.troilkatt.mapreduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import edu.princeton.function.troilkatt.fs.FSUtils;

/**
 * Make sure that a MapReduce system is configured properly by running a simple
 * test program.
 * 
 * The class is based on the WordCount "hello world" MapReduce program.
 * 
 * This class is written to be independent of Troilkatt so it copies a lot of
 * the TroilkatMapReduce code.
 * 
 * This class is started using 
 * 
 *   hadoop jar troilkatt.jar edu.princeton.function.troilkatt.mapreduce.MapReduceConfigTest
 */
public class MapReduceConfigTest {
	public static final String inputDir = "test/mapreduce/configTest/input";
	public static final String[] inputFiles = {inputDir + "/file1",
		inputDir + "/file2", inputDir + "/file3"};	
	public static final String outputDir = "test/mapreduce/configTest/output";		
	
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Logger mapLogger;

		@Override
		public void setup(Context context) throws IOException {
			// Do nothing
			System.err.println("In mapper setup");
			String taskAttemptID = context.getTaskAttemptID().toString();			
			mapLogger = Logger.getLogger("mapper-" + taskAttemptID);
			mapLogger.fatal("In map.setup");
			
			Configuration conf = context.getConfiguration();
			String maxMappers = conf.get("mapred.tasktracker.map.tasks.maximum");
			System.err.println("Max mapper tasks: " + maxMappers);
			long heapMaxSize = Runtime.getRuntime().maxMemory();
			System.err.println("Max heap size: " + heapMaxSize);
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			// Do nothing
			System.err.println("In mapper cleanup");
			mapLogger.fatal("In map.cleanup");		
			String[] lines = new String[1];
			lines[0] = "foobar\n";
			FSUtils.writeTextFile("/tmp/foobar", lines);
		}
		
		@Override
		public void finalize() {
			String[] lines = new String[1];
			lines[0] = "foobar\n";
			try {
				FSUtils.writeTextFile("/tmp/foobarbaz", lines);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			mapLogger.fatal("In map.map");
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());				 
				context.write(word, one);
			}
			throw new IOException("test");
		}
	}

	public static class WordCountReducer extends Reducer <Text, IntWritable, Text, IntWritable> {
		private Logger reduceLogger;
		
		@Override
		public void setup(Context context) throws IOException {
			// Do nothing
			System.err.println("In reducer setup");
			String taskAttemptID = context.getTaskAttemptID().toString();
			reduceLogger = Logger.getLogger("reducer-" + taskAttemptID);
			reduceLogger.fatal("In reducer.setup");
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			// Do nothing
			System.err.println("In reducer cleanup");
			reduceLogger.fatal("In reducer.cleaup");
		}

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			reduceLogger.fatal("In reducer.reduce");
			int sum = 0;
			for (IntWritable val: values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void prepareMapReduceJob() throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path inputPath = new Path(inputDir);
		Path outputPath = new Path(outputDir);
		
		// Create input path
		hdfs.mkdirs(inputPath);		
		FileStatus dirStatus = hdfs.getFileStatus(inputPath);
		if(! dirStatus.isDir()) {		
			System.err.println("Could not create input directory");
			System.exit(-1);
		}
		
		// Create input files if necessary
		Path localPath = new Path("/etc/fstab");
		for (String f: inputFiles) {
			Path path = new Path(f);
			if (! hdfs.isFile(path)) {
				hdfs.copyFromLocalFile(localPath, path);
			}
			if (! hdfs.isFile(path)) {
				System.err.println("Could not create input file: " + f);
				System.exit(-1);
			}	
		}
		
		// Make sure that output directory does not exist
		try {
			dirStatus = hdfs.getFileStatus(outputPath);
			if (dirStatus.isDir()) {
				hdfs.delete(outputPath, true);
			}
			dirStatus = hdfs.getFileStatus(outputPath);
			if (dirStatus.isDir()) {
				System.err.println("Could not delete output directory");
				System.exit(-1);
			}
		} catch (FileNotFoundException e) {			
		}
	}
	
	/**
	 * Create and execute the MapReduce job.
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public void run() throws IOException, InterruptedException, ClassNotFoundException {
		Logger jobLogger = Logger.getLogger("jobclient");
		jobLogger.fatal("Job logger initialized");
		
		Configuration conf = new Configuration();				
		Job job = new Job(conf, "map-reduce-config-test");
		job.setJarByClass(MapReduceConfigTest.class);
		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));		
		job.setMapperClass(WordCountMapper.class);	    				
		job.setReducerClass(WordCountReducer.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Execute job and wait for completion
		jobLogger.info("Starting MapReduce job");
		boolean rv = job.waitForCompletion(true);			
		if (rv != true) {
			System.err.println("MapReduce job failed");
			System.exit(-1);
		}
		
		jobLogger.fatal("Mapreduce Job done");
	}

	/**
	 * @param args none
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		/*
		 * Create and copy input files, and make sure output directory does not exist.
		 */
		MapReduceConfigTest.prepareMapReduceJob();

		/*
		 * Run MapReduce program
		 */
		MapReduceConfigTest o = new MapReduceConfigTest();
		o.run();
	}

}
