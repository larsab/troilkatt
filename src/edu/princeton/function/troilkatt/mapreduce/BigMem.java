package edu.princeton.function.troilkatt.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class BigMem extends PerFile {
	enum AllocCounters {
		SUCCESS,
		OUT_OF_MEMORY
	}
	
	
	/**
	 * This is a dummy mapper that reads the input file into memory, or it starts a subprocess that
	 * reads the input file into memory. 
	 * 
	 * Command line arguments specify the memory limits. These are then set by JobClient during job startup
	 *
	 */
	public static class BigMemMapper extends PerFileMapper {
		protected Counter successCounter;
		protected Counter outOfMemoryCounter;
		
		/**
		 * Setup global variables. This function is called before map()
		 * @throws IOException 
		 */
		@Override
		public void setup(Context context) throws IOException {
			super.setup(context);			
			
			successCounter = context.getCounter(AllocCounters.SUCCESS);
			outOfMemoryCounter = context.getCounter(AllocCounters.OUT_OF_MEMORY);
		}
		
		/**
		 * Read the input file into memory.
		 * 
		 * @param key: HDFS filename
		 * @param value: always null since the input files can be very large	
		 */
		@Override
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			String inputFilename = key.toString();
			context.setStatus("Read: " + inputFilename);
			
			// Note a SoftReference is used to avoid OutOfMemoryError's	
						
			SoftReference<ArrayList<String>> linesSR = new SoftReference<ArrayList<String>>(new ArrayList<String>());
			
			BufferedReader bri = openBufferedReader(inputFilename);
			
			String line;
			while ((line = bri.readLine()) != null) {
				// Get soft reference first. This will fail if the JVM is out of memory
				ArrayList<String> lines = linesSR.get();
				if (lines == null) { // Out of memory
					outOfMemoryCounter.increment(1);
					context.setStatus("Out of memory: " + inputFilename);
					bri.close();
					return;
				}
				
				lines.add(line);
			}
			successCounter.increment(1);
			context.setStatus("Success: " + inputFilename);
			bri.close();						
		}		
	}
	
	/**
	 * Create and execute MapReduce job
	 * 
	 * @param cargs command line arguments
	 * @return 0 on success, -1 of failure
	 */
	public int run(String[] cargs) {
		Configuration conf = new Configuration();
		HBaseConfiguration.merge(conf, HBaseConfiguration.create()); // add Hbase configuration
		
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
			
		/*
		 * Setup job
		 */				
		Job job;
		try {
			// Set memory limits
			// Note! must be done before creating job
		    setMemoryLimits(conf);
			
			job = Job.getInstance(conf, progName);
			job.setJarByClass(BigMem.class);
			
			/* Setup mapper: use the Compress class*/
			job.setMapperClass(BigMemMapper.class);				

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
			jobLogger.fatal("Job setup failed: ",  e1);
			return -1;
		} catch (StageInitException e) {
			jobLogger.fatal("Could not initialize job: ", e);
			return -1;
		}			
		
	    // Execute job and wait for completion
		return this.waitForCompletionLogged(job);
	}

	/**
	 * @param args[0] file that was written in pipeline.Mapreduce.writeMapReduceArgsFile
	 */
	public static void main(String[] args) {			
		BigMem o = new BigMem();
		int exitCode = o.run(args);		
		System.exit(exitCode);	    	    
	}

}
