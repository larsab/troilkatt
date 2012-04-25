package edu.princeton.function.troilkatt.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * Extract meta data from a SOFT file and store it in a separate file where the .soft 
 * extension is changed with a .meta extension
 */
public class SplitSoft extends PerFile {
	enum LineCounters {
		LINES_READ,
		META_LINES_WRITTEN
	}
	
	/**
	 * Mapper class that gets as input a filename and outputs a filename. For each file
	 * it does the following:
	 * 1. Read in one line at a time (the files can be tens of gigabytes in size)
	 * 2. For all meta-data lines write them to a seperate file on the local filesystem
	 * 3. Copy the meta data file to a task specific directory in HDFS
	 * 4. Output the HDFS filename to the reducer
	 */
	public static class PerFileSplit extends PerFileMapper {		
		// Counters
		protected Counter linesRead;
		protected Counter linesWritten;
		
		/**
		 * Setup global variables. This function is called before map()
		 * @throws IOException 
		 */
		@Override
		public void setup(Context context) throws IOException {
			super.setup(context);			
			
			// Counters used to report progress and avoid a job being assumed to be crashed
			linesRead = context.getCounter(LineCounters.LINES_READ);
			linesWritten = context.getCounter(LineCounters.META_LINES_WRITTEN);
		}
		
		/**
		 * Do the mapping: 
		 * 1. Read a file from HDFS
		 * 2. Write meta data to a file on the local FS
		 * 
		 * The output files will be written to HDFS in the master cleanup function.
		 *  
		 * Note that this class needs to make sure output files are deleted from the output 
	     * directory in case of exceptions. 
		 * 
		 * @param key HDFS soft filename
		 * @param value always null since the SOFT files can be very large	
		 * @throws IOException 
		 */
		@Override
		public void map(Text key, BytesWritable value, Context context) throws IOException {						
			/*
			 * Open input stream
			 */
			String inputFilename = key.toString();
			String basename = tfs.getFilenameName(inputFilename);
			context.setStatus("Split: " + inputFilename);							
			BufferedReader lin = openBufferedReader(inputFilename);			
			if (lin == null) {
				mapLogger.fatal("Could not open input file: " + inputFilename);				
				return;
			}
						
			/*
			 *  Open output stream to a file on the local FS						
			 */
			String outputFilename = OsPath.join(taskOutputDir, basename + ".meta");
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outputFilename)));
			try {						
				System.out.println("Split file: " + basename);	
				context.setStatus("Split file: " + basename);
				
				// First line is always the soft filename the meta data was extracted from
				bw.write("!Soft_filename = " + OsPath.basename(key.toString()) + "\n");
				
				/*
				 * Read one line at a time and write meta-lines to an output file
				 */
				long lcnt = 0;
				String line;
				while ((line = lin.readLine()) != null) {					
					linesRead.increment(1);
					lcnt++;
					
					if (lcnt % 1000 == 0) {
						context.setStatus("Lines read = " + lcnt);
						bw.flush();
					}
					
					if ((line.charAt(0) == '!') || (line.charAt(0) == '^')) { // is meta line																		
						bw.write(line.toString());
						bw.write("\n");
						linesWritten.increment(1);
					}
				}
				bw.close();
			} catch (IOException e) {
				mapLogger.error("IOExcpetion: " + e.getMessage());					
				closeDeleteLocalBufferedWriter(bw, outputFilename);		
				return;
			} finally {
				lin.close();
			}
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
		String[] remainingArgs;
		try {
			remainingArgs = new GenericOptionsParser(conf, cargs).getRemainingArgs();
		} catch (IOException e2) {
			System.err.println("Could not parse arguments: IOException: " + e2.getMessage());
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
			jobLogger.fatal("Could not create FileSystem object: " + e1.toString());			
			return -1;
		}
		
		/*
		 * Setup job
		 */				
		Job job;
		try {
			job = new Job(conf, progName);
			job.setJarByClass(SplitSoft.class);

			/* Setup mapper */
			job.setMapperClass(PerFileSplit.class);	    				

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
			jobLogger.fatal("Job setup failed due to IOException: " + e1.getMessage());
			return -1;
		} catch (StageInitException e) {
			jobLogger.fatal("Could not initialize job: " + e.getMessage());
			return -1;
		}	
		
	    // Execute job and wait for completion
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
	 * Arguments: see documentation for run
	 */
	public static void main(String[] args) throws Exception {			
		SplitSoft o = new SplitSoft();
		int exitCode = o.run(args);		
		System.exit(exitCode);	    	    
	}
}
