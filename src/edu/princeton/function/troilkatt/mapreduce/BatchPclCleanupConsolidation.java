package edu.princeton.function.troilkatt.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.princeton.function.troilkatt.hbase.GeoMetaTableSchema;
import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.tools.FilenameUtils;
import edu.princeton.function.troilkatt.tools.PclCleanupConsolidation;

public class BatchPclCleanupConsolidation extends BatchPclCommon {	
	/**
	 * Mapper class for final cleanup
	 */
	public static class FinalMapper extends PclMapper {				
		/**
		 * Setup global variables. This function is called before map()
		 * @throws IOException 
		 */
		@Override
		public void setup(Context context) throws IOException {
			super.setup(context);									
		}
		
		/**
		 * Helper function to read rows from the input file, process each row, and write the row 
		 * to the output file
		 * 
		 * @param lin initialized BufferedReader
		 * @param bw initialized BufferedWriter
		 * @throws IOException 
		 */
		@Override
		protected void processFile(BufferedReader lin, BufferedWriter bw,
				String inputFilename) throws IOException {
			String gid = FilenameUtils.getDsetID(inputFilename);
			// Read info data from GEO meta table
			String logged = GeoMetaTableSchema.getInfoValue(metaTable, gid, "logged", mapLogger);
			if (logged == null) {
				mapLogger.warn("Logged not calculated for file: " + inputFilename);
				errors.increment(1);
				return;
			}
			boolean logTransformed = logged.equals("1");
			
			// cleaner tool
			PclCleanupConsolidation cleaner = new PclCleanupConsolidation(logTransformed);
			cleaner.process(lin, bw);
		}
		
		/**
		 * Helper function to get the output file basename
		 * 
		 * @param inputBasename basename of input file
		 * @return basename of output file
		 */
		@Override
		protected String getOutputBasename(String inputBasename) {
			String parts[] = inputBasename.split("\\."); 
			return  parts[0] + ".final.pcl";	
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
			// Set memory limits
			// Note! must be done before creating job
			setMemoryLimits(conf);
						
			job = new Job(conf, progName);
			job.setJarByClass(BatchPclCleanupConsolidation.class);

			/* Setup mapper */
			job.setMapperClass(FinalMapper.class);	    				

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
		return waitForCompletionLogged(job);
	}

	/**
	 * Arguments: see documentation for run
	 */
	public static void main(String[] args) throws Exception {			
		BatchPclCleanupConsolidation o = new BatchPclCleanupConsolidation();
		int exitCode = o.run(args);		
		System.exit(exitCode);	    	    
	}
}
