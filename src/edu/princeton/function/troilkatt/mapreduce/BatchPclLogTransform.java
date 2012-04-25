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
import edu.princeton.function.troilkatt.tools.PclLogTransform;

/**
 * Log transform PCL files that have not already been transformed
 */
public class BatchPclLogTransform extends BatchPclCommon {
	
	/**
	 * Mapper
	 */
	public static class LogTransformMapper extends PclMapper {				
		
		/**
		 * Helper function to read rows from the input file, process each row, and write the row 
		 * to the output file
		 * 
		 * @param br initialized BufferedReader
		 * @param bw initialized BufferedWriter
		 * @param inputFilename input filename
		 * @throws IOException 
		 */
		@Override
		protected void processFile(BufferedReader br, BufferedWriter bw,
				String inputFilename) throws IOException {
			String gid = FilenameUtils.getDsetID(inputFilename);
			String loggedStr = GeoMetaTableSchema.getInfoValue(metaTable, gid, "logged", mapLogger);
			if (loggedStr == null) {
				mapLogger.fatal("Could not read meta data for: " + gid);
				errors.increment(1);
				return;
			}
			boolean logged = loggedStr.equals("1");
			if (! logged) {			
				PclLogTransform.process(br, bw);
			}
			else {
				copy(br, bw);
			}
		}
		
		/**
		 * Helper function to get the output file basename
		 * 
		 * @param inputBasename basename of input file
		 * @return basename of output file
		 */
		@Override
		protected String getOutputBasename(String inputBasename) {
			return inputBasename + ".log";	
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
			job.setJarByClass(BatchPclLogTransform.class);

			/* Setup mapper */
			job.setMapperClass(LogTransformMapper.class);	    				

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
		BatchPclLogTransform o = new BatchPclLogTransform();
		int exitCode = o.run(args);		
		System.exit(exitCode);	    	    
	}
}
