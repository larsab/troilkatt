package edu.princeton.function.troilkatt.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.tools.PclMapGeneNames;

/**
 * Map gene names to a common namespace
 */
public class BatchPclMapGeneNames extends BatchPclCommon {
	
	
	/**
	 * Mapper 
	 */
	public static class GeneNamesMapper extends PclMapper {				
		// Parser
		protected PclMapGeneNames mapper;
		
		/**
		 * Setup global variables. This function is called before map()
		 * @throws IOException 
		 */
		@Override
		public void setup(Context context) throws IOException {
			super.setup(context);			
			
			// Parse arguments
			String mapFilename = TroilkattMapReduce.confEget(conf, "troilkatt.stage.args");
			try {
				mapFilename = TroilkattMapReduce.setTroilkattSymbols(mapFilename, 
						conf, jobID, taskAttemptID, troilkattProperties, mapLogger);
			} catch (TroilkattPropertiesException e) {
				mapLogger.fatal("Could not set troilkatt symbols in args string", e);
				throw new IOException("Invalid troilkatt symbol in arguments");
			}
			mapper = new PclMapGeneNames();
			mapper.addMappings(mapFilename);					
		}
		
		/**
		 * Helper function to read rows from the input file, process each row, and write the row 
		 * to the output file
		 * 
		 * @param lin initialized BufferedReader
		 * @param bw initialized BufferedWriter
		 * @param inputFilename input filename
		 * @throws IOException 
		 */
		@Override
		protected void processFile(BufferedReader lin, BufferedWriter bw,
				String inputFilename) throws IOException {
			mapper.mapFile(lin, bw);
		}
		
		/**
		 * Helper function to get the output file basename
		 * 
		 * @param inputBasename basename of input file
		 * @return basename of output file
		 */
		@Override
		protected String getOutputBasename(String inputBasename) {
			return inputBasename + ".map";	
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
			setMemoryLimits(conf);
						
			job = Job.getInstance(conf, progName);
			job.setJarByClass(BatchPclMapGeneNames.class);

			/* Setup mapper */
			job.setMapperClass(GeneNamesMapper.class);	    				

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
			jobLogger.fatal("Could not initialize job: ", e);
			return -1;
		}	
		
	    // Execute job and wait for completion
		return waitForCompletionLogged(job);
	}

	/**
	 * Arguments: see documentation for run
	 */
	public static void main(String[] args) throws Exception {			
		BatchPclMapGeneNames o = new BatchPclMapGeneNames();
		int exitCode = o.run(args);		
		System.exit(exitCode);	    	    
	}
}
