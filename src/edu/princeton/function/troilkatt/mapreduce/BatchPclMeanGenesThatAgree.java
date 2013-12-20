package edu.princeton.function.troilkatt.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.tools.PclMeanGenesThatAgree;

public class BatchPclMeanGenesThatAgree extends BatchPclCommon {	
	/**
	 * Mapper class that uses a GeoGDS2Pcl parser to convert the file
	 */
	public static class MeanGenesMapper extends PclMapper {				
		// Argument
		protected boolean writePDF;
		
		/**
		 * Setup global variables. This function is called before map()
		 * @throws IOException 
		 */
		@Override
		public void setup(Context context) throws IOException {
			super.setup(context);
				
			// Parse arguments
			writePDF = TroilkattMapReduce.confEget(conf, "troilkatt.stage.args").equals("1");					
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
			PclMeanGenesThatAgree meaner = new PclMeanGenesThatAgree();
			meaner.readDataFromPCL(lin);
			if (writePDF) {
				meaner.createPDFs(OsPath.join(taskLogDir, "pdf.log"));
			}
			else {
				meaner.createPDFs(null);
			}
			meaner.meanAllThatAgree();
			meaner.writeNewPclToFile(bw);
		}
		
		/**
		 * Helper function to get the output file basename
		 * 
		 * @param inputBasename basename of input file
		 * @return basename of output file
		 */
		@Override
		protected String getOutputBasename(String inputBasename) {
			return inputBasename + ".avg";	
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
			job.setJarByClass(BatchPclMeanGenesThatAgree.class);

			/* Setup mapper */
			job.setMapperClass(MeanGenesMapper.class);	    				

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
			jobLogger.fatal("Job setup failed: " + e1);
			return -1;
		} catch (StageInitException e) {
			jobLogger.fatal("Could not initialize job: " + e);
			return -1;
		}	
		
	    // Execute job and wait for completion
		return waitForCompletionLogged(job);
	}

	/**
	 * Arguments: see documentation for run
	 */
	public static void main(String[] args) throws Exception {			
		BatchPclMeanGenesThatAgree o = new BatchPclMeanGenesThatAgree();
		int exitCode = o.run(args);		
		System.exit(exitCode);	    	    
	}
}
