package edu.princeton.function.troilkatt.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.tools.FilenameUtils;
import edu.princeton.function.troilkatt.tools.GeoGSE2Pcl;
import edu.princeton.function.troilkatt.tools.ParseException;

/**
 * Convert GEO Series SOFT file to the PCL format. 
 * 
 * The conversion consist of two stage:
 * -First BatchGeoGSESplit parses the SOFT file to identify the columns and fields
 * to use in the second stage, and it splits a multi-platform file into platform
 * specific SOFT files (by omitting all samples from other platforms).
 * -Second BathGeoGSE2Pcl uses the meta data output from the first to convert
 * the platform specific files from SOFT to PCL. 
 */
public class BatchGeoGSESplit extends PerFile {
	enum BatchCounters {
		FILES_READ,
		INVALID_INPUT_FILES,		
		FILES_WRITTEN,
		SER_FILES_WRITTEN,
		STAGE1_COMPLETE,		
		META_ERRORS,
		PARSER_EXCEPTIONS,
		PARSER_ERRORS,
		OUT_OF_MEMORY
	}

	/**
	 * Mapper class that uses a GeoGSEPcl parser to convert the file
	 */
	public static class GSEConvertMapper extends PerFileMapper {		
		// Counters
		protected Counter filesRead;
		protected Counter invalidInputFiles;		
		protected Counter filesWritten;
		protected Counter serFilesWritten;
		protected Counter stage1Complete;
		protected Counter metaErrors;
		protected Counter parserExceptions;		
		protected Counter parserErrors;
		protected Counter outOfMemory;

		// Parser object
		private GeoGSE2Pcl parser;

		/**
		 * Setup global variables. This function is called before map()
		 * @throws IOException 
		 */
		@Override
		public void setup(Context context) throws IOException {
			super.setup(context);			

			// Counters used to report progress and avoid a job being assumed to be crashed
			filesRead = context.getCounter(BatchCounters.FILES_READ);
			invalidInputFiles = context.getCounter(BatchCounters.INVALID_INPUT_FILES);			
			filesWritten = context.getCounter(BatchCounters.FILES_WRITTEN);
			serFilesWritten = context.getCounter(BatchCounters.SER_FILES_WRITTEN);
			stage1Complete = context.getCounter(BatchCounters.STAGE1_COMPLETE);
			metaErrors = context.getCounter(BatchCounters.META_ERRORS);
			parserExceptions = context.getCounter(BatchCounters.PARSER_EXCEPTIONS);
			parserErrors = context.getCounter(BatchCounters.PARSER_ERRORS);
			outOfMemory = context.getCounter(BatchCounters.OUT_OF_MEMORY);

			parser = new GeoGSE2Pcl();			
		}

		/**
		 * Do the mapping: 
		 * 1. Read one line at a time from a file in HDFS
		 * 2. Parse the line
		 * 3. When done write extracted meta-information to the Hbase table 
		 * 
		 * @param key HDFS soft filename
		 * @param value always null since the SOFT files can be very large	
		 * @throws IOException 
		 */
		@Override
		public void map(Text key, BytesWritable value, Context context) throws IOException {						
			String inputFilename = key.toString();
			context.setStatus("Split: " + inputFilename);
			String basename = tfs.getFilenameName(inputFilename);
			String dsetID = FilenameUtils.getDsetID(basename);

			filesRead.increment(1);

			/*
			 * Open input stream
			 */			
			BufferedReader br = openInputFile(inputFilename);
			if (br == null) {
				return;
			}			

			/*
			 * Do first stage to get column to use in second stage and split 
			 * the soft file into platform specific parts
			 */			
			context.setStatus("Parse: " + basename);
			String serFilename = OsPath.join(taskOutputDir, dsetID + ".stage1.ser");
			FileOutputStream serFile = null;
			
			// stage2 is to be run later so the stage1 data structures must be saved
			serFile = new FileOutputStream(serFilename);

			try {
				if (parser.stage1(br, serFile) == false) {
					parserExceptions.increment(1);
					// File could not be parsed
					serFile.close(); // close before deleting
					OsPath.delete(serFilename);									
				}
				else {
					serFilesWritten.increment(1);					
					serFile.close(); // always close
				}
			} catch (ParseException e) {
				parserExceptions.increment(1);
				serFile.close();
				OsPath.delete(serFilename);
				context.progress();
				mapLogger.error("ParseException: " + e.getMessage());
				br.close();
				return;
			} 			
			br.close();
			stage1Complete.increment(1);
			
			/*
			 * Split the input SOFT file by platforms and write parsed meta data 
			 * 
			 * This results in one soft file per platform with name: DSETID-PLATID.soft,
			 * and _one_ meta data file DSETID.ser
			 */
			ArrayList<String> pids = parser.getPlatformIDs();
			ArrayList<String> basenames = new ArrayList<String>();
			for (String pid: pids) {					
				String outputBasename = FilenameUtils.mergeDsetPlatIDs(dsetID, pid) + ".soft";
				context.setStatus("Write: " + outputBasename);

				// Re-open input file
				if ((br = openInputFile(inputFilename)) == null) {
					return;
				}

				// Attempt to open output stream to a HDFS file
				BufferedWriter bw = openBufferedWriter(outputBasename, compressionFormat, context);
				// Set if output cannot be directly written to HDFS
				String localFilename = null;
				try {					
					if (bw != null) {						
						parser.writeSoftPerPlatform(br, bw, pid);
						bw.close();
					}
					else { 	// bw == null	
						// Could not write directly to HDFS, so must fallback on local file system
						// All output files will be written in cleanup()
						localFilename = OsPath.join(taskOutputDir, outputBasename);					
						try {
							bw = new BufferedWriter(new FileWriter(new File(localFilename)));	
							parser.writeSoftPerPlatform(br, bw, pid);
							bw.close();
						} catch (IOException e) {
							mapLogger.error("IOExcpetion: " + e.getMessage());					
							closeDeleteLocalBufferedWriter(bw, localFilename);
							return;
						}				
					} // else if bw == null
					br.close();
					filesWritten.increment(1);
					basenames.add(outputBasename);
				} catch (ParseException e) {					
					mapLogger.fatal("Could not re-parse input file: " + inputFilename, e);
					if (localFilename == null) { // writing directly to HDFS
						closeDeleteBufferedWriter(bw, outputBasename, compressionFormat, context);
					}
					else {
						closeDeleteLocalBufferedWriter(bw, localFilename);
					}
					continue;
				} // try
			} // for each platform
		} // map

		/**
		 * Helper function to open the input file
		 * 
		 * @param inputFilename file to open
		 * @throws IOException 
		 */
		private BufferedReader openInputFile(String inputFilename) throws IOException {
			BufferedReader br = openBufferedReader(inputFilename);			
			if (br == null) {
				mapLogger.fatal("Could not open input file: " + inputFilename);
				invalidInputFiles.increment(1);		
			}
			return br;
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
			System.err.println("Error: Could not parse arguments: IOException: " + e2.getMessage());
			return -1;
		}

		if (parseArgs(conf, remainingArgs) == false) {				
			System.err.println("Error: Invalid arguments " + cargs);
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
			job.setJarByClass(BatchGeoGSESplit.class);

			/* Setup mapper */
			job.setMapperClass(GSEConvertMapper.class);	    				

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
		BatchGeoGSESplit o = new BatchGeoGSESplit();
		int exitCode = o.run(args);		
		System.exit(exitCode);	    	    
	}
}
