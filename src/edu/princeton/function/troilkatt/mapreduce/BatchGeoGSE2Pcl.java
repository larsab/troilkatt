package edu.princeton.function.troilkatt.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
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
public class BatchGeoGSE2Pcl extends PerFile {
	enum BatchCounters {
		FILES_READ,
		SER_FILES_READ,
		INVALID_INPUT_FILES,
		INVALID_SER_FILES,
		FILES_WRITTEN,
		STAGE2_COMPLETE,
		META_ERRORS,
		PARSER_EXCEPTIONS,
		OUT_OF_MEMORY
	}

	/**
	 * Mapper class that uses a GeoGSEPcl parser to convert the file
	 */
	public static class GSEConvertMapper extends PerFileMapper {		
		// Counters
		protected Counter filesRead;
		protected Counter serFilesRead;
		protected Counter invalidInputFiles;
		protected Counter invalidSerFiles;
		protected Counter filesWritten;
		protected Counter stage2Complete;
		protected Counter metaErrors;
		protected Counter parserExceptions;		
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
			serFilesRead = context.getCounter(BatchCounters.SER_FILES_READ);
			invalidInputFiles = context.getCounter(BatchCounters.INVALID_INPUT_FILES);
			invalidSerFiles = context.getCounter(BatchCounters.INVALID_SER_FILES);
			filesWritten = context.getCounter(BatchCounters.FILES_WRITTEN);
			stage2Complete = context.getCounter(BatchCounters.STAGE2_COMPLETE);
			metaErrors = context.getCounter(BatchCounters.META_ERRORS);
			parserExceptions = context.getCounter(BatchCounters.PARSER_EXCEPTIONS);
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
			context.setStatus("Convert: " + inputFilename);
			mapLogger.info("Convert: " + inputFilename);
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
			 * The stage1 output data structures must be read from the output file created in 
			 * stage 1
			 */
			// Construct stage1 output filename using the following rules
			// 1. it is in the same directory as the input soft file
			// 2. the name does contain the dsetID but not platformID
			// 3. it has the same compression and timestamp as the input file
			String serFilename = tfs.getFilenameDir(inputFilename) + "/" +  
					FilenameUtils.getDsetID(inputFilename, false) + ".stage1.ser." + 
					tfs.getFilenameTimestamp(inputFilename) + "." +
					tfs.getFilenameCompression(inputFilename);
			if (! tfs.isfile(serFilename)) {
				// Also try file that includes platform ID
				serFilename = tfs.getFilenameDir(inputFilename) + "/" +  
						FilenameUtils.getDsetID(inputFilename, true) + ".stage1.ser." + 
						tfs.getFilenameTimestamp(inputFilename) + "." +
						tfs.getFilenameCompression(inputFilename);
				
				if (! tfs.isfile(serFilename)) {
					mapLogger.fatal("File does not exist: " + serFilename);
					invalidSerFiles.increment(1);
					return;
				}
			}
			// serFilename is a valid file
			String localSerFilename = tfs.getFile(serFilename, taskTmpDir, taskTmpDir, taskLogDir);

			if (localSerFilename == null) {
				mapLogger.fatal("Could not download stage1 output file: " + serFilename);
				invalidSerFiles.increment(1);
				return;
			}

			FileInputStream fis = new FileInputStream(localSerFilename);
			try {
				parser.readStage1Results(fis);
			} catch (ClassNotFoundException e) {
				invalidInputFiles.increment(1);
				mapLogger.fatal(e.getStackTrace());
				mapLogger.fatal("Could not read serialized stage1 results");					
			}
			serFilesRead.increment(1);
												
			/*
			 *	Run stage 2 to create data structures holding the expression values per gene
			 */				
			context.setStatus("Stage2: " + basename);
			// Stage 2 processing is per platform				
			ArrayList<String> pids = parser.getPlatformIDs();
			// If stage1 was run previously the soft file is split per platform, after the meta
			// data has been calculated
			String inputPid = FilenameUtils.getPlatID(basename);
			if (inputPid != "") {
				// only run stage2 on the input files platform 				
				pids.clear();				
				pids.add(inputPid);
			}
			else {
				mapLogger.warn("Could not get platform ID for filename: " + basename);
			}

			for (String pid: pids) {
				String outputBasename = dsetID + ".pcl";

				if ((br = openInputFile(inputFilename)) == null) {
					return;
				}

				// Attempt to open output stream to a HDFS file							
				BufferedWriter bw = openBufferedWriter(outputBasename, compressionFormat, context);
				// Set if output cannot be directly written to HDFS
				String localFilename = null;
				try {
					if (bw != null) {												
						parser.stage2(br, bw, pid);	// parse and write output file
						bw.close();						
					}
					else { 	// bw == null	
						// Could not write directly to HDFS, so must fallback on local file system
						// All output files will be written in cleanup()
						localFilename = OsPath.join(taskOutputDir, outputBasename);					
						try {
							bw = new BufferedWriter(new FileWriter(new File(localFilename)));	
							parser.stage2(br, bw, pid); // parse and write output file
							bw.close();
						} catch (IOException e) {
							mapLogger.error("IOExcpetion: ", e);					
							closeDeleteLocalBufferedWriter(bw, localFilename);
							return;
						}
					} 		
				} catch (ParseException e1) {
					parserExceptions.increment(1);
					mapLogger.error("ParseException: ", e1);
					if (localFilename == null) { // writing directly to HDFS
						closeDeleteBufferedWriter(bw, outputBasename, compressionFormat, context);
					}
					else {
						closeDeleteLocalBufferedWriter(bw, localFilename);
					}
					return;
				} 
				filesWritten.increment(1);
			} // for each platform
			stage2Complete.increment(1);			
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
			// Set memory limits
			// Note! must be done before creating job
			setMemoryLimits(conf);
						
			job = new Job(conf, progName);
			job.setJarByClass(BatchGeoGSE2Pcl.class);

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
		BatchGeoGSE2Pcl o = new BatchGeoGSE2Pcl();
		int exitCode = o.run(args);		
		System.exit(exitCode);	    	    
	}
}
