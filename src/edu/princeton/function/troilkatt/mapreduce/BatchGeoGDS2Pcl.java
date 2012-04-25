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
import edu.princeton.function.troilkatt.tools.GeoGDS2Pcl;
import edu.princeton.function.troilkatt.tools.ParseException;

public class BatchGeoGDS2Pcl extends PerFile {
	enum BatchCounters {
		FILES_READ,
		INVALID_INPUT_FILES,
		FILES_WRITTEN,		
		LINES_READ,
		LINES_WRITTEN,
		PARSER_EXCEPTIONS
	}
	
	/**
	 * Mapper class that uses a GeoGDS2Pcl parser to convert the file
	 */
	public static class GDSConvertMapper extends PerFileMapper {		
		// Counters
		protected Counter filesRead;
		protected Counter invalidInputFiles;
		protected Counter filesWritten;		
		//protected Counter linesRead;
		//protected Counter linesWritten;
		protected Counter parserExceptions;
		
		// Parser object
		private GeoGDS2Pcl parser;
		
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
			//linesRead = context.getCounter(BatchCounters.LINES_READ);
			//linesWritten = context.getCounter(BatchCounters.LINES_WRITTEN);
			parserExceptions = context.getCounter(BatchCounters.PARSER_EXCEPTIONS);			
			
			parser = new GeoGDS2Pcl();
		}
		
		/**
		 * Do the mapping: 
		 * 1. Read one line at a time from a file in HDFS
		 * 2. Parse the line
		 * 3. Write output to a file in HDFS
		 * 
		 * @param key HDFS soft filename
		 * @param value always null since the SOFT files can be very large	
		 * @throws IOException 
		 */
		@Override
		public void map(Text key, BytesWritable value, Context context) throws IOException {						
			filesRead.increment(1);
			// Reset parser since it is reused
			parser.reset();
			
			/*
			 * Open input stream
			 */
			String inputFilename = key.toString();
			context.setStatus("Convert: " + inputFilename);							
			BufferedReader lin = openBufferedReader(inputFilename);			
			if (lin == null) {
				mapLogger.fatal("Could not open input file: " + inputFilename);
				invalidInputFiles.increment(1);
				return;
			}
			
			/*
			 * Open output stream and write converted file
			 */
			String basename = tfs.getFilenameName(inputFilename);	
			String outputBasename = OsPath.replaceLastExtension(basename, "pcl");
			BufferedWriter bw = openBufferedWriter(outputBasename, compressionFormat, context);
			if (bw != null) {						
				try {
					parser.convert(lin, bw);
					bw.close();	
				} catch (ParseException e) {
					parserExceptions.increment(1);
					mapLogger.warn("Failed to convert file due to parser error: " + e.getMessage());
					closeDeleteBufferedWriter(bw, outputBasename, compressionFormat, context);
					return;
				}							
			}
			else { 				
				// Could not write directly to HDFS, so must fallback on local file system				
				String localFilename = OsPath.join(taskOutputDir, outputBasename);
				bw = null;
				try {
					bw = new BufferedWriter(new FileWriter(new File(localFilename)));
					parser.convert(lin, bw);
					bw.close();
				} catch (IOException e) {
					mapLogger.error("IOExcpetion: " + e.getMessage());					
					closeDeleteLocalBufferedWriter(bw, localFilename);	
					return;
				} catch (ParseException e) {
					parserExceptions.increment(1);
					mapLogger.warn("Failed to convert file due to parser error: " + e.getMessage());					
					closeDeleteLocalBufferedWriter(bw, localFilename);
					return;
				}				
				// All local output files will be written to HDFS in cleanup()
			} 
			lin.close();
			filesWritten.increment(1);			
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
			job.setJarByClass(BatchGeoGDS2Pcl.class);

			/* Setup mapper */
			job.setMapperClass(GDSConvertMapper.class);	    				

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
		BatchGeoGDS2Pcl o = new BatchGeoGDS2Pcl();
		int exitCode = o.run(args);		
		System.exit(exitCode);	    	    
	}

}
