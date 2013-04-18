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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.hbase.GeoMetaTableSchema;
import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.tools.FilenameUtils;
import edu.princeton.function.troilkatt.tools.PclMissingValues;

public class BatchPclMissingValues extends BatchPclCommon {
	/**
	 * Mapper class that uses a GeoGDS2Pcl parser to convert the file
	 */
	public static class MissingValuesMapper extends PclMapper {				
		/*
		 * Arguments
		 */
		protected float geneCutoff;
		protected int sampleCutoff;
		protected float datasetCutoff;
		// Optional arguments that are read from the GEO Meta table if they are not set
		protected String zeroAreMVsStr;
		protected String mvCutoffStr;
		protected boolean optArgsSpecified;
		
		
		// MissingValue tool (initialized in each map call)
		protected PclMissingValues converter;
		
		/**
		 * Setup global variables. This function is called before map()
		 * @throws IOException 
		 */
		@Override
		public void setup(Context context) throws IOException {
			super.setup(context);			
			
			// Parse arguments
			String[] args = TroilkattMapReduce.confEget(conf, "troilkatt.stage.args").split(" ");
			if (args.length < 3) {
				mapLogger.fatal("Invalid arguments: " + args);				
				throw new IOException("Invalid arguments: " + args);
			}
			try {
				geneCutoff = Float.valueOf(args[0]);
				sampleCutoff = Integer.valueOf(args[1]);
				datasetCutoff = Float.valueOf(args[2]);				
			} catch (NumberFormatException e) {
				mapLogger.fatal("Not a number", e);				
				throw new IOException("Invalid number format in arguments: " + args);
			}
			
			if (args.length == 5) {
				zeroAreMVsStr = args[3];
				mvCutoffStr = args[4];
				optArgsSpecified = true;
			}
			else {
				zeroAreMVsStr = null;
				mvCutoffStr = null;
				optArgsSpecified = false;
			}
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
			String inputFilename = key.toString();
			
			String gid = FilenameUtils.getDsetID(inputFilename);
			if (! optArgsSpecified) {
				zeroAreMVsStr = GeoMetaTableSchema.getInfoValue(metaTable, gid, "zerosAreMVs", mapLogger);
				if (zeroAreMVsStr == null) {
					mapLogger.fatal("Could not read meta data for: " + gid);
					errors.increment(1);
					return;
				}
			}
			boolean zerosAsMVs = zeroAreMVsStr.equals("1");
			
			if (! optArgsSpecified) {
				mvCutoffStr = GeoMetaTableSchema.getInfoValue(metaTable, gid, "cutoff", mapLogger);
				if (mvCutoffStr == null) {
					mapLogger.fatal("Could not read meta data for: " + gid);
					errors.increment(1);
					return;
				}
			}
			float mvCutoff = Float.NaN;
			// Need to catch the NumberFormatException since this value may NaN
			try {
				mvCutoff = Float.valueOf(mvCutoffStr);
			} catch (NumberFormatException e) {
				// Do nothing since this is expected for som datasets
				mapLogger.info("Missing value cutoff is: NaN (this is expected for some datasets)");
			}
			
			converter = new PclMissingValues(geneCutoff, sampleCutoff, datasetCutoff, zerosAsMVs, mvCutoff);
					
			/*
			 * Open input stream
			 */			
			context.setStatus("Convert: " + inputFilename);							
			BufferedReader bri = openBufferedReader(inputFilename);			
			if (bri == null) {
				mapLogger.fatal("Could not open input file: " + inputFilename);
				errors.increment(1);
				return;
			}
			
			/*
			 * Open output stream to local FS and write converted file
			 */
			String outputBasename = tfs.getFilenameName(inputFilename) + ".mv";			
			String localTmpFilename = OsPath.join(taskTmpDir, outputBasename); // In tmp dir			
			try {
				BufferedWriter bw = new BufferedWriter(new FileWriter(new File(localTmpFilename)));
				processFile(bri, bw, inputFilename);
				bw.close();
			} catch (IOException e) {
				mapLogger.error("Could not process file", e);					
				// files in tmp directory will automatically be deleted
				return;
			} 				
			if (! converter.tooManyMissingValues()) {
				// move to output directory where it will be written to HDFS in cleanup()
				String localFilename = OsPath.join(taskOutputDir, outputBasename);
				OsPath.rename(localTmpFilename, localFilename);
				filesWritten.increment(1);
			}
			// else too many missing values so the file remains in the tmp directory where it will
			// be deleted in cleanup()
			 
			bri.close();			
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
			String line;
			while ((line = lin.readLine()) != null) {
				String outputLine = converter.insertMissingValues(line);
				if (outputLine != null) {
					bw.write(outputLine);
				}
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
						
			job = new Job(conf, progName);
			job.setJarByClass(BatchPclMissingValues.class);

			/* Setup mapper */
			job.setMapperClass(MissingValuesMapper.class);	    				

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
		BatchPclMissingValues o = new BatchPclMissingValues();
		int exitCode = o.run(args);		
		System.exit(exitCode);	    	    
	}
}
