package edu.princeton.function.troilkatt.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.tools.FilenameUtils;
import edu.princeton.function.troilkatt.tools.GeoGSMOverlap;


public class BatchPclRemoveOverlapping extends PerFile {
	enum BatchCounters {
		FILES_READ,	
		ROWS_READ,
		ROWS_WRITTEN,
		COLUMNS_READ,		
		FILES_WRITTEN,		
		FILES_DELETED,
		SAMPLES_DELETED,
		READ_ERRORS
	}

	/**
	 * Mapper class 
	 */
	public static class RemoveOverlapMapper extends PerFileMapper {		
		// Counters
		protected Counter filesRead;		
		protected Counter rowsRead;
		protected Counter rowsWritten;
		protected Counter columnsRead;		
		protected Counter filesWritten;
		protected Counter filesDeleted;
		protected Counter samplesDeleted;
		protected Counter readErrors;

		// The file that contains the dataset/series and samples to delete
		protected String filename;

		// Datasets and samples to be deleted
		protected ArrayList<String> deletedDatasets = new ArrayList<String>();
		protected HashMap<String, String[]> deletedSamples = new HashMap<String, String[]>();

		/**
		 * Setup global variables. This function is called before map()
		 * @throws IOException 
		 */
		@Override
		public void setup(Context context) throws IOException {
			super.setup(context);			

			// Counters used to report progress and avoid a job being assumed to be crashed
			filesRead = context.getCounter(BatchCounters.FILES_READ);			
			rowsRead = context.getCounter(BatchCounters.ROWS_READ);
			rowsWritten = context.getCounter(BatchCounters.ROWS_WRITTEN);
			columnsRead = context.getCounter(BatchCounters.COLUMNS_READ);			
			filesWritten = context.getCounter(BatchCounters.FILES_WRITTEN);
			filesDeleted = context.getCounter(BatchCounters.FILES_DELETED);
			readErrors = context.getCounter(BatchCounters.READ_ERRORS);
			samplesDeleted = context.getCounter(BatchCounters.SAMPLES_DELETED);

			String stageArgs = TroilkattMapReduce.confEget(conf, "troilkatt.stage.args");
			try {
				filename = TroilkattMapReduce.setTroilkattSymbols(stageArgs, 
						conf, jobID, taskAttemptID, troilkattProperties, mapLogger);
			} catch (TroilkattPropertiesException e) {
				mapLogger.fatal("Invalid properties file", e);				
				throw new IOException("Could not read the properties file");
			}
			
			GeoGSMOverlap.readOverlapFile(filename, deletedDatasets, deletedSamples, mapLogger);
		}

		/**
		 * Do the mapping: remove datasets and series that contain overlapping samples.
		 * 
		 * @param key HDFS pcl filename
		 * @param value always null since the pcl files can be very large	
		 * @throws IOException 
		 */
		@Override
		public void map(Text key, BytesWritable value, Context context) throws IOException {						
			filesRead.increment(1);
			String inputFilename = key.toString();			

			String dsetID = FilenameUtils.getDsetID(inputFilename);
			if (deletedDatasets.contains(dsetID)) {
				filesDeleted.increment(1);
				// Nothing more to do
				return;
			}

			/*
			 * Open input stream
			 */			
			context.setStatus("Check: " + inputFilename);
			BufferedReader br = openBufferedReader(inputFilename);			
			if (br == null) {
				mapLogger.error("Could not open input file: " + inputFilename);
				readErrors.increment(1);
				return;
			}

			// Read header line
			String headerLine = br.readLine();
			if (headerLine == null) {
				mapLogger.error("Could not read header line");
				readErrors.increment(1);
				return;
			}
			
			rowsRead.increment(1);
			
			int[] deleteColumnIndexes = null;
			if (deletedSamples.containsKey(dsetID)) { // some samples should be removed
				String[] toDelete = deletedSamples.get(dsetID);
				
				// Parse first line to find indexes of samples to delete
				deleteColumnIndexes = GeoGSMOverlap.getDeleteColumnIndexes(toDelete, headerLine);
				if (deleteColumnIndexes == null ) {
					mapLogger.error("Could not find all samples to delete in header line: " + headerLine);
					readErrors.increment(1);
					return;
				}
				
				columnsRead.increment(headerLine.split("\t").length);				
				samplesDeleted.increment(deleteColumnIndexes.length);
			}

			/*
			 * Open output stream and process the file
			 */					
			String basename = tfs.getFilenameName(inputFilename);	
			String outputBasename = basename + ".rmo";
			BufferedWriter bw = openBufferedWriter(outputBasename, compressionFormat, context);
			if (bw != null) {										
				processFile(br, bw, deleteColumnIndexes, headerLine);
				bw.close();										
			}
			else { 				
				// Could not write directly to HDFS, so must fallback on local file system				
				String localFilename = OsPath.join(taskOutputDir, outputBasename);
				bw = null;
				try {
					bw = new BufferedWriter(new FileWriter(new File(localFilename)));
					processFile(br, bw, deleteColumnIndexes, headerLine);
					bw.close();
				} catch (IOException e) {
					mapLogger.error("Could not process file: ", e);					
					closeDeleteLocalBufferedWriter(bw, localFilename);					
				} 			
				// All local output files will be written to HDFS in cleanup()
			} 
			br.close();
			filesWritten.increment(1);
		}	
		
		/**
		 * Helper function to read in, check samples against samples to be deleted, and write non-deleted
		 * samples
		 * @param lin initialized line input stream
		 * @param bw initialized buffered writer stream
		 * @param deleteColumnIndexes indexes to delete, or null if no samples should be deleted
		 * @param headerLine previously read header lien
		 * @throws IOException 
		 */
		public void processFile(BufferedReader lin, BufferedWriter bw, int[] deleteColumnIndexes,
				String headerLine) throws IOException {
			String line = headerLine;
			while (line != null) {
				// read line is at end of the loop since the first line to parse is the
				// provided header line

				String outputLine = null;
				if (deleteColumnIndexes == null) { // nothing to delete
					outputLine = line;
				}
				else {
					outputLine = GeoGSMOverlap.deleteColumnsFromLine(line, deleteColumnIndexes);
				}

				bw.write(outputLine + "\n");
				rowsWritten.increment(1);
				
				// Read in next line to parse				
				line = lin.readLine();
				rowsRead.increment(1);
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
			job.setJarByClass(BatchPclRemoveOverlapping.class);

			/* Setup mapper */
			job.setMapperClass(RemoveOverlapMapper.class);	    				

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
		BatchPclRemoveOverlapping o = new BatchPclRemoveOverlapping();
		int exitCode = o.run(args);		
		System.exit(exitCode);	    	    
	}

}