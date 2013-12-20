package edu.princeton.function.troilkatt.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.princeton.function.troilkatt.hbase.GeoMetaTableSchema;
import edu.princeton.function.troilkatt.hbase.HbaseException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.tools.FilenameUtils;
import edu.princeton.function.troilkatt.tools.ParseException;
import edu.princeton.function.troilkatt.tools.Pcl2Info;

/**
 * Calculate statistics for a PCL file.
 */
public class BatchPcl2Info extends PerFile {
	enum BatchCounters {
		FILES_READ,	
		LINES_READ,
		ROWS_UPDATED,		
		READ_ERRORS,
		UPDATE_ERRORS
	}
	
	/**
	 * Mapper class that uses a GeoGSEPcl parser to convert the file
	 */
	public static class PclConvertMapper extends PerFileMapper {		
		// Counters
		protected Counter filesRead;		
		protected Counter linesRead;
		protected Counter rowsUpdated;		
		protected Counter readErrors;
		protected Counter updateErrors;
		
		// Parser object
		private Pcl2Info parser;
		
		// Table handle
		protected GeoMetaTableSchema geoMetaTable;
		protected HTable table;
		
		/**
		 * Setup global variables. This function is called before map()
		 * @throws IOException 
		 */
		@Override
		public void setup(Context context) throws IOException {
			super.setup(context);			
			
			// Counters used to report progress and avoid a job being assumed to be crashed
			filesRead = context.getCounter(BatchCounters.FILES_READ);			
			linesRead = context.getCounter(BatchCounters.LINES_READ);
			rowsUpdated = context.getCounter(BatchCounters.ROWS_UPDATED);			
			readErrors = context.getCounter(BatchCounters.READ_ERRORS);
			updateErrors = context.getCounter(BatchCounters.UPDATE_ERRORS);
			
			parser = new Pcl2Info();
			
			/* Setup Htable */
			//Configuration hbConf = HBaseConfiguration.create();
			Configuration hbConf = conf;
			geoMetaTable = new GeoMetaTableSchema();
			try {
				table = geoMetaTable.openTable(hbConf, true);
			} catch (HbaseException e) {
				mapLogger.fatal("Could not open table", e);				
				throw new IOException("Could not open table: " + e);
			}
		}			
	
	
		/**
		 * Do the mapping: 
		 * 1. Read one line at a time from a file in HDFS
		 * 2. Parse the line
		 * 3. When done write calculated meta-information to the Hbase table 
		 * 
		 * @param key HDFS pcl filename
		 * @param value always null since the pcl files can be very large	
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
			context.setStatus("Calculate: " + inputFilename);
			BufferedReader lin = openBufferedReader(inputFilename);			
			if (lin == null) {
				mapLogger.error("Could not open input file: " + inputFilename);
				readErrors.increment(1);
				return;
			}

			/*
			 * Do calculation
			 */
			HashMap<String, String> results;
			try {
				results = parser.calculate(lin);
			} catch (ParseException e1) {				
				mapLogger.warn("Could not parse file", e1);
				readErrors.increment(1);
				return;
			}			

			/*
			 * Create row with meta data and save the row in Hbase
			 */	
			String dsetID = FilenameUtils.getDsetID(inputFilename);
			if (dsetID == null) {
				readErrors.increment(1);
				mapLogger.fatal("No dsetID for filename: " + inputFilename);				
				throw new IOException("No dsetID for filename: " + inputFilename);
			}
			else if ((! dsetID.startsWith("GSE")) && (! dsetID.startsWith("GDS"))) {
				readErrors.increment(1);
				mapLogger.fatal("Invalid dsetID: " + dsetID + " for filename: " + inputFilename);				
				throw new IOException("Invalid dsetID: " + dsetID + " for filename: " + inputFilename);
			}
			 
			byte[] colFam = Bytes.toBytes("calculated");
			try {
				Put update = null;

				update = new Put(Bytes.toBytes(dsetID));
				update.add(Bytes.toBytes("files"), Bytes.toBytes("pclFilename"), Bytes.toBytes(inputFilename));
				String dsetIDNoPlatform = FilenameUtils.getDsetID(inputFilename, false);
				update.add(Bytes.toBytes("calculated"), Bytes.toBytes("id-noPlatform"), Bytes.toBytes(dsetIDNoPlatform));
				for (String k: results.keySet()) {
					update.add(colFam, Bytes.toBytes(k), Bytes.toBytes(results.get(k)));
				}
				mapLogger.info("Update row " + dsetID + " in table " + geoMetaTable.tableName);
				table.put(update);			
			} catch (IOException e) {
				updateErrors.increment(1);
				mapLogger.warn("Could not save updated row in Hbase: ", e);				
			}  
			rowsUpdated.increment(1);
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
			job.setJarByClass(BatchPcl2Info.class);

			/* Setup mapper */
			job.setMapperClass(PclConvertMapper.class);	    				

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
		BatchPcl2Info o = new BatchPcl2Info();
		int exitCode = o.run(args);		
		System.exit(exitCode);	    	    
	}
}
