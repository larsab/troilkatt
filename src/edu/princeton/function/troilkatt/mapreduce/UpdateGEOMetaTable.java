package edu.princeton.function.troilkatt.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;

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
import edu.princeton.function.troilkatt.hbase.TroilkattTable;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.tools.FilenameUtils;
import edu.princeton.function.troilkatt.tools.GeoGDSParser;
import edu.princeton.function.troilkatt.tools.GeoGSEParser;
import edu.princeton.function.troilkatt.tools.GeoSoftParser;
import edu.princeton.function.troilkatt.tools.ParseException;

/**
 * Extract meta-data information from a SOFT file and put these into the Hbase meta table.
 * 
 * The fields extracted from a SOFT file are:
 * - DatasetID (used as key)
 * - Title
 * - Date
 * - PubMedID
 * - Organisms
 * - Series description
 * - Platform IDs
 * - Platform names
 * - Platform descriptions
 * - Number of genes (rows) 
 * - Number of samples
 * - Sample IDs
 * - Sample descriptions
 * 
 * The format of the Hbase table is:
 * - key: dataset or series ID (GSEXXX or GDSXXX)
 * - column families: meta, calculated
 * - columns: meta:organism, meta:platformID, and so on
 * - values: tab separated values 
 */
public class UpdateGEOMetaTable extends PerFile {
	// MapReduce prgoress status counters
	enum LineCounters {
		LINES_READ,        // Number of lines read
		DATASETS_READ,     // Number of datasets processed
		ROWS_WRITTEN,      // Number of rows added to the meta data table
		TAGS_FOUND,        // Total number of meta-data tags found
		UNKNOWN_FILETYPES, // Not a GSE or GDS file
		INVALID_FILES      // Some important meta-data field was not found
	}	
	
	/**
	 * Mapper class that gets as input a filename and outputs a filename. For each file
	 * it does the following:
	 * 1. Read in one line at a time (the files can be tens of gigabytes in size)
	 * 2. For all meta lines check if the key=value matches one of the specified meta-tags
	 * 3. Build a row for the dataset and store it in Hbase
	 * 3. Output 
	 */
	public static class MetaParserMapper extends PerFileMapper {
		// Table handle
		protected GeoMetaTableSchema geoMetaTable;
		protected HTable table;
		
		// Counters used to report progress and avoid a job being assumed to be crashed
		protected Counter datasetsRead;
		protected Counter linesRead;			
		protected Counter tagsFound;
		protected Counter unknownFiletypes;
		protected Counter invalidFiles;
		protected Counter metaTableRowsAdded;
		
		/**
		 * Setup global variables. This function is called once per taks before map()
		 * @throws IOException 
		 */
		@Override
		public void setup(Context context) throws IOException {
			super.setup(context);
						
			/* Setup Htable */
			Configuration hbConf = HBaseConfiguration.create();
			geoMetaTable = new GeoMetaTableSchema();
			try {
				table = geoMetaTable.openTable(hbConf, true);
			} catch (HbaseException e) {
				throw new IOException("HbaseException: " + e.getMessage());
			}
			
			// Counters used to report progress and avoid a job being assumed to be crashed
			datasetsRead = context.getCounter(LineCounters.DATASETS_READ);
			linesRead = context.getCounter(LineCounters.LINES_READ);			
			tagsFound = context.getCounter(LineCounters.TAGS_FOUND);
			unknownFiletypes = context.getCounter(LineCounters.UNKNOWN_FILETYPES);
			invalidFiles = context.getCounter(LineCounters.INVALID_FILES);
			metaTableRowsAdded = context.getCounter(LineCounters.ROWS_WRITTEN);
		}			
		
		/**
		 * Cleanup function that is called once at the end of the task
		 */
		@Override
		protected void cleanup(Context context) throws IOException {
			table.close();
			super.cleanup(context);
		}

		/**
		 * Do the mapping by reading lines from the input file and parsing these.
		 * 
		 * @param key: HDFS soft filename
		 * @param value: always null since the SOFT files can be very large	
		 * @throws IOException 
		 * @throws IOException if input file could not be read
		 */
		@Override
		public void map(Text key, BytesWritable value, Context context) throws IOException {						
			/*
			 * Setup file for parsing
			 */
			datasetsRead.increment(1);
			
			// Open input stream
			String inputFilename = key.toString();			
			
			String basename = tfs.getFilenameName(inputFilename);
			BufferedReader lin = openBufferedReader(inputFilename);
			if (lin == null) {
				mapLogger.error("Could not open input file: " + inputFilename);
				invalidFiles.increment(1);
				return;
			}
			
			GeoSoftParser parser = null;
			if (basename.startsWith("GDS")) {
				parser = new GeoGDSParser();
			}
			else if (basename.startsWith("GSE")) {
				parser = new GeoGSEParser();
			}
			else {
				mapLogger.error("Uknown file type: " + basename);
				unknownFiletypes.increment(1);
				return;
			}
				
			/*
			 * Parse file
			 */
			long lcnt = 0;
			String line;
			while ((line = lin.readLine()) != null) {				
				linesRead.increment(1);
				lcnt++;
				
				if (lcnt % 1000 == 0) {
					context.setStatus("Lines read = " + lcnt);
				}
				
				if (line.isEmpty()) {
					continue;
				}
				
				if ((line.charAt(0) != '!') && (line.charAt(0) != '^')) { // is not a meta line
					continue;
				}
			
				// Parse the line
				if (parser.parseLine(line.toString())) {
					tagsFound.increment(1);
				}	
			}
			
			lin.close();
			
			/*
			 * Create row with meta data and save the row in Hbase
			 * 
			 * Note that the parser provides the key-value mappings put into Hbase.
			 */
			Put update = null;
			String dsetID = null;
			try {
				dsetID = FilenameUtils.getDsetID(inputFilename, true);
				if (dsetID == null) { // No ID found for file
					mapLogger.warn("ID not found for file: " + basename);					
					invalidFiles.increment(1);
					return;
				}
				
				update = new Put(Bytes.toBytes(dsetID), timestamp);
				//update = new Put(Bytes.toBytes(dsetID));
				byte[] family = Bytes.toBytes("meta");
				
				update.add(Bytes.toBytes("files"), Bytes.toBytes("softFilename"), Bytes.toBytes(inputFilename));
				for (String k: parser.singleKeys) {
					String val = parser.getSingleValue(k);
					if (val != null) {
						update.add(family, Bytes.toBytes(k), Bytes.toBytes(val));
					}
				}
				for (String k: parser.multiKeys) {
					ArrayList<String> val = parser.getValues(k);					
					if (val != null) {
						String s = TroilkattTable.array2string(val);					
						update.add(family, Bytes.toBytes(k), Bytes.toBytes(s));
					}
				}
			} catch (ParseException e) {
				mapLogger.warn("Parse exception for file: " + basename);
				invalidFiles.increment(1);
				return;
			}
			
			// Do the update
			try {
				mapLogger.info("Add row " + dsetID + " to table " + geoMetaTable.tableName);
				table.put(update);				
				metaTableRowsAdded.increment(1);
			} catch (IOException e) {
				mapLogger.error("Could not save updated row in Hbase: " + dsetID, e);
				throw e;
			}  
		}		
	}
	
	/**
	 * Create and execute MapReduce job
	 * 
	 * @param cargs command line arguments
	 * @throws StageInitException 
	 * @throws StageException 
	 */
	public int run(String[] cargs) {		
		Configuration conf = new Configuration();
		Configuration hbConf = HBaseConfiguration.create();
		
		String[] remainingArgs;
		try {
			remainingArgs = new GenericOptionsParser(conf, cargs).getRemainingArgs();
		} catch (IOException e2) {
			System.err.println("Could not parse arguments: IOException: " + e2.getMessage());
			return -1;
		}
		
		if (parseArgs(conf, remainingArgs) == false) {				
			System.err.println("Inavlid arguments " + cargs);
			return -1;
		}
		
		/*
		 * Setup HDFS
		 */
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e1) {		
			jobLogger.fatal("Could not create FileSystem object: " + e1.toString());			
			return -1;
		}
				
		GeoMetaTableSchema geoMetaTable = new GeoMetaTableSchema();			
		try {
			// Create the table if it does not already exist
			geoMetaTable.openTable(hbConf, true);
		} catch (HbaseException e1) {
			jobLogger.fatal("HbaseException: " + e1.getMessage());
			return -1;
		}
		
		/*
		 * Setup MapReduce job
		 */						
		Job job;
		try {
			job = new Job(conf, progName);
			job.setJarByClass(UpdateGEOMetaTable.class);
			
			/* Setup mapper: use the Compress class*/
			job.setMapperClass(MetaParserMapper.class);				

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
		try {
			return job.waitForCompletion(true) ? 0: -1;
		} catch (InterruptedException e) {
			jobLogger.fatal("Interrupt exception: " + e.toString());
			return -1;
		} catch (ClassNotFoundException e) {
			jobLogger.fatal("Class not found exception: " + e.toString());
			return -1;
		} catch (IOException e) {
			jobLogger.fatal("Job execution failed: IOException: " + e.toString());
			return -1;
		}	
	}

	/**
	 * Arguments: see documentation for run
	 */
	public static void main(String[] args) throws Exception {			
		// Hadoop configuration (core-default.xml and core-site.xml must be in classpath)
		UpdateGEOMetaTable o = new UpdateGEOMetaTable();
		int exitCode = o.run(args);		
		System.exit(exitCode);	    	    
	}
}
