package edu.princeton.function.troilkatt.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import edu.princeton.function.troilkatt.PipelineException;
import edu.princeton.function.troilkatt.fs.LogTable;
import edu.princeton.function.troilkatt.hbase.GSMTableSchema;
import edu.princeton.function.troilkatt.hbase.GeoMetaTableSchema;
import edu.princeton.function.troilkatt.hbase.HbaseException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * Update entries in the GEO GSM table by reading information from the GEO Meta table.
 */
public class UpdateGSMTable extends TroilkattMapReduce {
	enum GSMCounters {
		GSE_ROWS_READ,
		GDS_ROWS_READ,
		GSM_MAPPINGS_WRITTEN,
		INVALID_ROWS,
		INVALID_GSM_IDS,
		GSM_ROWS_UPDATED
	}
	
	/**
	 * Mapper that takes as input GEO meta-data table rows and outputs (GSMx, GSEy|GDSz)
	 * tuples. The reducer will then update the row for GSM in the GSM table. 
	 */
	public static class GSM2GIDMapper extends TableMapper<Text, Text> {
		/*
		 * All global variables are set in setup()
		 */
		protected Configuration conf;
		// Global variables necessary for setting up and saving log files
		protected String taskAttemptID;
		protected String taskLogDir;
		protected LogTable logTable;
		protected Logger mapLogger;
		
		protected Counter gdsRowsRead;
		protected Counter gseRowsRead;
		protected Counter gsmMappingsWritten;
		protected Counter invalidRows;
		protected Counter invalidGSMIds;
				
		
		/**
		 * Setup global variables. This function is called once per task before map()
		 */
		@Override
		public void setup(Context context) throws IOException {
			Configuration hbConf = HBaseConfiguration.create();
			conf = context.getConfiguration();
			String pipelineName = TroilkattMapReduce.confEget(conf, "troilkatt.pipeline.name");
			try {
				logTable = new LogTable(pipelineName, hbConf);
			} catch (PipelineException e) {
				throw new IOException("Could not create logTable object: " + e.getMessage());
			}
			
			String taskAttemptID = context.getTaskAttemptID().toString();
			taskLogDir = TroilkattMapReduce.getTaskLocalLogDir(context.getJobID().toString(), taskAttemptID);
			mapLogger = TroilkattMapReduce.getTaskLogger(conf);
			
			gseRowsRead = context.getCounter(GSMCounters.GSE_ROWS_READ);
			gdsRowsRead = context.getCounter(GSMCounters.GDS_ROWS_READ);
			gsmMappingsWritten = context.getCounter(GSMCounters.GSM_MAPPINGS_WRITTEN);
			invalidRows = context.getCounter(GSMCounters.INVALID_ROWS);
			invalidGSMIds = context.getCounter(GSMCounters.INVALID_GSM_IDS);
		}
		
		/**
		 * Cleanup function that is called once at the end of the task
		 */
		@Override
		protected void cleanup(Context context) throws IOException {			
			TroilkattMapReduce.saveTaskLogFiles(conf, taskLogDir, taskAttemptID, logTable);			
		}
		
		/**
		 * Do the mapping by parsing rows from the GEO meta table and outputting
		 * GSM to GSE/GDS mappings
		 * 
		 * @param key GSE or GDS id (table row key)
		 * @param value meta data for series or dataset
		 * @throws IOException 
		 */
		@Override
		public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException, InterruptedException {
			String gid  = Bytes.toString(row.get());
			
			if (gid.startsWith("GSE")) {
				gseRowsRead.increment(1);
			}
			else if (gid.startsWith("GDS")) {
				gdsRowsRead.increment(1);
			}
			else {
				mapLogger.warn("Invalid row ID: " + gid);
				invalidRows.increment(1);
				return;
			}
			
			// Get GSM values
			KeyValue gsmValue = values.getColumnLatest(Bytes.toBytes("meta"), Bytes.toBytes("sampleIDs"));			
			if (gsmValue == null) {
				mapLogger.warn("Row does not contain meta:sampleIDs column: " + gid);
				invalidRows.increment(1);
				return;
			}
			byte[] gsmBytes = gsmValue.getValue(); 
			if (gsmBytes == null) {
				// Not sure if this condition is possible
				mapLogger.warn("Column meta:sampleIDs had null value: " + gid);
				invalidRows.increment(1);
				return;
			}
			String gsmString = Bytes.toString(gsmBytes);
			if (gsmString.isEmpty()) {
				mapLogger.warn("Column meta:sampleIDs had empty value: " + gid);
				invalidRows.increment(1);
				return;
			}
			String[] gsms = gsmString.split("\n");
			if (gsms.length == 0) {
				System.err.println("No GSMs for " + gid);
			}
			
			// Output all pairs (GSM, GID) pairs
			Text gidText = new Text(gid);
			for (String gsm: gsms) {
				if (! gsm.startsWith("GSM")) {
					mapLogger.warn("Invalid GSM id: " + gsm + " in row: " + gid);
					invalidGSMIds.increment(1);
				}
				context.write(new Text(gsm), gidText);
				gsmMappingsWritten.increment(1);
			}
		}
	}
	
	/**
	 * Reducer class that retrieves (GSM, GSE/GDS) tuples from the mapper and uses these to
	 * create the list of GSM to GSE/GDS mappings in the GSM table.
	 */
	public static class GSMTableUpdateReducer extends Reducer <Text, Text, Text, Text> {
		/*
		 * All global variables are set in setup()
		 */
		protected Configuration conf;
		// Global variables necessary for setting up and saving log files
		protected String taskAttemptID;
		protected String taskLogDir;
		protected long timestamp;
		protected LogTable logTable;
		protected Logger reduceLogger;
		
		// GSM Table handle
		protected GSMTableSchema gsmTable;
		protected HTable table;
		
		protected Counter gsmRowsUpdated;
		
		/**
		 * This function is called once at the start of the task
		 */
		@Override
		public void setup(Context context)  throws IOException {			
			Configuration hbConf = HBaseConfiguration.create();
			conf = context.getConfiguration();
			String pipelineName = TroilkattMapReduce.confEget(conf, "troilkatt.pipeline.name");
			try {
				logTable = new LogTable(pipelineName, hbConf);
			} catch (PipelineException e) {
				throw new IOException("Could not create logTable object: " + e.getMessage());
			}
			timestamp = Long.valueOf(TroilkattMapReduce.confEget(conf, "troilkatt.timestamp"));
			
			gsmTable = new GSMTableSchema();
			try {
				table = gsmTable.openTable(hbConf, true);
			} catch (HbaseException e) {
				throw new IOException("HbaseException: " + e.getMessage());
			}
			
			String taskAttemptID = context.getTaskAttemptID().toString();
			taskLogDir = TroilkattMapReduce.getTaskLocalLogDir(context.getJobID().toString(), taskAttemptID);
			reduceLogger = TroilkattMapReduce.getTaskLogger(conf);
			
			gsmRowsUpdated = context.getCounter(GSMCounters.GSM_ROWS_UPDATED);
		}
		
		/**
		 * Cleanup function that is called once at the end of the task
		 */
		@Override
		protected void cleanup(Context context) throws IOException {
			table.close();
			TroilkattMapReduce.saveTaskLogFiles(conf, taskLogDir, taskAttemptID, logTable);
		}
		
		/**
		 *  Do the reduce
		 *  
		 *  @param key GSM id
		 *  @param values either GSE or GDS ids
		 *  @param context MapReduce context supplied by the runtime system
		 */
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String gsm = key.toString();			
			String gses = null;
			String gdss = null;
			
			for (Text val: values) {
				String v = val.toString();
				if (v.startsWith("GSE")) {
					if (gses == null) {
						gses = v;
					}
					else {
						gses = gses + "\n" + v;
					}
				}
				else if (v.startsWith("GDS")) {
					if (gdss == null) {
						gdss = v;
					}
					else {
						gdss = gdss + "\n" + v;
					}
				}
				else {					
					throw new IOException("Invalid value: " + v);
				}
			}
			    			
			// Create updated row
			Put update = new Put(Bytes.toBytes(gsm), timestamp);
			byte[] family = Bytes.toBytes("in");							
			if (gses != null) {
				update.add(family, Bytes.toBytes("GSE"), Bytes.toBytes(gses));
				// Also output to file for debugging purposes
				context.write(key, new Text(gses.replace("\n", ",")));
			}
			if (gdss != null) {
				update.add(family, Bytes.toBytes("GDS"), Bytes.toBytes(gdss));
				context.write(key, new Text(gdss.replace("\n", ",")));
			}
			if ((gses == null) && (gdss == null)) {
				reduceLogger.warn("Warning: no gses nor gdss for key: " + key.toString());
				return;
			}
					
			// Do the update
			try {
				table.put(update);
			} catch (IOException e) {
				reduceLogger.error("Could not save updated row in GSM Hbase table: " + e.getMessage());
				throw e;
			}  
			gsmRowsUpdated.increment(1);
		}			
	}
	
	/**
	 * Create and execute MapReduce job
	 * 
	 * @param cargs command line arguments
	 * @return 0 on success, -1 of failure
	 */
	public int run(String[] cargs) {				
		Configuration conf = getMergedConfiguration();
		
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
		
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e1) {		
			jobLogger.fatal("Could not create FileSystem object: " + e1.toString());			
			return -1;
		}			
			
		/*
		 * Setup MapReduce job
		 */						
		Job job;
		try {
			job = new Job(conf, progName);
			job.setJarByClass(UpdateGSMTable.class);
		
			/* Setup mapper */		
			job.setMapperClass(GSM2GIDMapper.class);
			Scan scan = new Scan();
		    scan.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("sampleIDs"));		    
		    GeoMetaTableSchema geoMeta = new GeoMetaTableSchema();
		    System.out.println("Table name = " + geoMeta.tableName);
		    TableMapReduceUtil.initTableMapperJob(geoMeta.tableName, scan,
		    	      GSM2GIDMapper.class, Text.class, Text.class, job);
			
			/* Setup reducer */					
			job.setReducerClass(GSMTableUpdateReducer.class);	    	    		
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			setOutputPath(hdfs, job);
		} catch (IOException e1) {
			jobLogger.fatal("Job setup failed due to IOException: " + e1.getMessage());
			return -1;
		} catch (StageInitException e2) {
			jobLogger.fatal("Job setup failed due to set output path exception: " + e2.getMessage());
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
	public static void main(String[] args) {
		UpdateGSMTable o = new UpdateGSMTable();
		int exitCode = o.run(args);		
		System.exit(exitCode);	 
	}

}
