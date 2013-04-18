package edu.princeton.function.troilkatt.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
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
import edu.princeton.function.troilkatt.fs.LogTableHbase;
import edu.princeton.function.troilkatt.hbase.GSMTableSchema;
import edu.princeton.function.troilkatt.hbase.GeoMetaTableSchema;
import edu.princeton.function.troilkatt.hbase.HbaseException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.tools.GeoGSMOverlap;

/**
 * Calculate sample (GSM) overlap in GEO datsets (GDS) and series (GSE) files.
 */
public class GSMOverlap extends TroilkattMapReduce {
	enum GSMOverlapCounters {
		ROWS_READ,
		INVALID_ROWS,
		PAIRS_WRITTEN,
		GSM_WITHOUT_GSE_MAPPING,
		META_ROWS_READ,
		OVERLAPS_WRITTEN,
		DUPLICATES
	}
	
	/**
	 * Mapper that takes as input GEO GSM table rows and outputs pairs of series 
	 * and datasets with overlapping samples. These are then used by the reducer
	 * to count the number of overlapping samples between each pair of dataset/series.
	 */
	public static class OverlapMapper extends TableMapper<Text, Text> {
		/*
		 * All global variables are set in setup()
		 */
		protected Configuration conf;
		// Global variables necessary for setting up and saving log files
		protected String taskAttemptID;
		protected String taskLogDir;
		protected LogTableHbase logTable;
		protected Logger mapLogger;
		
		protected Counter rowsRead;
		protected Counter invalidRows;
		protected Counter gsmsWritten;
		protected Counter gsmWithoutGSE;		

		/**
		 * Setup global variables. This function is called once per task before map()
		 */
		@Override
		public void setup(Context context) throws IOException {
			conf = context.getConfiguration();
			String pipelineName = TroilkattMapReduce.confEget(conf, "troilkatt.pipeline.name");
			
			String taskAttemptID = context.getTaskAttemptID().toString();
			taskLogDir = TroilkattMapReduce.getTaskLocalLogDir(context.getJobID().toString(), taskAttemptID);
			mapLogger = TroilkattMapReduce.getTaskLogger(conf);
			
			try {
				logTable = new LogTableHbase(pipelineName);
			} catch (PipelineException e) {
				mapLogger.fatal("Could not create logTable object: ", e);
				throw new IOException("Could not create logTable object: " + e);
			}
			
			rowsRead = context.getCounter(GSMOverlapCounters.ROWS_READ);
			invalidRows = context.getCounter(GSMOverlapCounters.INVALID_ROWS);
			gsmsWritten = context.getCounter(GSMOverlapCounters.PAIRS_WRITTEN);
			gsmWithoutGSE = context.getCounter(GSMOverlapCounters.GSM_WITHOUT_GSE_MAPPING);
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
		 * @param key GSM id (table row key)
		 * @param value GSE ids and GDS ids for series/datasets that contain sample GSM
		 * @throws IOException 
		 */
		@Override
		public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException, InterruptedException {			
			rowsRead.increment(1);
			String gsm = Bytes.toString(row.get());
			if (! gsm.startsWith("GSM")) {
				mapLogger.warn("Invalid row ID: " + gsm);
				invalidRows.increment(1);
			}
			
			byte[] family = Bytes.toBytes("in");

			// Get GDS values
			String[] gdss = new String[0];
			byte[] gdsBytes = values.getValue(family, Bytes.toBytes("GDS"));
			if (gdsBytes == null) { // possible if there are no mappings to datasets
				mapLogger.debug("Null value for column in:GDS for row: " + gsm);
			}
			else {
				gdss = Bytes.toString(gdsBytes).split("\n");
			}
			
			// Get GSE values
			String[] gses = new String[0];
			byte[] gseBytes = values.getValue(family, Bytes.toBytes("GSE"));
			if (gseBytes == null) { // possible if there are no mappings to series
				mapLogger.debug("Null value for column in:GSE for row: " + gsm);
				gsmWithoutGSE.increment(1);
			}
			else {
				gses = Bytes.toString(gseBytes).split("\n");
			}
			
			// Merge two lists and convert them to Text while at it
			String[] gids = new String[gses.length + gdss.length];        // ids
			Text[] gidsWithGsm = new Text[gses.length + gdss.length]; // samples
			for (int i = 0; i < gdss.length; i++) {				
				gids[i] = gdss[i];
				gidsWithGsm[i] = new Text(gdss[i] + "\t" + gsm);
			}
			for (int i = 0; i < gses.length; i++) {
				gids[gdss.length + i] = gses[i];
				gidsWithGsm[gdss.length + i] = new Text(gses[i] + "\t" + gsm);
			}

			// Output all pairs
			for (int i = 0; i < gids.length; i++) {
				Text gidIT = new Text(gids[i]);
				for (int j = i + 1; j < gids.length; j++) {
					if (GeoGSMOverlap.compareIDs(gids[i], gids[j]) < 0) {
						context.write(gidIT, gidsWithGsm[j]);
						gsmsWritten.increment(1);
					}
					else {
						context.write(new Text(gids[j]), gidsWithGsm[i]);
						gsmsWritten.increment(1);
					}					
				}		
			}
		}
	}

	/**
	 * Reducer that takes as input (GIDi, GIDj\tGSM) pairs which it uses to count the number
	 * of overlapping samples between series/dataset i and series/dataset j. The resulting
	 * counts are output to files which are later processed to find duplicates, subseries,
	 * and partial overlaps.
	 * 
	 * The line format for the output file is:
	 *   GID_i<tab>GID_j<tab>overlap count,GID_i sample count,GID_j sample count<tab>
	 *   [overlapping samples]<tab>meta_i<tab>meta_j<newline>
	 * 
	 * where [overlapping samples] is the list of overlapping samples in the following format
	 *   GSM_1,GSM_2,..,GSM_N<tab>
	 * 
	 * and meta_i contains meta data read from the GEO meta data table for GID_i
	 *   updateDate<tab>Organism1,Organism2...OrganismN
	 */
	public static class PairCounterReducer extends Reducer <Text, Text, Text, Text> {
		/*
		 * All global variables are set in setup()
		 */
		protected Configuration conf;
		// Global variables necessary for setting up and saving log files
		protected String taskAttemptID;
		protected String taskLogDir;
		protected LogTableHbase logTable;
		protected Logger reduceLogger;

		// The GEO meta data table is used to get meta data information about
		// series and datasets which is added to the output files
		protected GeoMetaTableSchema geoMetaSchema;
		protected HTable geoMetaTable;

		/*
		 *  Caches with meta data already read from the geoMetaTable
		 */
		// key: GSE or GDS id, value: "date\torganism1,organism2,..."
		protected HashMap<String, String> gid2meta;
		// key: GSE or GDS id, value: number of GSMs for series/dataset
		protected HashMap<String, Integer> gid2nSamples;
		
		protected Counter metaRowsRead;
		protected Counter overlapsWritten;
		protected Counter duplicatesFound;

		/**
		 * This function is called once at the start of the task
		 */
		@Override
		public void setup(Context context)  throws IOException {
			conf = context.getConfiguration();
			String pipelineName = TroilkattMapReduce.confEget(conf, "troilkatt.pipeline.name");
			
			String taskAttemptID = context.getTaskAttemptID().toString();
			taskLogDir = TroilkattMapReduce.getTaskLocalLogDir(context.getJobID().toString(), taskAttemptID);
			reduceLogger = TroilkattMapReduce.getTaskLogger(conf);
			
			try {
				logTable = new LogTableHbase(pipelineName);
			} catch (PipelineException e) {				
				throw new IOException("Could not create logTable object: " + e);
			}
			geoMetaSchema = new GeoMetaTableSchema();
			try {
				geoMetaTable = geoMetaSchema.openTable(logTable.hbConfig, false);
			} catch (HbaseException e) {
				reduceLogger.fatal("Could not open Hbase table: ", e);
				throw new IOException("Could not open Hbase table: " + e);
			}			
			
			gid2nSamples = new HashMap<String, Integer>();
			gid2meta = new HashMap<String, String>();
			
			metaRowsRead = context.getCounter(GSMOverlapCounters.META_ROWS_READ);
			overlapsWritten = context.getCounter(GSMOverlapCounters.OVERLAPS_WRITTEN);
			duplicatesFound = context.getCounter(GSMOverlapCounters.DUPLICATES);
		}

		/**
		 * Cleanup function that is called once at the end of the task
		 */
		@Override
		protected void cleanup(Context context) throws IOException {
			geoMetaTable.close();
			TroilkattMapReduce.saveTaskLogFiles(conf, taskLogDir, taskAttemptID, logTable);
		}

		/**
		 *  Do the reduce
		 *  
		 *  @param key GSE or GDS id
		 *  @param values
		 *  @param context MapReduce context supplied by the runtime system
		 */
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String keyGid = key.toString();
			if (loadMeta(keyGid) == false) {				
				throw new IOException("Could not get GEO meta data for row: " + keyGid);
			}
			metaRowsRead.increment(1);
			String meta1 = gid2meta.get(keyGid);
			
			HashMap<String, ArrayList<String>> overlappingSamples = new HashMap<String, ArrayList<String>>();
			for (Text val: values) {
				String vs = val.toString();
				String parts[] = vs.split("\t");
				if (parts.length != 2) {
					throw new IOException("Invalid value: " + vs);
				}
				String gid = parts[0];
				String gsm = parts[1];
				ArrayList<String> gsms = overlappingSamples.get(gid);
				if (gsms == null) { // not in hashmap
					gsms = new ArrayList<String>();
					overlappingSamples.put(gid, gsms);
				}
				if (gsms.contains(gsm)) { // duplicate found
					reduceLogger.warn("Duplicate found for: " + gsm + ": " + gid);
					duplicatesFound.increment(1);
				}
				else {
					gsms.add(gsm);
				}
			}
			
			for (String gid: overlappingSamples.keySet()) {
				if (loadMeta(gid) == false) {
					throw new IOException("Could not get GEO meta data for row: " + gid);
				}
				metaRowsRead.increment(1);
				
				ArrayList<String> gsms = overlappingSamples.get(gid);
				// Build output value
				StringBuilder sb = new StringBuilder();
				sb.append(gid);
				sb.append("\t");
				sb.append(gsms.size());
				sb.append(",");
				sb.append(gid2nSamples.get(keyGid));
				sb.append(",");
				sb.append(gid2nSamples.get(gid));
				sb.append("\t");
				sb.append(gsms.get(0));
				for (int i = 1; i < gsms.size(); i++) {
					sb.append(",");
					sb.append(gsms.get(i));
				}
				sb.append("\t");
				sb.append(meta1);
				sb.append("\t");
				sb.append(gid2meta.get(gid));				
				String outputValue = sb.toString();
				
				context.write(key, new Text(outputValue));
				overlapsWritten.increment(1);
			}		
		}

		/**
		 * Get meta data string for a series/dataset, and update gid2meta and gid2nSamples
		 * 
		 * @param GSE or GDS id
		 * @return true if data was successfully loaded or it was already cached. False 
		 * indicates an error in this program.
		 * @throws IOException 
		 */
		protected boolean loadMeta(String gid) throws IOException {
			if (gid2meta.containsKey(gid)) { // Already loaded
				return true; 
			}
			
			Get get = new Get(Bytes.toBytes(gid));	
			byte[] metaFam = Bytes.toBytes("meta");
			get.addColumn(metaFam, Bytes.toBytes("organisms"));
			get.addColumn(metaFam, Bytes.toBytes("sampleIDs"));
			get.addColumn(metaFam, Bytes.toBytes("date"));
			
			Result result;
			try {
				result = geoMetaTable.get(get);
			} catch (IOException e) {				
				throw new IOException("Could not get row: " + gid + ": ", e);
			}
			
			if (result == null) {
				reduceLogger.error("Could not get meta data for row: " + gid);
				return false;
			}						
			
			byte[] orgBytes = result.getValue(metaFam, Bytes.toBytes("organisms"));
			if (orgBytes == null) {
				reduceLogger.error("Null value for meta:organisms column in row: " + gid);
				return false;
			}
			String[] orgs = Bytes.toString(orgBytes).split("\n");
			
			byte[] dateBytes = result.getValue(metaFam, Bytes.toBytes("date"));
			if (dateBytes == null) {
				reduceLogger.error("Null value for meta:date column in row: " + gid);
				return false;
			}
			String date = Bytes.toString(dateBytes);
			String meta = date + "\t" + orgs[0];
			for (int i = 1; i < orgs.length; i++) {
				meta = meta + "," + orgs[i]; 
			}			
			
			byte[] samplesBytes = result.getValue(metaFam, Bytes.toBytes("sampleIDs"));
			if (samplesBytes == null) {
				reduceLogger.error("Null value for meta:sampleIDs column in row: " + gid);
				return false;
			}
			
			String[] sampleIDs = Bytes.toString(samplesBytes).split("\n");
			int nSamples = sampleIDs.length;
			if (nSamples < 1) {
				reduceLogger.error("No samples for row: " + gid);
				return false;
			}
			
			gid2meta.put(gid, meta);
			gid2nSamples.put(gid, sampleIDs.length);
			return true;
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
			e2.printStackTrace();
			System.err.println("Could not parse arguments: " + e2);
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
			jobLogger.fatal("Could not create FileSystem object: ", e1);			
			return -1;
		}

		/*
		 * Setup MapReduce job
		 */						
		Job job;
		try {
			// Set memory limits
			// Note! must be done before creating job
			setMemoryLimits(conf);
						
			job = new Job(conf, progName);
			job.setJarByClass(GSMOverlap.class);		
			
			/* Setup mapper */		
			job.setMapperClass(OverlapMapper.class);
			Scan scan = new Scan();		    
		    GSMTableSchema gsmTable = new GSMTableSchema();		    
		    TableMapReduceUtil.initTableMapperJob(gsmTable.tableName, scan,
		    		OverlapMapper.class, Text.class, Text.class, job);
			
			/* Setup reducer */					
			job.setReducerClass(PairCounterReducer.class);	    	    		
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			setOutputPath(hdfs, job);			
		} catch (IOException e1) {
			jobLogger.fatal("Job setup failed:v", e1);
			return -1;
		} catch (StageInitException e) {
			jobLogger.fatal("Could not initialize job: ", e);
			return -1;
		}

		// Execute job and wait for completion
		try {
			return job.waitForCompletion(true) ? 0: -1;
		} catch (InterruptedException e) {
			jobLogger.fatal("Job exception failed: ", e);
			return -1;
		} catch (ClassNotFoundException e) {
			jobLogger.fatal("Job exception failed: ", e);
			return -1;
		} catch (IOException e) {
			jobLogger.fatal("Job exception failed: ", e);
			return -1;
		}
	}

	/**
	 * Arguments: see documentation for run
	 */
	public static void main(String[] args) throws Exception {			
		// Hadoop configuration (core-default.xml and core-site.xml must be in classpath)
		GSMOverlap o = new GSMOverlap();
		int exitCode = o.run(args);		
		System.exit(exitCode);	    	    
	}
}
