package edu.princeton.function.troilkatt.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.hbase.GeoMetaTableSchema;
import edu.princeton.function.troilkatt.hbase.HbaseException;
import edu.princeton.function.troilkatt.hbase.TroilkattTable;
import edu.princeton.function.troilkatt.pipeline.Stage;
import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.tools.FilenameUtils;


public class BatchRawRemoveOverlapping extends PerFile {
	enum BatchCounters {
		FILES_READ,	
		PLATFORMS_READ,   // Input file is split into multiple platforms
		PLATFORMS_WRITTEN,
		PLATFORMS_DELETED, // Some are deleted due to being overlapping or having no samples		
		SAMPLES_READ,      // sum of all samples
		SAMPLES_WRITTEN,   // samples with raw files
		SAMPLES_IGNORED,   // raw files not found		
		READ_ERRORS,
		WRITE_ERRORS
	}

	/**
	 * Mapper class that splits the sample CEL files by platform, remove overlapping samples, and creates
	 *  a tar file for each platform. Each tar file contains the CEL files for that platform (and only 
	 *  the CEL files).
	 */
	public static class SplitRemoveOverlapMapper extends PerFileMapper {		
		// Counters
		protected Counter filesRead;
		protected Counter platformsRead;
		protected Counter platformsWritten;		
		protected Counter platformsDeleted;
		protected Counter samplesRead;
		protected Counter samplesWritten;
		protected Counter samplesIgnored;		
		protected Counter readErrors;
		protected Counter writeErrors;

		// The file that contains the dataset/series and samples to delete
		//protected String filename;
		
		// Table handle	
		protected HTable metaTable;

		/**
		 * Setup global variables. This function is called before map()
		 * @throws IOException 
		 */
		@Override
		public void setup(Context context) throws IOException {
			super.setup(context);			

			// Counters used to report progress and avoid a job being assumed to be crashed
			filesRead = context.getCounter(BatchCounters.FILES_READ);
			platformsRead = context.getCounter(BatchCounters.PLATFORMS_READ);
			platformsWritten = context.getCounter(BatchCounters.PLATFORMS_WRITTEN);
			platformsDeleted = context.getCounter(BatchCounters.PLATFORMS_DELETED);			
			samplesRead = context.getCounter(BatchCounters.SAMPLES_READ);
			samplesWritten = context.getCounter(BatchCounters.SAMPLES_WRITTEN);
			samplesIgnored = context.getCounter(BatchCounters.SAMPLES_IGNORED);
			readErrors = context.getCounter(BatchCounters.READ_ERRORS);
			writeErrors = context.getCounter(BatchCounters.WRITE_ERRORS);	

			//String stageArgs = TroilkattMapReduce.confEget(conf, "troilkatt.stage.args");
			//try {
			//	filename = TroilkattMapReduce.setTroilkattSymbols(stageArgs, 
			//			conf, jobID, taskAttemptID, troilkattProperties, mapLogger);
			//} catch (TroilkattPropertiesException e) {
			//	mapLogger.fatal("Invalid properties file", e);				
			//	throw new IOException("Could not read the properties file");
			//}	
			
			/* Setup Htable */
			Configuration hbConf = HBaseConfiguration.create();
			GeoMetaTableSchema metaTableSchema = new GeoMetaTableSchema();
			try {
				metaTable = metaTableSchema.openTable(hbConf, true);
			} catch (HbaseException e) {
				mapLogger.error("Could not get handle to meta data table", e);
				throw new IOException("Could not get handle to meta data table");
			}
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
			/*
			 * Get meta data necessary for the split
			 */
			filesRead.increment(1);
			String inputFilename = key.toString();			
			String seriesID = FilenameUtils.getDsetID(inputFilename);
			HashMap<String, ArrayList<String>> sidp2gsm = getParts(seriesID);
			
			/*
			 * Unpack raw file
			 */
			// Copy from HDFS to local FS
			String localInputFilename = tfs.getFile(inputFilename, taskInputDir, taskTmpDir, taskLogDir);
			if (localInputFilename == null) {
				mapLogger.error("Could not copy file to local FS: " + inputFilename);
				readErrors.increment(1);
				return;
			}

			// Untar
			if (untarRaw(localInputFilename) != 0) {
				mapLogger.error("Could not untar raw file: " + localInputFilename);
				readErrors.increment(1);
				return;
			}

			/*
			 * Split CEL files into platform specific files, and remove overlapping
			 */
			Set<String> sidp = sidp2gsm.keySet();
			for (String s: sidp) { // for each platform
				platformsRead.increment(1);
				ArrayList<String> gsms = sidp2gsm.get(s);
				int added = splitCelFiles(s, gsms);
				samplesRead.increment(gsms.size());
				
				if (added == -1) {
					mapLogger.error("Could not create tar file for series: " + seriesID);
					writeErrors.increment(1);
					continue;
				}
				if (added == 0) {					
					platformsDeleted.increment(1);
					samplesIgnored.increment(gsms.size());
					continue;
				}											
				
				samplesIgnored.increment(gsms.size() - added);
				samplesWritten.increment(added);
			}
			
			// The output files will be saved in cleanup 
			platformsWritten.increment(1);
		} // map			
		
		/**
		 * Get platform specific parts for a series
		 * 
		 * @param seriesID series ID
		 * @return hash map where the platform-specific series ID is used as key, and 
		 * the value consist of a list with sample IDs in that platform (with samples
		 * removed due to overlap)
		 */
		protected HashMap<String, ArrayList<String>> getParts(String seriesID) {
			HashMap<String, ArrayList<String>> sid2gsm = new HashMap<String, ArrayList<String>>();
			/*
			 *  Read rows from Hbase meta table
			 */					
			String rowStartKey = seriesID;
			String rowEndKey = seriesID + "zzz";
			// Filter to ensure that only rows for the given stage are selected
			Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
				      new SubstringComparator(seriesID));
			// Need a scanner to iterate over all MapReduce task rows
			Scan scan = new Scan(Bytes.toBytes(rowStartKey), Bytes.toBytes(rowEndKey));		
			scan.setFilter(filter);
			byte[] fam = Bytes.toBytes("calculated");
			byte[] qual = Bytes.toBytes("sampleIDs-overlapRemoved");
			//byte[] fam = Bytes.toBytes("meta");
			//byte[] qual = Bytes.toBytes("sampleIDs");
			scan.addColumn(fam, qual);
			
			// Get rows
			ResultScanner scanner;
			try {
				scanner = metaTable.getScanner(scan);
			} catch (IOException e) {
				mapLogger.warn("Could not create Hbase table scanner ", e);
				return null;
			}			
			// and iterate over all returned rows					
			for (Result res: scanner) {
				String sidWithPlatform = Bytes.toString(res.getRow());
				if (! sidWithPlatform.startsWith(seriesID)) {
					mapLogger.warn("Scanner returned row which was not a split of: " + rowStartKey);
					continue;
				}
				
				byte[] valBytes = res.getValue(fam, qual);
				if (valBytes == null) {
					mapLogger.error("Null value for sampleID in row: " + sidWithPlatform);
					continue;	
				}				
				String val = Bytes.toString(valBytes);
				ArrayList<String> gsmIDs = TroilkattTable.string2array(val);
				sid2gsm.put(sidWithPlatform, gsmIDs);
			}
			
			return sid2gsm;
		}
		
		/**
		 * Helper function to untar a Geo raw file
		 * 
		 * @param tarFilename of the tar file in the local file system
		 * @return 0 on success, non-zero if the file could ot be untared
		 */
		protected int untarRaw(String tarFilename) {
			// for use in the log file names
			String seriesID = OsPath.basename(tarFilename).split("\\.")[0];
			
			String cmd = String.format("tar xvf %s -C %s > %s 2> %s",
					tarFilename,
					taskInputDir,
					OsPath.join(taskLogDir, "untar." + seriesID + ".output"),
					OsPath.join(taskLogDir, "untar." + seriesID + ".error"));
			return Stage.executeCmd(cmd, mapLogger);			
		}

		/**
		 * Helper function to get all input files files that belong to a specific sample 
		 * 
		 * @param gsmID sample ID
		 * @return list of raw files that belong to this sample
		 */
		protected ArrayList<String> getRawFiles(String gsmID) {
			ArrayList<String> gsmFiles = new ArrayList<String>();
			String[] files = OsPath.listdirR(taskInputDir);
			
			for (String f: files) {
				String basename = OsPath.basename(f);
				// Convert everything to lower case to make matching simpler
				basename = basename.toLowerCase();
				gsmID = gsmID.toLowerCase();
				
				if (basename.startsWith(gsmID) && 
						((basename.contains(".cel.") || basename.endsWith(".cel")))) { 
					// Is a Affymetrix CEL file that can be processed later
					gsmFiles.add(f);
				}
			}
			
			return gsmFiles;
		}		
		
		/**
		 * Select the CEL files for the specified samples and add these to a tar file in
		 * thhe output directory.
		 * 
		 * @param seriesID that included the platformID if there are multiple platforms for 
		 * a sample. The seriesID is used as the filename for the output tar file.
		 * @param gsms list of samples to add.
		 * @return number of samples added to tar file. Zero if there are no samples to add
		 * or if all have been deleted due to overlap. -1 if an output tar file could not
		 * be created.
		 */
		protected int splitCelFiles(String seriesID, ArrayList<String> gsms) {
			if (gsms.isEmpty()) {
				mapLogger.warn("No sample Ids for: " + seriesID);
				return 0;
			}
			if (gsms.get(0).equals("none")) {
				// All samples deleted due to overlap
				mapLogger.info("All samples deleted for: " + seriesID);
				return 0;
			}
				
			String outputTar = OsPath.join(taskOutputDir, seriesID + ".tar");
			int nWritten = 0;
			boolean isFirstTime = true;
			String cmd = null;
			try {
				for (int i = 0; i < gsms.size(); i++) {				
					ArrayList<String> rawFiles = getRawFiles(gsms.get(i));
					if (rawFiles.isEmpty()) { 
						// no raw CEL files found	for sample			
						continue;
					}

					for (String rf: rawFiles) {								
						if (isFirstTime) {
							isFirstTime = false;						
							// Create tar file
							cmd = String.format("cd %s; tar cvf %s %s > %s 2> %s",
									taskInputDir,
									outputTar, OsPath.basename(rf),
									// resuse log files
									OsPath.join(taskLogDir, "tar.output"),
									OsPath.join(taskLogDir, "tar.error"));
							if (Stage.executeCmd(cmd, mapLogger) != 0) {	
								// The exception is caught below
								mapLogger.fatal("Could not create tar file: " + outputTar);	
								throw new Exception("Could not create tar file: " + outputTar);								
							}
						}
						else { // not first time
							// Add file to existing tar file
							cmd = String.format("cd %s; tar rvf %s %s > %s 2> %s",
									taskInputDir, 
									outputTar, OsPath.basename(rf),
									// resuse log files
									OsPath.join(taskLogDir, "tar.output"),
									OsPath.join(taskLogDir, "tar.error"));
							if (Stage.executeCmd(cmd, mapLogger) != 0) {
								// The exception is caught below
								mapLogger.fatal("Could not append to tar file: " + outputTar);
								throw new Exception("Could not append to tar file: " + outputTar);		
							}
						}							
					} // for all raw files
					nWritten++;
				} // for all samples
			} catch (Exception e) {				
				OsPath.delete(outputTar);
				return -1;
			}
			return nWritten;
		} // for all platforms
		
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
						
			job = Job.getInstance(conf, progName);
			job.setJarByClass(BatchPclRemoveOverlapping.class);

			/* Setup mapper */
			job.setMapperClass(SplitRemoveOverlapMapper.class);	    				

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
		BatchRawRemoveOverlapping o = new BatchRawRemoveOverlapping();
		int exitCode = o.run(args);		
		System.exit(exitCode);	    	    
	}

}
