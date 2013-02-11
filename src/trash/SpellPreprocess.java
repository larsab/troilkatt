package edu.princeton.function.troilkatt.mapreduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;
import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFile2;
import edu.princeton.function.troilkatt.tables.DataTable;
import edu.princeton.function.troilkatt.tables.PipelineTable;
import edu.princeton.function.troilkatt.tables.TableFactory;

/**
 * Calculate datastructures used distributed search. These are:
 * 1. A gene-to-gene matrix with the number of datasets where a pair of genes
 *    are correlated.
 * 2. The .sinfo files used during search
 */
public class SpellPreprocess extends PerFile{
	enum LineCounters {
		GENES_CALCULATED		
	}

	/**
	 * Constructor
	 */
	public SpellPreprocess() {
		super();
		PROGNAME = "SplitSoft";
	}

	/**
	 * Mapper class that gets as input a filename and outputs the .sinfo filename, and a 
	 * list of  gene scores.
	 *  
	 * For each file it does the following:
	 * 1. Read in a PCL file at a time
	 * 2. Do the correlation calculation
	 * 3. Copy the .sinfo file to a task specific directory in HDFS
	 * 4. Output the HDFS filename to the reducer
	 * 5. Output all correlation counts to the reducer
	 */
	public static class PerFileParse
	extends Mapper<Text, BytesWritable, Text, Text> {
		// All of these are initialized in configure() 
		Logger logger = null;		

		long timestamp = 0;

		// On local filesystem
		String myDir = null;		
		// Outputdir in HDFS
		String mapredOutputDir = null;

		TroilkattProperties troilkattProperties = null;	
		Configuration conf;

		// HDFS filesystem handle
		FileSystem hdfs;

		/**
		 * Helper function for setup(): error wrapped conf.get(). 
		 * 
		 * @param key: property key
		 * @return property value
		 * @throws RuntimeException if the property does not exists in the configuration map
		 */
		public String confEget(String key) {
			String val = conf.get(key);
			if (val == null) {
				logger.fatal("Key not in configuration map: " + key);
				throw new RuntimeException("Key not in configuration map: " + key);
			}
			return val;
		}

		/**
		 * Setup global variables. This function is called once per task before map()
		 */
		@Override
		public void setup(Context context) {
			conf = context.getConfiguration();			

			String configFile = conf.get("configFile");
			if (configFile == null) {
				throw new RuntimeException("Could not read configFilename from configuration");
			}

			myDir = conf.get("myDir");
			if (myDir == null) {
				throw new RuntimeException("Could not read myDir from configuration");
			}
			String logDir = OsPath.join(myDir, "log");

			// Logging is to a task specific file in the local filesystem
			troilkattProperties = Troilkatt.getProperties(configFile);
			Troilkatt.setupLogging("debug", logDir, "spell-preprocessor-" + conf.get("mapred.task.id") + ".log");
			logger = Logger.getLogger("troilkatt.spell-preprocessor");

			timestamp = conf.getLong("timestamp", 0);
			if (timestamp == 0) {
				logger.fatal("Timestamp not set in configuration parameters");
				throw new RuntimeException("Timestamp not set in configuration parameters");
			}

			// Must use task specific output directory in case of speculative execution
			mapredOutputDir = confEget("mapred.output.dir");
			troilkattProperties.set("troilkatt.tfs.root.dir", mapredOutputDir);			

			try {
				hdfs = FileSystem.get(conf);
			} catch (IOException e1) {
				logger.fatal("Could not create HDFS FileSystem object");
				logger.fatal(e1.toString());
				throw new RuntimeException(e1);
			}
		}		

		/**
		 * Cleanup function that is called once at the end of the task
		 */
		@Override
		protected void cleanup(Context context)
		{
			//try {
			//	hdfs.close();
			//} catch (IOException e) {
			//	logger.warn("Could not close HDFS filesystem.");
			//	logger.warn(e.toString());
			//}
		}

		/**
		 * Do the mapping: 
		 * 1. Read a file from HDFS
		 * 2. Write meta data to a meta file
		 * 3. Copy the file to HDFS
		 * 4. Send the filename to the reducer
		 * 
		 * @param key: HDFS soft filename
		 * @param value: always null since the SOFT files can be very large	
		 */
		@Override
		public void map(Text key, BytesWritable value, Context context) 
		throws IOException, InterruptedException {
			// Maximum line size
			final int MAX_LINE_SIZE = 1048576; // 1MB			
			// Buffer for lines read from the input file
			Text line = new Text(new byte[MAX_LINE_SIZE]);

			// Counters used to report progress and avoid a job being assumed to be crashed
			Counter linesRead = context.getCounter(LineCounters.GENES_CALCULATED);					

			/*
			 * Get/create filenames 
			 */
			String srcFilename = key.toString();
			logger.info("Map file: " + srcFilename);			
			context.setStatus("Split file: " + srcFilename);
			String basename = TroilkattFile2.stripBasename(OsPath.basename(srcFilename), logger);
			String hdfsBasename = basename + ".meta." + timestamp + ".none";
			String dstFilename = OsPath.join(myDir, hdfsBasename);
			String hdfsFilename = OsPath.join(mapredOutputDir, hdfsBasename);

			/*
			 * Read one line at a time and write meta-lines to an output file
			 */
			FSDataInputStream in = hdfs.open(new Path(srcFilename));			 
			LineReader lin = new LineReader(in);			
			BufferedWriter os = new BufferedWriter(new FileWriter(new File(dstFilename)));
			// First line is alway the soft filename the meta data was extraced from
			os.write("!Soft_filename = " + key.toString() + "\n");

			long lcnt = 0;
			while (true) {
				int bytesRead = lin.readLine(line, MAX_LINE_SIZE); 
				if (bytesRead <= 0) {
					break;
				}
				linesRead.increment(1);
				lcnt++;

				if (lcnt % 1000 == 0) {
					context.setStatus("Lines read = " + lcnt);
				}

				if (line.charAt(0) == '!') { // is meta line
					if (bytesRead >= MAX_LINE_SIZE) {
						throw new IOException("Line size > MAX_LINE_SIZE (" + MAX_LINE_SIZE + ")");
					}

					os.write(line.toString() + "\n");
					//linesWritten.increment(1);
				}
			}
			os.close();
			lin.close();
			in.close();

			/*
			 * Move local file to HDFS
			 */
			logger.info("Copy local file to HDFS:" + hdfsFilename);
			context.setStatus("Copy local file to HDFS:" + hdfsFilename);
			hdfs.copyFromLocalFile(new Path(dstFilename), new Path(hdfsFilename));

			/*
			 * Output list of new files to reduce stage
			 */			
			context.write(new Text("meta"), new Text(hdfsFilename));														
		}		
	}

	/**
	 * Reduce class. Move files from map-specific directories to the global output
	 * directory and update Hbase with the new meta-data.
	 */
	public static class MergeFilelist extends Reducer<Text, Text, Text, Text> {
		// All of these are initialized in setup()		
		Logger logger;

		Configuration conf;
		TroilkattProperties troilkattProperties = null;

		String stageName = null;
		String dataTableType = null;
		String dataTableName = null;
		String hdfsRootDir;
		long timestamp = 0;

		PipelineTable pipelineTable;
		DataTable datasetTable;

		ArrayList<TroilkattFile2> allFiles = new ArrayList<TroilkattFile2>(); 
		ArrayList<TroilkattFile2> newFiles = new ArrayList<TroilkattFile2>();
		ArrayList<TroilkattFile2> updatedFiles = new ArrayList<TroilkattFile2>();
		ArrayList<TroilkattFile2> deletedFiles = new ArrayList<TroilkattFile2>();

		FileSystem hdfs;

		/**
		 * Helper function for setup(): error wrapped conf.get(). 
		 * 
		 * @param key: property key
		 * @return property value
		 * @throws RuntimeException if the property does not exists in the configuration map
		 */
		public String confEget(String key) {
			String val = conf.get(key);
			if (val == null) {
				logger.fatal("Key not in configuration map: " + val);
				throw new RuntimeException("Key not in configuration map: " + val);
			}
			return val;
		}		

		/**
		 * This function is called once at the start of the task
		 */
		@Override
		public void setup(Context context) {					
			/*
			 * Get arguments
			 */
			conf = context.getConfiguration();			

			String configFile = conf.get("configFile");
			if (configFile == null) {
				throw new RuntimeException("Could not read configFilename from configuration");
			}

			String myDir = conf.get("myDir");
			if (myDir == null) {
				throw new RuntimeException("Could not read myDir from configuration");
			}
			String logDir = OsPath.join(myDir, "log");

			troilkattProperties = Troilkatt.getProperties(configFile);
			Troilkatt.setupLogging("debug", logDir, "splitsoft-reduce.log");
			logger = Logger.getLogger("troilkatt.splitsoft-reduce");

			stageName = confEget("stageName");
			dataTableType = confEget("dataTableType");
			dataTableName = confEget("dataTableName");

			timestamp = conf.getLong("timestamp", 0);	
			if (timestamp == 0) {
				logger.fatal("Timestamp not set in configuration parameters");
				throw new RuntimeException("Timestamp not set in configuration parameters");
			}

			/*
			 * Get troilkatt configuration and setup tables
			 */			
			hdfsRootDir = troilkattProperties.get("troilkatt.tfs.root.dir");			
			HBaseConfiguration hbConf = new HBaseConfiguration();
			pipelineTable = new PipelineTable(conf, hbConf, troilkattProperties);
			datasetTable = TableFactory.newTable(dataTableType, dataTableName, 
					conf, hbConf, troilkattProperties, logger);

			try {
				hdfs = FileSystem.get(conf);
			} catch (IOException e1) {
				logger.fatal("Could not create HDFS FileSystem object");
				logger.fatal(e1.toString());
				throw new RuntimeException(e1);
			}
		}

		/**
		 * Cleanup function that is called once at the end of the task
		 */
		@Override
		protected void cleanup(Context context)
		{
			//try {
			//	hdfs.close();
			//} catch (IOException e) {
			//	logger.warn("Could not close HDFS filesystem.");
			//	logger.warn(e.toString());
			//}
		}

		/**
		 *  Do the reduce
		 *  1. Move task specific outputfiles to the output directory
		 *  2. Update the Hbase meta data with the new file lists
		 *  
		 *  @param key: always "meta"
		 *  @param value: HDFS task specific filename
		 *  @param context
		 */
		@Override
		public void reduce(Text key, 
				Iterable<Text> value,
				Context context)
		throws IOException, InterruptedException {
			String keyType = key.toString();
			logger.fatal("Reducing: " + keyType);

			ArrayList<TroilkattFile2> outputFiles = new ArrayList<TroilkattFile2>();						

			for (Text t: value) {
				String srcName = t.toString();
				Path srcPath = new Path(srcName);
				String basename = TroilkattFile2.stripBasename(srcPath.getName(), logger);
				TroilkattFile2 dstFile = new TroilkattFile2(basename, null, hdfsRootDir, 
						dataTableName, datasetTable.getFiletype(basename), timestamp, 
						datasetTable.getCompression(basename));					
				Path dstPath = new Path(dstFile.getHDFSFilename());
				Path dstDir = new Path(dstFile.getHDFSDir());

				if (! hdfs.exists(dstDir)) {
					logger.debug("Creating new destination directory: " + dstFile.getHDFSDir());
					if (! hdfs.mkdirs(dstDir)) {
						logger.fatal("Failed to create new destination directory: " + dstDir);
						throw new RuntimeException("Failed to create new destination directory: " + dstDir);
					}
				}

				if (hdfs.exists(dstPath)) {
					logger.debug("Deleting existing file: " + dstPath);
					if (! hdfs.delete(dstPath, false)) {
						logger.fatal("Failed to delete existing destination file: " + dstPath);
						throw new RuntimeException("Failed to delete existing destination file: " + dstPath);
					}
				}

				logger.debug("Move file from map-specific output directory to job output directory: " + basename);
				if (! hdfs.rename(srcPath, dstPath)) {
					logger.fatal("Could not move outputfile");
					logger.fatal("Source: " + srcPath);
					logger.fatal("Destination: " + dstPath);
					throw new RuntimeException("Could not move file: " + basename);
				}											

				outputFiles.add(dstFile);
				context.write(key, new Text(basename));
			}	    			    	

			updateLists(outputFiles, new ArrayList<TroilkattFile2>());
			pipelineTable.saveStageOutput(stageName, 
					allFiles, newFiles, updatedFiles, deletedFiles, 
					new ArrayList<String>(), new HashMap<String, Object>(), timestamp);		
		}

		/**
		 * Update all files list by adding new files and removing deleted files.
		 * 
		 * @param ouputFiles: list of files added to the output directory during the 
		 *  processing of this stage. Can be null. Note that the output file list may
		 *  contain duplicated.
		 * @param deletedFiles: list of files deleted during this crawl. Can be null. Note 
		 *  that the list may contain duplicates. 
		 */
		public void updateLists(ArrayList<TroilkattFile2> outputFiles, ArrayList<TroilkattFile2> myDeletedFiles) {
			allFiles = pipelineTable.getAllFiles(stageName);			

			newFiles.clear();
			updatedFiles.clear();

			newFiles = pipelineTable.getFilelist(stageName, "new", timestamp);
			updatedFiles = pipelineTable.getFilelist(stageName, "updated", timestamp);

			logger.debug(String.format("Add %d output files to allFiles with %d entries\n", 
					outputFiles.size(), allFiles.size()));

			/* Detect new and updated files by comparing the output files to the allFiles list
			 * Note that the outputFiles may contain duplicates */

			// Verify that there are no duplicates
			Vector<String> checked = new Vector<String>();
			for (TroilkattFile2 hbf: outputFiles) {
				if (checked.contains(hbf.getFilename())) {
					continue;			
				}

				checked.add(hbf.getFilename());							

				if (! allFiles.contains(hbf)) { // is new file										
					newFiles.add(hbf);
				}
				else { // is updated file
					// Make sure allFiles list has TroilkattFile handle with most recent timestamp
					// Note! that the allFiles list contains the new file if the file basenames are equal
					allFiles.remove(hbf);
					allFiles.add(hbf);
					updatedFiles.add(hbf);
				}        
			}

			logger.debug(String.format("Delete %d files from allFiles with %d entries\n", 
					myDeletedFiles.size(), allFiles.size()));

			// Delete files from the allFiles list
			checked.clear();
			for (TroilkattFile2 df: myDeletedFiles) {
				if (checked.contains(df.getFilename())) {					
					continue;			
				}

				checked.add(df.getFilename());

				if (! allFiles.contains(df)) {
					//logger.warn("Deleted file not in allFiles list: "  + df);
				}
				else {
					allFiles.remove(df);
					deletedFiles.add(df);
				}

			}    

			logger.debug("Updated allFiles size is: " + allFiles.size());
		}
	}


	/**
	 * Print the usage string for this program.
	 */
	static void printUsage() {
		System.out.println("Usage: java ParseSoft <troilkatt-config-file> <stage-name> <previous-stage-name> <filelist> <data-table-type:data-table-name> <timestamp> <directory>");		
	}	

	/**
	 * Do subclass specific initialization of arguments.
	 * 
	 * @param hadoopConfig: Hadoop configuration
	 * @param args: list of command line arguments (see below)
	 * 
	 * Arguments:
	 *   args[0]: troilkatt configuration file   
	 *   args[1]: stage name (rowkey in pipeline table for output files)
	 *   args[2]: previous stage name (rowkey in pipeline table for input filelists)
	 *   args[3]: filelist to process (all, new, update, or delete)
	 *   args[4]: tableType:tableName where output files are stored   
	 *   args[5]: timestamp for input and output HBase values
	 *   args[6]: directory on local filesystem for log and tmp files	
	 */
	public void myConfInit(Configuration hadoopConfig, String[] args) {
		/*
		 * Parse arguments
		 */
		if (args.length < 6) {			
			printUsage();
			System.exit(-2);
		}

		String configFile = args[0];
		hadoopConfig.set("configFile", configFile);		
		String stageName = args[1];
		hadoopConfig.set("stageName", stageName);
		String prevStageName = args[2];
		hadoopConfig.set("prevStageName", prevStageName);
		String filelist = args[3];
		hadoopConfig.set("filelist", filelist);

		String[] tableParts = args[4].split(":");
		if (tableParts.length != 2) {
			System.err.println("Invalid tableType:tableName argument: " + args[4]);
			System.exit(-2);
		}
		String dataTableType = tableParts[0];
		hadoopConfig.set("dataTableType", dataTableType);
		String dataTableName = tableParts[1];
		hadoopConfig.set("dataTableName", dataTableName);

		long timestamp = Long.valueOf(args[5]);
		hadoopConfig.setLong("timestamp", timestamp);
		String myDir = args[6];				
		hadoopConfig.set("myDir", myDir);				
	}

	/**
	 * Create and execute MapReduce job
	 */
	public int run(Configuration myConf, String[] args) throws IOException {
		myConfInit(myConf, args);

		myConf.setBoolean("mapred.map.tasks.speculative.execution", false);		
		myConf.set("mapred.child.java.opts", "-Djava.class.path=/nhome/larsab/apps/hbase-0.20.0/conf");

		/*
		 * Setup job
		 */				
		Job job = new Job(myConf, PROGNAME);	
		job.setJarByClass(ExecuteStage.class);

		/* Setup mapper */
		job.setMapperClass(PerFileParse.class);	    				

		/* Setup reducer */
		//job.setCombinerClass(UpdateFilelist.class);
		job.setReducerClass(MergeFilelist.class);	    	    		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		commonConfInit(myConf, job);
		if (setInputPaths(myConf, job) == 0) { // No input files
			return 0;
		}
		setOutputPath(myConf, job);

		try {
			return job.waitForCompletion(true) ? 0: 1;
		} catch (InterruptedException e) {
			logger.fatal("Interrupt exception: " + e.toString());
			return -1;
		} catch (ClassNotFoundException e) {
			logger.fatal("Class not found exception: " + e.toString());
			throw new RuntimeException(e);
		}
	}

	/**
	 * Arguments: see documentation for run
	 */
	public static void main(String[] args) throws Exception {			
		// Hadoop configuration (core-default.xml and core-site.xml must be in classpath)
		Configuration conf = new Configuration();
		String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		SplitSoft o = new SplitSoft();
		int exitCode = o.run(conf, remainingArgs);		
		System.exit(exitCode);	    	    
	}
}
