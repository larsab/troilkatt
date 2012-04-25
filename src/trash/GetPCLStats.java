package edu.princeton.function.troilkatt.mapreduce;

import java.io.IOException;
import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

/**
 * Extract meta data from a SOFT file and store it in a separate file.
 */
public class GetPCLStats extends PerFile {
	enum LineCounters {
		LINES_READ,
		META_LINES_WRITTEN
	}
	
	/**
	 * Constructor
	 */
	public GetPCLStats() {
		super();
		PROGNAME = "SplitSoft";
	}

	/**
	 * Mapper class that gets as input a PCL filename and outputs statistics about that file.
	 */
	public static class PCLFileStats
	extends Mapper<Text, BytesWritable, Text, Integer> {
		// All of these are initialized in configure() 
		Logger logger = null;		

		// On local filesystem
		String myDir = null;		

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
			Troilkatt.setupLogging("debug", logDir, "getpclstats-map-" + conf.get("mapred.task.id") + ".log");
			logger = Logger.getLogger("getpclstats-map");								
						
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
		 * 2. Count the number of columns and rows
		 * 3. Send counts to reducer
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
			Counter linesRead = context.getCounter(LineCounters.LINES_READ);				
			
			/*
			 * Get/create filenames 
			 */
			String srcFilename = key.toString();
			logger.info("Map file: " + srcFilename);			
			context.setStatus("Stat file: " + srcFilename);
			String basename = TroilkattFile2.stripBasename(OsPath.basename(srcFilename), logger);			
			
			/*
			 * Read one line at a time and write meta-lines to an output file
			 */
			FSDataInputStream in = hdfs.open(new Path(srcFilename));			 
			LineReader lin = new LineReader(in);						
						
			int columns = 0;
			int rows = 0;
			
			// First line is header
			int bytesRead = lin.readLine(line, MAX_LINE_SIZE); 
			if (bytesRead > 0) {
				String[] cols = lin.toString().split("\t");
				// First three columns are always: YORF, NAME and GWEIGHT
				columns = cols.length - 3;
			}
			// Second line are the EWEIGHT's
			bytesRead = lin.readLine(line, MAX_LINE_SIZE);
			linesRead.increment(2);
			
			while (true) {
				bytesRead = lin.readLine(line, MAX_LINE_SIZE); 
				if (bytesRead <= 0) {
					break;
				}
				linesRead.increment(1);
				rows++;
				
				if (rows % 1000 == 0) {
					context.setStatus("Rows read = " + rows);
				}
								
			}			
			lin.close();
			in.close();
						
			
			/*
			 * Output stats to reduce stage
			 */			
			context.write(new Text("cols"), new Integer(columns));
			context.write(new Text("rows"), new Integer(rows));
			context.write(new Text("cols-" + basename), new Integer(columns));
			context.write(new Text("rows-" + basename), new Integer(columns));
		}		
	}
	
	/**
	 * Reduce class. Move files from map-specific directories to the global output
	 * directory and update Hbase with the new meta-data.
	 */
	public static class MergeStats extends Reducer<Text, Integer, Text, Integer> {
		// All of these are initialized in setup()		
		Logger logger;

		Configuration conf;
		TroilkattProperties troilkattProperties = null;
		
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
			Troilkatt.setupLogging("debug", logDir, "getpclstats-reduce.log");
			logger = Logger.getLogger("troilkatt.getpclstats-reduce");
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
				Iterable<Integer> value,
				Context context)
		throws IOException, InterruptedException {
			String keyType = key.toString();
			logger.fatal("Reducing: " + keyType);				    												
			
			int sum = 0;
			for (Integer t: value) {
				sum += t;				
			}	
			
			context.write(key, new Integer(sum));
		}
	}

	/**
	 * Print the usage string for this program.
	 */
	static void printUsage() {
		System.out.println("Usage: java GetPCLStats <input-dir> <output-dir>");		
	}	
	
	/**
	 * Create and execute MapReduce job
	 */
	public int run(Configuration myConf, String[] args) throws IOException {
		if (args.length < 8) {			
			printUsage();
			System.exit(-2);
		}
		
		parseArgs(myConf, args);
		
		myConf.setBoolean("mapred.map.tasks.speculative.execution", false);		
		myConf.set("mapred.child.java.opts", "-Djava.class.path=/nhome/larsab/apps/hbase-0.20.0/conf");
		
		/*
		 * Setup job
		 */				
		Job job = new Job(myConf, PROGNAME);	
		job.setJarByClass(GetPCLStats.class);
		
		/* Setup mapper */
		job.setMapperClass(PCLFileStats.class);	    				

		/* Setup reducer */
		//job.setCombinerClass(UpdateFilelist.class);
		job.setReducerClass(MergeStats.class);	    	    		
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Integer.class);
	    
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
		
		GetPCLStats o = new GetPCLStats();
		int exitCode = o.run(conf, remainingArgs);		
		System.exit(exitCode);	    	    
	}
}
