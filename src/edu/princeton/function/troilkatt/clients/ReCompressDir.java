package edu.princeton.function.troilkatt.clients;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.TroilkattStatus;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import edu.princeton.function.troilkatt.mapreduce.TroilkattMapReduce;
import edu.princeton.function.troilkatt.mapreduce.PerFile.WholeFileInputFormat;
import gnu.getopt.Getopt;

/**
 * Change the compression format used for a troilkatt directory.
 */
public class ReCompressDir extends TroilkattClient {
	private String clientName = "ReCompressDir";
	
	enum CompressCounters {
		FILES_COMPRESSED		
	}
	
	public static class CompressMapper extends  Mapper<Text, BytesWritable, Text, Text>  {
		protected Counter filesCompressed;
		
		protected Configuration conf;		
		protected FileSystem hdfs;
		protected TroilkattFS tfs;
		
		protected String compressionFormat;
		protected long timestamp;
		protected String tmpDir;
		
		/**
		 * Setup global variables. This function is called before map()
		 * @throws IOException 
		 */
		@Override
		public void setup(Context context) throws IOException {
			conf = context.getConfiguration();
			hdfs = FileSystem.get(conf);
			tfs = new TroilkattFS(hdfs);
			
			compressionFormat = TroilkattMapReduce.confEget(conf, "troilkatt.compression.format");
			timestamp = Long.valueOf(TroilkattMapReduce.confEget(conf, "troilkatt.timestamp"));
			tmpDir = (TroilkattMapReduce.confEget(conf, "mapred.tmp.dir"));
			
			filesCompressed = context.getCounter(CompressCounters.FILES_COMPRESSED);
		}
		
		/**
		 * Read and uncompress a file, and compress and then write the file.
		 * 
		 * @param key: HDFS filename
		 * @param value: always null since the input files can be very large	
		 */
		@Override
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			// Split filename into its different components
			String inputFilename = key.toString();
			Path inputPath = new Path(inputFilename);			
			if (! tfs.isfile(inputFilename)) {
				System.err.println("Not a file: " + inputFilename);
				return;
			}
			String basename = tfs.getFilenameName(inputFilename);
			long srcTimestamp = tfs.getFilenameTimestamp(inputFilename);
			String srcCompression = tfs.getFilenameCompression(inputFilename);
			if ((basename == null) || (srcCompression == null) || (srcTimestamp == -1)) {
				System.err.println("Invalid filename: " + inputFilename);
				return;
			}
			
			// Check if the compression format for the input file used is supported by hadoop
			CompressionCodecFactory factory = new CompressionCodecFactory(conf);
			CompressionCodec inputCodec = factory.getCodec(inputPath);
			
			// Create output filename
			Path outputDirPath = FileOutputFormat.getOutputPath(context); // get task specific output directory
			String outputDir = outputDirPath.toString();
			String outputFilename = OsPath.join(outputDir, basename + "." + srcTimestamp + "." + compressionFormat);
			Path outputPath = new Path(outputFilename);			
			CompressionCodec outputCodec = factory.getCodec(outputPath);			
			
			if ((inputCodec != null) || (srcCompression.equals("none")) &&
					(outputCodec != null) || (compressionFormat.equals("none"))) { // both input and output codec is supported by hadoop
				OutputStream out = null;
				try {
					// Read compressed file directly and write compressed file directly
					InputStream in = null;
					if (inputCodec != null) {
						in = inputCodec.createInputStream(hdfs.open(inputPath));
					}
					else {
						in = hdfs.open(inputPath);
					}				
					if (outputCodec != null) {
						out = outputCodec.createOutputStream(hdfs.create(outputPath));
					}
					else {
						out = hdfs.create(outputPath);
					}
					IOUtils.copyBytes(in, out, conf);
					IOUtils.closeStream(in);
					IOUtils.closeStream(out);
					filesCompressed.increment(1);
				} catch (IOException e) {
					System.err.println("Could not recompress file: " + inputFilename);
					IOUtils.closeStream(out);
					tfs.deleteFile(outputFilename);
					return;
				}
			}			
			else {
				// Read input file to local FS, uncompress, re-compress and rewrite back to hadoop
				// The TroilkattFS methods will use a compression codec if possible
				OsPath.deleteAll(tmpDir);
				OsPath.mkdir(tmpDir);
				String localInputFilename = tfs.getFile(inputFilename, tmpDir, tmpDir, tmpDir);
				if (localInputFilename == null) {
					System.err.println("Could not uncompress input file: " + inputFilename);
					return;
				}
				if (tfs.putLocalFile(localInputFilename, outputDir, tmpDir, tmpDir, compressionFormat, srcTimestamp) == null) {
					System.err.println("Could not re-compress to: " + compressionFormat + " input file: " + inputFilename);
					return;
				}
				OsPath.deleteAll(tmpDir);
			}
		}		
	}
	
	public ReCompressDir() {
		super();
		DEFAULT_ARGS.put("compressionFormat", "none");
		DEFAULT_ARGS.put("timestamp", "current");
	}

	/**
	 * Print usage information.
	 * 
	 * @param progName: sys.argv[0]
	 */
	protected void usage(String progName) {
		System.out.println(String.format("%s [options] hdfs-dir\n\n" + 
				"Required:\n" +
				"\thdfs-dir:      HDFS directory with files to re-compress.\n\n"+				
				"Options:\n" +		
				"\t-t TIMESTMP    Specify timestamp to use (default: current time).\n" +				
				"\t-c FILE        Specify troilkatt configuration FILE to use (default: %s).\n" +
				"\t-l FILE        log4j.properties file to use (default: %s).\n" +
				"\t-t TIMESTAMP   Timestamp to add to file (default: current time).\n" +
				"\t-z COMPRESSION Compression format to use (default: %s).\n" +				
				"\t-h             Display command line options.", 
				progName, DEFAULT_ARGS.get("configFile"), DEFAULT_ARGS.get("logProperties")));
	}

	/**
	 * Parse command line arguments. See the usage() output for the currently supported command
	 * line arguments.
	 *
	 * @param argv: command line arguments including the program name (sys.argv[0])
	 * @return: a map with arguments
	 */
	@Override
	protected HashMap<String, String> parseArgs(String[] argv, String progName) {
		HashMap<String, String> argDict = new HashMap<String, String>(DEFAULT_ARGS);				

		// Set defaults		
		argDict.put("configFile", DEFAULT_ARGS.get("configFile"));
		argDict.put("logging", DEFAULT_ARGS.get("logProperties"));		
		argDict.put("timestamp", DEFAULT_ARGS.get("timestamp"));	
		argDict.put("compressionFormat", DEFAULT_ARGS.get("compressionFormat"));

		Getopt g = new Getopt("troilkatt", argv, "hc:l:t:");
		int c;		

		while ((c = g.getopt()) != -1) {
			switch (c) {						
			case 'c':
				argDict.put("configFile", g.getOptarg());
				break;
			case 'l':
				argDict.put("logProperties", g.getOptarg());
				break;
			case 't':
				String timestampStr = g.getOptarg();
				// Make sure it is a valid timestamp
				try {
					Long.valueOf(timestampStr);
				} catch (NumberFormatException e) {
					System.err.println("Invalid timestamp in arguments: " + timestampStr);
					System.exit(2);
				}
				argDict.put("timestamp", timestampStr);
				break;
			case 'z':
				String cf = g.getOptarg();
				if (! TroilkattFS.isValidCompression(cf)) {
					System.err.println("Not a valid compression format: " + cf);
					System.exit(2);
				}
				argDict.put("compressionFormat", cf);
				break;		
			case 'h':
				usage(progName);
				System.exit(0);
				break;
			default:
				System.err.println("Unhandled option: " + c);	
			}
		}
		
		if (argv.length - g.getOptind() < 1) {
			usage(progName);
			System.exit(2);
		}
		argDict.put("hdfsDir",  argv[g.getOptind()]);

		return argDict;
	}		
	
	/**
	 * Timestamp HDFS directory.
	 *
	 * @param argv: sys.argv
	 * @throws TroilkattPropertiesException 
	 */
	public void run(String[] argv) throws TroilkattPropertiesException {
		/*
		 * Parse arguments
		 */
		setupClient(argv, clientName);
	
		System.out.println("Recmpress directory: " + args.get("hdfsDir"));
		String hdfsDir = getDirectory(args.get("hdfsDir"));
		if (hdfsDir == null) {
			System.exit(0);
		}		
		
		long timestamp = 0;		
		if (args.containsKey("timestamp")) {
			timestamp = Long.valueOf(args.get("timestamp"));
		}
		else {
			timestamp = TroilkattStatus.getTimestamp();
		}
		
		String compressionFormat = args.get("compressionFormat");
		if (! TroilkattFS.isValidCompression(compressionFormat)) {
			System.err.println("Not a valid compression format: " + compressionFormat);
		}

		/*
		 * Setup MapReduce job
		 */		
		Configuration conf = new Configuration();	
						
		Job job = null;
		try {
			job = new Job(conf, clientName);
			job.setJarByClass(ReCompressDir.class);
			
			/* Setup mapper: use the Compress class*/
			job.setMapperClass(CompressMapper.class);				

			/* Specify that no reducer should be used */
			job.setNumReduceTasks(0);	
		   
		    // Do per file job configuration
			conf.setBoolean("mapred.map.tasks.speculative.execution", false);		
			conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
			conf.setInt("mapred.task.timeout", 30 * 60 * 1000);
			job.setInputFormatClass(WholeFileInputFormat.class);
		    
			// Arguments are passed to the mapper using the configuration map
			conf.set("troilkatt.timestamp", String.valueOf(timestamp));
			conf.set("troilkatt.compression.format", compressionFormat);
			String tmpDir = OsPath.join(TroilkattMapReduce.confEget(conf, "troilkatt.tmp.dir"), "client/recompress");
			conf.set("mapred.tmp.dir", tmpDir);

		    // Set input and output paths
			FileInputFormat.setInputPaths(job, new Path(hdfsDir));		   
			String hdfsTmpDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), "client-tmp");
			FileOutputFormat.setOutputPath(job, new Path(hdfsTmpDir));
		} catch (IOException e1) {
			System.err.println("Job setup failed due to IOException: " + e1.getMessage());
			System.exit(2);
		} 		
		
	    // Execute job and wait for completion
		try {
			if (job.waitForCompletion(true) == false) {
				System.err.println("MapReduce job failed");
				System.exit(2);
			}
			else {
				System.out.println("Directory successfully re-compressed");
			}
		} catch (InterruptedException e) {
			System.err.println("Interrupt exception: " + e.getMessage());
			System.exit(2);
		} catch (ClassNotFoundException e) {
			System.err.println("Class not found exception: " + e.getMessage());
			System.exit(2);
		} catch (IOException e) {
			System.err.println("Job execution failed: IOException: " + e.getMessage());
			System.exit(2);
		}	
	}
	
	/**
	 * @param args: see description in usage()
	 */
	public static void main(String[] args) {
		TimestampDir dt = new TimestampDir();
		try {
			dt.run(args);
		} catch (TroilkattPropertiesException e) {
			System.err.println("Configuration file error");
		}
	}
}
