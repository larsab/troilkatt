package edu.princeton.function.troilkatt.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.princeton.function.troilkatt.PipelineException;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.LogTable;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;

/**
 * Superclass for MapReduce jobs that read and write files.
 */
public class PerFile extends TroilkattMapReduce {

	/**
	 * Generic PerFile mapper class. The mapper will be executed once per file, with the 
	 * filename given as key, and the value field set to NULL. 
	 */
	public static class PerFileMapper extends Mapper<Text, BytesWritable, Text, Text> {		
		/*
		 * All global variables are initialized in setup() 
		 */		
		protected Configuration conf;
		protected TroilkattProperties troilkattProperties;
		protected FileSystem hdfs;
		protected TroilkattFS tfs;
		
		protected String compressionFormat;
		protected long timestamp;
		
		// Passed as arguments in the configuration context		
		//protected String localTmpDir;
		//protected String localOutputDir;
		//protected String localInputDir;
		//protected String localLogDir;
		
		// Task specific variables
		protected String jobID;
		protected String taskAttemptID;
		protected String taskLogDir;
		protected String taskMetaDir;
		protected String taskTmpDir;
		protected String taskInputDir;
		protected String taskOutputDir;
		
		protected Logger mapLogger;
		protected LogTable logTable;
		
		// Set to true when output and log files have been saved. This is normally done in 
		// cleanup, but in case of an IOException it may be necessary to do it in the 
		// object destructor. 
		protected boolean cleanupComplete;
		
		/**
		 * Setup global variables, logger, tfs, and local directories.
		 * 
		 * This function is called once per task before map()
		 * 
		 * @param context Initialized context object
		 * @throws  
		 */
		@Override
		public void setup(Context context) throws IOException {
			conf = context.getConfiguration();
			Configuration hbConf = HBaseConfiguration.create();
			String pipelineName = TroilkattMapReduce.confEget(conf, "troilkatt.pipeline.name");
			compressionFormat = TroilkattMapReduce.confEget(conf, "troilkatt.compression.format");
			timestamp = Long.valueOf(TroilkattMapReduce.confEget(conf, "troilkatt.timestamp"));
			try {
				logTable = new LogTable(pipelineName, hbConf);
			} catch (PipelineException e) {
				throw new IOException("Could not create logTable object: " + e.getMessage());
			}
			
			jobID = context.getJobID().toString();
			taskAttemptID = context.getTaskAttemptID().toString();
			taskLogDir = TroilkattMapReduce.getTaskLocalLogDir(jobID, taskAttemptID);
			taskMetaDir = TroilkattMapReduce.getTaskLocalMetaDir(conf, jobID, taskAttemptID);
			taskTmpDir = TroilkattMapReduce.getTaskLocalTmpDir(conf, jobID, taskAttemptID);
			taskInputDir = TroilkattMapReduce.getTaskLocalInputDir(conf, jobID, taskAttemptID);
			taskOutputDir = TroilkattMapReduce.getTaskLocalOutputDir(conf, jobID, taskAttemptID);
			// The logger writes to the MapReduce userlogs/<job-id>/<task-id>/syslog file
			mapLogger = TroilkattMapReduce.getTaskLogger(conf);
			String troilkattConfigFile = TroilkattMapReduce.confEget(conf, "troilkatt.configuration.file");
			try {
				troilkattProperties = new TroilkattProperties(troilkattConfigFile);
			} catch (TroilkattPropertiesException e) {
				mapLogger.fatal("Could not create troilkatt properties object from file: " + troilkattConfigFile, e);				
				throw new IOException("Failed to create troilkatt properties object from file: " + troilkattConfigFile);
			}
			
			/* Setup TFS/ HDFS */
			hdfs = FileSystem.get(conf);
			tfs = new TroilkattFS(hdfs);
			
			cleanupComplete = false;
		}		
		
		/**
		 * Cleanup function that is called once at the end of the task
		 * 
		 * Note! This function will not be run in if an IOException is thrown in the 
		 * mapper or setup
		 */
		@Override
		protected void cleanup(Context context) throws IOException {
			doCleanup(true);
		}

		/**
		 * This method is called when the object is garbage collected
		 */
		public void finalize() {				
			try {
				doCleanup(false);
			} catch (IOException e) {
				// This should never happend
				e.printStackTrace();
				throw new RuntimeException("Should not happen");
			}
		}
		
		/**
		 * Helper function to save output and log files, and to delete temporary files.
		 * @throws IOException 
		 */
		public void doCleanup(boolean saveOutputFiles) throws IOException {
			if (cleanupComplete) {
				return;
			}
			
			IOException eThrown = null;
			
			// LogTable is closed in LogTable.destructor
			try { 
				TroilkattMapReduce.saveTaskOutputFiles(tfs, conf, taskOutputDir, taskTmpDir, taskLogDir, compressionFormat, timestamp);
			} catch (IOException e) {
				// Throw exception later such that temporary files are deleted
				eThrown = e;
				e.printStackTrace();
				System.err.println("Could not save output files");
			}
			
			try {
				TroilkattMapReduce.saveTaskLogFiles(conf, taskLogDir, taskAttemptID, logTable);
			} catch (IOException e) {			
				e.printStackTrace();
				System.err.println("Could not save log files");
			}
			// Local FS directories will be deleted in finalize
			
			OsPath.deleteAll(taskMetaDir);
			OsPath.deleteAll(taskTmpDir);
			OsPath.deleteAll(taskInputDir);
			OsPath.deleteAll(taskOutputDir);
			// log directory is automatically deleted by MapReduce framework
			
			cleanupComplete = true;
			
			if (eThrown != null) {
				throw new IOException(eThrown);
			}
		}
		
		/**
		 * Do the mapping.
		 * 
		 * @param key: HDFS filename
		 * @param value: always null since the input files can be very large	
		 */
		@Override
		public void map(Text key, BytesWritable value, Context context) 
		throws IOException, InterruptedException {
			throw new RuntimeException("Subclass should implement this function");	
		}		
				
		/**
		 * Helper function to open a line reader for a file either in HDFS or a file 
		 * copied to the local file system (the file is stored in the stage input 
		 * directory). The latter is necessary for files compressed with a codec that
		 * is not supported by HDFS.
		 * 
		 * @param inputFilename HDFS filename to open
		 * @return initialized LineReader, or null if the file could not be opened		
		 */
		public BufferedReader openBufferedReader(String inputFilename) {
			Path inputPath = new Path(inputFilename);
			String basename = tfs.getFilenameName(inputFilename);			
			String compression = tfs.getFilenameCompression(inputFilename);
			long timestamp = tfs.getFilenameTimestamp(inputFilename);
			if ((basename == null) || (compression == null) || (timestamp == -1)) {				
				return null;
			}

			// Check if the compression format for the input file used is supported by Hadoop
			CompressionCodecFactory factory = new CompressionCodecFactory(conf);
			CompressionCodec inputCodec = factory.getCodec(inputPath);

			// open a linereader either directly on the HDFs file or on a copy on local FS
			InputStream ins = null;
			FileInputStream fin = null;
			if (compression.equals("none")) {
				try {
					ins = hdfs.open(inputPath); 	
					return new BufferedReader(new InputStreamReader(ins));
				} catch (FileNotFoundException e) {
					return null;
				} catch (IOException e) {
					return null;					
				}
			}
			else if (inputCodec != null) {
				try {
					ins = inputCodec.createInputStream(hdfs.open(inputPath));
					return new BufferedReader(new InputStreamReader(ins));					
				} catch (FileNotFoundException e) {					
					return null;
				} catch (IOException e) {
					return null;					
				}
			}
			else {
				try {
					String localInputFilename = tfs.getFile(inputFilename, taskInputDir, taskTmpDir, taskLogDir);
					if (localInputFilename == null) {
						return null;
					}
					// Read input file to local FS and open a stream to the local file				
					fin = new FileInputStream(new File(localInputFilename));				
					return new BufferedReader(new InputStreamReader(fin));
				} catch (IOException e) {
					return null;
				}
			}
		}
		
		/**
		 * Helper function to open a buffered writer to a file in the MapReduce task output directory
		 * in HDFS. 
		 * 
		 * @param filename base filename
		 * @param compression to use. Note this method does not check if the compression method is 
		 * valid.
		 * @param context HDFS MapReduce provided context handle
		 * @return BufferedWriter handle, or null if the file could no be opened due to an 
		 * unsupported compression codec-
		 * @throws IOException
		 */
		public BufferedWriter openBufferedWriter(String filename, String compression, Context context) throws IOException {			
			String outputDir = TroilkattMapReduce.getTaskHDFSOutputDir(context);
			String hdfsOutputFilename =  OsPath.join(outputDir,  filename + "." + compression);
			String outputCompression = tfs.getFilenameCompression(hdfsOutputFilename);
			Path outputPath = new Path(hdfsOutputFilename);
			
			// Check if the compression format for the output file is supported by Hadoop
			CompressionCodecFactory factory = new CompressionCodecFactory(conf);
			CompressionCodec outputCodec = factory.getCodec(outputPath);
							
			if ((outputCodec != null) || outputCompression.equals("none")) { // both input and output codec is supported by hadoop				
				// Write directly to HDFS
				OutputStream os = outputCodec.createOutputStream(hdfs.create(outputPath));
				return new BufferedWriter(new OutputStreamWriter(os));	
			}
			else {
				return null;
			}
		}		
		
		/**
		 * Helper function to close and delete a file in the MapReduce task output directory.
		 * 
		 * Note! This function throws an IOException in case the HDFS file cannot be closed
		 * 
		 * @param bw file stream to close. Can also be null, in which case the stream is not closed
		 * and the file is not deleted.
		 * @param filename base filename 
		 * @param compression to use. Note this method does not check if the compression method is 
		 * valid. 
		 * @param context HDFS MapReduce provided context handle
		 * @throws IOException 
		 */
		public void closeDeleteBufferedWriter(BufferedWriter bw, String filename, 
				String compression, Context context) throws IOException {
			if (bw == null) {
				return;
			}
			
			// close...
			bw.close();
			
			// ...and delete
			String outputDir = TroilkattMapReduce.getTaskHDFSOutputDir(context);
			String hdfsOutputFilename =  OsPath.join(outputDir,  filename + "." + compression);
			tfs.deleteFile(hdfsOutputFilename);
		}
	
		/**
		 * Helper function to close and delete a file on the local file system.
		 * 
		 * Note! This function catch IOExceptions on the local files and ignores these.
		 * 
		 * @param bw file stream to close. Can also be null
		 * @param localFilename absolute filename of file to delete
		 */
		public void closeDeleteLocalBufferedWriter(BufferedWriter bw, String localFilename) {
			
			if (bw == null) { // nothing to do
				return;
			}
						
			try {
				bw.close();
			} catch (IOException e) {
				mapLogger.warn("Could not close local file: " + e.getMessage());
			}
			
			OsPath.delete(localFilename);
		}
	}
	
	
	/**
	 * This class is called to create a split to be processed by a mapper. The split
	 * is divided into one-file-per-record records. 
	 * 
	 * The code in this class is run by the JobClient the node where the MapReduce job is started.
	 */
	public static class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable>  {

		/**
		 * The superclass' function is overriden to prevent spliting a file into multiple
		 * chunks
		 * 
		 * @param context ignord argument
		 * @param filename ignored argument
		 * @return always false
		 */
		@Override
		protected boolean isSplitable(JobContext context, Path filename) {			
			return false;
		}

		/**
		 * This function creates a record reader that delivers the file contents to
		 * each mapper.
		 */
		@Override
		public RecordReader<Text, BytesWritable>  createRecordReader(InputSplit split, TaskAttemptContext context)
		throws IOException, InterruptedException {			
			return new FilenameReader();
			// The framework will call FilenameReader.initialize(InputSplit, TaskAttemptContext) 
			// before the split is used. 
		}
	}
	
	/**
	 * This class implemented a record reader that delivers filenames to the PerFileMapper.
	 * It does not read the file content, nor send it to the mappers.
	 * 
	 * There is one RecordReader instance per file (input split)
	 * 
	 * Note! The implementation assumes that files cannot be split.
	 * 
	 * The code in this class is run by the TaskTracker on each of the worker nodes in 
	 * the cluster.
	 */
	public static class FilenameReader extends RecordReader<Text, BytesWritable>  {				
		private FileSplit filesplit;
		// Each split consits of a file which is either read or not. 
		// When the filename has been returned to the map() function this
		// value is set to processed
		private boolean processed;
		
		// Key is the filename and is set for each file read
		private Text key = null; 
		// Value is always null
		private BytesWritable value = null;
		
		/**
		 * Called once at initialization.
		 * 
		 * Since there is one instance per input split, this function is called once per fil.
		 * 
		 * @param split
		 * @param context
		 */
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) {
			this.filesplit = (FileSplit) split;
			this.processed = false;
		}

		/**
		 * Close the record reader
		 */
		@Override
		public void close() {
			// nothing to do
		}

		/**
		 * @return the current key or null if there is no current key
		 */
		@Override
		public Text getCurrentKey() {
			return key;
		}

		/**
		 * @return the current value which is always null, since it the filename is stored in the key
		 */
		@Override
		public BytesWritable getCurrentValue() {
			return value;
		}

		/**
		 * The current progress of the record reader through its data. 
		 * 
		 * @return Zero progress if the file has not bean read, and full progress if it has.
		 */
		@Override
		public float getProgress() throws IOException {
			// A file is eaitehr not read or read, so the progress is either zero or 100%
			return processed ? 1.0f: 0.0f;
		}

		/**
		 * Read the next key, value pair. 
		 * 
		 * @return true if a key/value pair was read. False otherwise 
		 */
		@Override
		public boolean nextKeyValue() throws IOException {
			if (! processed) {			
				// Get filename of input split
				Path file = filesplit.getPath();			
				key = new Text(file.toString());
				
				// The file content is not read since it can be very large, and the mapper may not need
				// to read the entire file
				value = null;
								
				// The input split has been processed
				processed = true;
				return true;
			}
			else {
				// Input split has already been processed
				return false;
			}
		}		
	}			

	/**
	 * Initialize MapReduce job to use per-file input and output classes 
	 * 
	 * This function should be called in the run() function of the subclass
	 * 
	 * @param conf initialized hadoop configuration object
	 * @param job initialized job object
	 */
	public void perFileConfInit(Configuration conf, Job job) {
		/* Turn off speculative execution since per-file-processing often results in an unbalanced workload
		 * and therefore some task will take much longer to run. These should not be restarted on other
		 * nodes. */
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);		
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);		
		
		conf.setInt("mapred.task.timeout", 30 * 60 * 1000);
		
		/* Setup filter */
		job.setInputFormatClass(WholeFileInputFormat.class);		
		//job.setOutputFormatClass(FileOutputFormat.class);
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(Text.class);
	}	
}
