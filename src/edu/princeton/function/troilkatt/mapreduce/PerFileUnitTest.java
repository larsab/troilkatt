package edu.princeton.function.troilkatt.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * Unit test for PerFile: it copies all input files to the output directory.
 * 
 * Note! This program assumes that the files to copy are compressed using 
 * an hadoop supported codec.
 */
public class PerFileUnitTest extends PerFile {

	public static class CopyFileMapper extends PerFileMapper {	
		/**
		 * Setup global variables. This function is called before map()
		 * @throws IOException 
		 */
		@Override
		public void setup(Context context) throws IOException {
			super.setup(context);			
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
			String basename = tfs.getFilenameName(inputFilename);
						// Check if the compression format for the input file used is supported by hadoop
			CompressionCodecFactory factory = new CompressionCodecFactory(conf);
			CompressionCodec inputCodec = factory.getCodec(inputPath);
			
			// Create output filename
			String outputDir = TroilkattMapReduce.getTaskHDFSOutputDir(context);		
			String outputFilename = OsPath.join(outputDir, basename + "." + compressionFormat);
			Path outputPath = new Path(outputFilename);
			
			CompressionCodec outputCodec = factory.getCodec(outputPath);
			
			if (inputCodec == null) {
				System.err.println("Compression codec not supported for file: " + inputFilename);
				return;
			}
			if (outputCodec == null) {
				System.err.println("Compression codec not supported for file: " + outputFilename);
				return;
			}
						
			// Read compressed file directly and write compressed file directly
			System.err.println("Copy file " + inputFilename + " to " + outputFilename);
			InputStream in = inputCodec.createInputStream(hdfs.open(inputPath)); 
			OutputStream out = outputCodec.createOutputStream(hdfs.create(outputPath));
			IOUtils.copyBytes(in, out, conf);
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);			
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
			System.err.println("Could not parse arguments: IOException: " + e2.getMessage());
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
			jobLogger.fatal("Could not create FileSystem object: " + e1.toString());			
			return -1;
		}
			
		/*
		 * Setup job
		 */				
		Job job = null;
		try {
			job = new Job(conf, progName);
			job.setJarByClass(PerFileUnitTest.class);
			
			/* Setup mapper: use the Compress class*/
			job.setMapperClass(CopyFileMapper.class);	    				

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
	 * @param args[0] file that was written in pipeline.Mapreduce.writeMapReduceArgsFile
	 */
	public static void main(String[] args)  {			
		PerFileUnitTest o = new PerFileUnitTest();
		int exitCode = o.run(args);		
		System.exit(exitCode);	    	    
	}
}
