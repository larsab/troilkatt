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
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * Recompress a set of files without changing the timestamp
 */
public class ReCompress extends PerFile {	
	enum CompressCounters {
		FILES_COMPRESSED		
	}
	
	public static class CompressMapper extends PerFileMapper {
		protected Counter filesCompressed;
		
		/**
		 * Setup global variables. This function is called before map()
		 * @throws IOException 
		 */
		@Override
		public void setup(Context context) throws IOException {
			super.setup(context);			
			
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
					e.printStackTrace();
					System.err.println("Could not recompress file: " + inputFilename);
					IOUtils.closeStream(out);
					tfs.deleteFile(outputFilename);
					return;
				}
			}			
			else {
				// Read input file to local FS, uncompress, re-compress and rewrite back to hadoop
				// The TroilkattFS methods will use a compression codec if possible
				String localInputFilename = tfs.getFile(inputFilename, taskInputDir, taskTmpDir, taskLogDir);
				if (localInputFilename == null) {
					System.err.println("Could not uncompress input file: " + inputFilename);
					return;
				}
				if (tfs.putLocalFile(localInputFilename, outputDir, taskTmpDir, taskLogDir, compressionFormat, srcTimestamp) == null) {
					System.err.println("Could not re-compress to: " + compressionFormat + " input file: " + inputFilename);
					return;
				}
			}
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
			e2.printStackTrace();
			System.err.println("Could not parse arguments: " + e2);
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
						
			job = new Job(conf, progName);
			job.setJarByClass(ReCompress.class);
			
			/* Setup mapper: use the Compress class*/
			job.setMapperClass(CompressMapper.class);				

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
			jobLogger.fatal("Job setup failed", e1);
			return -1;
		} catch (StageInitException e) {
			jobLogger.fatal("Could not initialize job: ", e);
			return -1;
		}			
		
	    // Execute job and wait for completion
		return this.waitForCompletionLogged(job);
	}
	
	/**
	 * @param args[0] file that was written in pipeline.Mapreduce.writeMapReduceArgsFile
	 */
	public static void main(String[] args) {			
		ReCompress o = new ReCompress();
		int exitCode = o.run(args);		
		System.exit(exitCode);	    	    
	}
}
