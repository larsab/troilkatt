package edu.princeton.function.troilkatt.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.mapreduce.PerFile.PerFileMapper;
import edu.princeton.function.troilkatt.mapreduce.ReCompress.CompressCounters;

public class BigMem extends PerFile {
	enum AllocCounters {
		SUCCESS,
		KILLED
	}
	
	
	/**
	 * This is a dummy mapper that reads the input file into memory, or it starts a subprocess that
	 * reads the input file into memory. 
	 * 
	 * Command line arguments specify the memory limits. These are then set by JobClient during job startup
	 *
	 */
	public static class BigMemMapper extends PerFileMapper {
		protected Counter successCounter;
		protected Counter killedCounter;
		
		// Start 
		private boolean runScript;
		
		/**
		 * Setup global variables. This function is called before map()
		 * @throws IOException 
		 */
		@Override
		public void setup(Context context) throws IOException {
			super.setup(context);			
			
			successCounter = context.getCounter(AllocCounters.SUCCESS);
			killedCounter = context.getCounter(AllocCounters.KILLED);
		}
		
		/**
		 * Read the input file into memory.
		 * 
		 * @param key: HDFS filename
		 * @param value: always null since the input files can be very large	
		 */
		@Override
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			String inputFilename = key.toString();
			Path inputPath = new Path(inputFilename);			
			long srcTimestamp = tfs.getFilenameTimestamp(inputFilename);
			String srcCompression = tfs.getFilenameCompression(inputFilename);			
			
			// Make sure the compression format for the input file used is supported by hadoop
			CompressionCodecFactory factory = new CompressionCodecFactory(conf);
			CompressionCodec inputCodec = factory.getCodec(inputPath);
			if ((inputCodec == null) && (! srcCompression.equals("none"))) {
				throw new IOException("Compression codec not supported by hadoop");
			}

			
			LineReader
			
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
			
		}		
	}
	
	public BigMem() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
