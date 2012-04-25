package edu.princeton.function.troilkatt.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;

import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.hbase.GeoMetaTableSchema;
import edu.princeton.function.troilkatt.hbase.HbaseException;

/**
 * Superclass for Batch PCL processing. This class provides many of the most commonly
 * used functions for processing PCL files.
 */
public class BatchPclCommon extends PerFile {
	enum BatchPclCounters {
		FILES_READ,
		ERRORS,
		FILES_WRITTEN,	
		FILES_DISCARDED,
		ROWS_READ,
		ROWS_WRITTEN,
		ROWS_DISCARDED		
	}
	
	/**
	 * PCL file processing mapper super class.
	 */
	public static class PclMapper extends PerFileMapper {				
		// Counters
		protected Counter filesRead;
		protected Counter errors;
		protected Counter filesWritten;
		protected Counter filesDiscarded;	
		protected Counter rowsRead;
		protected Counter rowsWritten;
		protected Counter rowsDiscarded;		
		
		// Table handle for reading meta data used in the computation
		protected GeoMetaTableSchema geoMetaTable;
		protected HTable metaTable;
				
		/**
		 * Setup global variables. This function is called before map()
		 * @throws IOException 
		 */
		@Override
		public void setup(Context context) throws IOException {
			super.setup(context);			
			
			// Counters used to report progress and avoid a job being assumed to be crashed
			filesRead = context.getCounter(BatchPclCounters.FILES_READ);
			errors = context.getCounter(BatchPclCounters.ERRORS);
			filesWritten = context.getCounter(BatchPclCounters.FILES_WRITTEN);
			filesDiscarded = context.getCounter(BatchPclCounters.FILES_DISCARDED); 
			rowsRead = context.getCounter(BatchPclCounters.ROWS_READ);
			rowsWritten = context.getCounter(BatchPclCounters.ROWS_WRITTEN);
			rowsWritten = context.getCounter(BatchPclCounters.ROWS_DISCARDED);			
			
			// Setup meta-data table
			Configuration hbConf = HBaseConfiguration.create();
			geoMetaTable = new GeoMetaTableSchema();
			try {
				metaTable = geoMetaTable.openTable(hbConf, false);
			} catch (HbaseException e) {
				mapLogger.fatal("HbaseException", e);				
				throw new IOException("HbaseException: " + e.getMessage());
			}
		}
		
		/**
		 * Do the mapping: 
		 * 1. Read one line at a time from a file in HDFS
		 * 2. Do some processing on the line (as implemented by subclass)
		 * 3. Write output to a file in HDFS
		 * 
		 * @param key HDFS soft filename
		 * @param value always null since the SOFT files can be very large	
		 * @throws IOException 
		 */
		@Override
		public void map(Text key, BytesWritable value, Context context) throws IOException {						
			filesRead.increment(1);
							
			/*
			 * Open input stream
			 */
			String inputFilename = key.toString();
			context.setStatus("Convert: " + inputFilename);							
			BufferedReader lin = openBufferedReader(inputFilename);			
			if (lin == null) {
				mapLogger.fatal("Could not open input file: " + inputFilename);
				errors.increment(1);
				return;
			}
				
			/*
			 * Open output stream and write converted file
			 */
			String basename = tfs.getFilenameName(inputFilename);	
			String outputBasename = getOutputBasename(basename);
			BufferedWriter bw = openBufferedWriter(outputBasename, compressionFormat, context);
			if (bw != null) {						
				processFile(lin, bw, inputFilename);
				bw.close();											
			}
			else { 				
				// Could not write directly to HDFS, so must fallback on local file system		
				// In addition IOExceptions must be explicitly caugth in order to do cleanup
				// on open files
				String localFilename = OsPath.join(taskOutputDir, outputBasename);
				bw = null;
				try {
					bw = new BufferedWriter(new FileWriter(new File(localFilename)));
					processFile(lin, bw, inputFilename);
					bw.close();
				} catch (IOException e) {
					mapLogger.error("IOExcpetion: " + e.getMessage());					
					closeDeleteLocalBufferedWriter(bw, localFilename);		
					return;
				} 			
				// All local output files will be written to HDFS in cleanup()
			} 
			lin.close();
			filesWritten.increment(1);					
		}		
		
		/**
		 * Helper function to read rows from the input file, process each row, and write the row 
		 * to the output file
		 * 
		 * Note! Subclasses should implement this method 
		 * 
		 * @param lin initialized BufferedReader
		 * @param bw initialized BufferedWriter
		 * @param filename input filename
		 * @throws IOException
		 */
		protected void processFile(BufferedReader lin, BufferedWriter bw,
				String filename) throws IOException {
			throw new RuntimeException("Subclass should implement this function.");		
		}
		
		/**
		 * Helper function to get the output file basename
		 * 
		 * Note! Subclasses should implement this method
		 * 
		 * @param inputBasename basename of input file
		 * @return basename of output file
		 */
		protected String getOutputBasename(String inputBasename) {
			throw new RuntimeException("Subclass should implement this function.");	
		}
				
		
		
		/**
		 * Helper function to copy a file line by line
		 * 
		 * @param br source file
		 * @param bw destination file
		 * @return 
		 * @throws IOException 
		 */
		protected void copy(BufferedReader br, BufferedWriter bw) throws IOException {
			String line;
			while ((line = br.readLine()) != null) {
				bw.write(line + "\n");
			}
		}
	}
}
