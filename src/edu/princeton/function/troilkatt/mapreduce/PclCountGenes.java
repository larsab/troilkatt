package edu.princeton.function.troilkatt.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.tools.FilenameUtils;

/**
 * Count the number of rows (genes) in each file and write the count to the GEO MetaTable
 *
 */
public class PclCountGenes extends BatchPclCommon {
	enum BatchPclGeneCounters {
		FILES_READ,
		TABLE_UPDATED,
		GENES_COUNTED,			
	}
	
	/**
	 * Mapper
	 */
	public static class CountMapper extends PclMapper {		
		// Additional couners
		protected Counter genesCounted;
		
		/**
		 * Setup global variables. This function is called before map()
		 * @throws IOException 
		 */
		@Override
		public void setup(Context context) throws IOException {
			super.setup(context);	
			filesRead = context.getCounter(BatchPclGeneCounters.FILES_READ);
			rowsWritten = context.getCounter(BatchPclGeneCounters.TABLE_UPDATED);
			genesCounted = context.getCounter(BatchPclGeneCounters.GENES_COUNTED);
		}
		
		
		/**
		 * Helper function to read rows from the input file, process each row, and write the row 
		 * to the output file
		 * 
		 * @param br initialized BufferedReader
		 * @param bw initialized BufferedWriter
		 * @param inputFilename input filename
		 * @throws IOException 
		 */
		@Override
		protected void processFile(BufferedReader br, BufferedWriter bw,
				String inputFilename) throws IOException {
			String gid = FilenameUtils.getDsetID(inputFilename);
			String basename = tfs.getFilenameName(inputFilename);
			
			filesRead.increment(1);
			
			/*
			 * Count genes
			 */
			// Initialized to -2 since the two first lines are headers, and eweight
			int cnt = -2;
			@SuppressWarnings("unused")
			String line;
			while ((line = br.readLine()) != null) {
				cnt++;
			}
			
			genesCounted.increment(cnt);
			
			/*
			 * Update meta data table with the gene count 
			 */			
			try {
				Put update = new Put(Bytes.toBytes(gid));
				update.add(Bytes.toBytes("processed"), Bytes.toBytes("nGenes-" + basename), Bytes.toBytes(cnt));				
				mapLogger.info("Set " + geoMetaTable.tableName + ":"  + gid + ":processed:nGenes-" + basename + " to " + cnt);
				metaTable.put(update);			
			} catch (IOException e) {				
				mapLogger.warn("Could not save updated row in Hbase: ", e);				
			}  
			rowsWritten.increment(1);
			
			System.out.println(basename + ": " + cnt);
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
			job.setJarByClass(PclCountGenes.class);

			/* Setup mapper */
			job.setMapperClass(CountMapper.class);	    				

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
			jobLogger.fatal("Job setup failed due to IOException: ", e1);
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
		BatchPclLogTransform o = new BatchPclLogTransform();
		int exitCode = o.run(args);		
		System.exit(exitCode);	    	    
	}
}
