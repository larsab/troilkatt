package edu.princeton.function.troilkatt.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.Stage;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

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
				
		private boolean runScript;
		private String scriptCmd;
		
		/**
		 * Setup global variables. This function is called before map()
		 * @throws IOException 
		 */
		@Override
		public void setup(Context context) throws IOException {
			super.setup(context);			
			
			// Parse arguments
			String[] args = TroilkattMapReduce.confEget(conf, "troilkatt.stage.args").split(" ");
			if (args.length != 1) {					
				throw new IOException("Invalid arguments: " + args);
			}
			if (args[0].equals("script")) {
				runScript = true;			
			}
			else {
				runScript = false;
			}
			try {
				scriptCmd = "python " + OsPath.join(troilkattProperties.get("troilkatt.localfs.scripts.dir"), "bigmem.py");
			} catch (TroilkattPropertiesException e) {
				throw new IOException("TroilkattProperties read error");
			} 
			
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

			context.setStatus("Read: " + inputFilename);
			
			BufferedReader bri = openBufferedReader(inputFilename);
			if (! runScript) { // do it in Java
								
				ArrayList<String> lines = new ArrayList<String>();
				String line;
				while ((line = bri.readLine()) != null) {
					lines.add(line);
				}
				successCounter.increment(1);
			}
			else { // copy file and run script
				String filename = OsPath.join(taskTmpDir, "script.input");
				BufferedWriter bwo = new BufferedWriter(new FileWriter(filename));
				String line;
				while ((line = bri.readLine()) != null) {
					bwo.write(line + "\n");
				}
								
				int rv = Stage.executeCmd(scriptCmd + " " + filename, mapLogger);
				if (rv == -1) {
					killedCounter.increment(1);
				}
				else {
					successCounter.increment(1);
				}
			}
			bri.close();						
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
			System.err.println("Inavlid arguments " + cargs);
			return -1;
		}			
		
		/*
		 * Get and set job specific args
		 */
		try {
			String argStr = TroilkattMapReduce.confEget(conf, "troilkatt.stage.args");
			String[] allArgs = argStr.split(" ");
			if (allArgs.length != 4) {
				jobLogger.fatal("Invalid args: " + argStr);
			}
			long maxTroilkattVMem = Long.valueOf(allArgs[0]);
			long maxMapredVMem = Long.valueOf(allArgs[1]);
			long maxPMem = Long.valueOf(allArgs[2]);
			String runScript = allArgs[3];
			
			// Remove all except runScript from stage args
			conf.set("troilkatt.stage.args", runScript);
			
			// Set job specific memory limits
			this.setMemoryLimits(conf, maxTroilkattVMem, maxMapredVMem, maxPMem);
			
		} catch (IOException e2) {
			jobLogger.fatal("Could not parse args: ", e2);			
			return -1;
		} catch (NumberFormatException e3) {
			jobLogger.fatal("Could not parse args: ", e3);
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
			job = new Job(conf, progName);
			job.setJarByClass(BigMem.class);
			
			/* Setup mapper: use the Compress class*/
			job.setMapperClass(BigMemMapper.class);				

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
			jobLogger.fatal("Job setup failed due to IOException: ",  e1);
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
		BigMem o = new BigMem();
		int exitCode = o.run(args);		
		System.exit(exitCode);	    	    
	}

}
