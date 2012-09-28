/**
 * Export the content of an Hbase table to a file on the local filesystem. The code
 * is based on the org.apache.hadoop.hbase.mapreduce.Export.java class in the Hbase
 * source code.
 * 
 * This is a standalone mapreduce job that is intended to be run from the hbaseExport.py script.
 */

package edu.princeton.function.troilkatt.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HbaseExport {
	  /**
	   * Mapper.
	   */
	  static class Exporter
	  extends TableMapper<ImmutableBytesWritable, Result> {
	    /**
	     * @param row  The current table row key.
	     * @param value  The columns.
	     * @param context  The current context.
	     * @throws IOException When something is broken with the data.
	     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
	     *   org.apache.hadoop.mapreduce.Mapper.Context)
	     */
	    @Override
	    public void map(ImmutableBytesWritable row, Result value,  Context context)  throws IOException {
	    	try {
	    		context.write(row, value);
	    	} catch (InterruptedException e) {
	    		e.printStackTrace();
	    	}
	    }
	  }

	  /**
	   * Sets up the job.
	   *
	   * @param conf  The current configuration.
	   * @param args  The command line parameters.
	   * @return The newly created job.
	   * @throws IOException When setting up the job fails.
	   */
	  public static Job createSubmittableJob(Configuration conf, String[] args)  throws IOException {
		  /*
		   * Parse args and setup job
		   */
		  String tableName = args[0];
		  Path hdfsOutputDir = new Path(args[1]);
		  
		  // Set memory limits
		  // Note! must be done before creating job
		  conf.setLong("mapred.job.map.memory.mb", 2048); // in MB		
		  conf.setLong("mapred.job.reduce.memory.mb", 2048); // in MB				
		  conf.set("mapred.child.java.opts", "-Xmx 1024m -XX:MaxPermSize=256m"); // in MB
					
		  Job job = new Job(conf, "hbase_export_" + tableName);
		  job.setJarByClass(Exporter.class);
		  		  
		  /*
		   * Setup scanner and parse optional args
		   */
		  Scan s = new Scan();
		  // Optional arguments.
		  //int versions = args.length > 2? Integer.parseInt(args[2]): 1;
		  //s.setMaxVersions(versions);
		  //long startTime = args.length > 3? Long.parseLong(args[3]): 0L;
		  //long endTime = args.length > 4? Long.parseLong(args[4]): Long.MAX_VALUE;
		  //s.setTimeRange(startTime, endTime);
		  //Log.info("verisons=" + versions + ", starttime=" + startTime + ", endtime=" + endTime);
		  
		  TableMapReduceUtil.initTableMapperJob(tableName, s, Exporter.class, null,  null, job);
		  
		  // No reducers.  Just write straight to output files.
		  job.setNumReduceTasks(0);
		  job.setOutputFormatClass(SequenceFileOutputFormat.class);
		  job.setOutputKeyClass(ImmutableBytesWritable.class);
		  job.setOutputValueClass(Result.class);
		  FileOutputFormat.setOutputPath(job, hdfsOutputDir);
		  return job;
	  }

	  /*
	   * @param errorMsg Error message.  Can be null.
	   */
	  private static void usage(final String errorMsg) {
		  if (errorMsg != null && errorMsg.length() > 0) {
			  System.err.println("ERROR: " + errorMsg);
		  }
		  System.err.println("Usage: Export <tablename> <outputdir> [<versions> " +
				  "[<starttime> [<endtime>]]]");
	  }

	  /**
	   * Main entry point.
	   *
	   * @param args  The command line parameters.
	   * @throws Exception When running the job fails.
	   */
	  public static void main(String[] args) throws Exception {
		  Configuration conf = new Configuration();
		  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		  if (otherArgs.length < 2) {
			  usage("Wrong number of arguments: " + otherArgs.length);
			  System.exit(-1);
		  }
		  Job job = createSubmittableJob(conf, otherArgs);
		  System.exit(job.waitForCompletion(true)? 0 : 1);
	  }
}
