package edu.princeton.function.troilkatt.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Export an HBase table.
 * 
 * Writes content to text files in HDFS. 
 */
public class HbaseAsciiExport {		
	/**
	 * Mapper that takes as input hbase rows and outputs the specified columns.
	 */
	public static class AsciiExporter extends TableMapper<Text, Text> {
		// Columns to write to file. Stored as byte arrays since the Hbase result
		// query functions take byte arrays as input
		// Initialized in setup using a configuration value set in run()
		protected HashMap<byte[], ArrayList<byte[]>> fam2qual;
		
		/**
		 * Setup global variables. This function is called once per task before map()
		 */
		@Override
		public void setup(Context context) throws IOException {							
			Configuration conf = context.getConfiguration();
			fam2qual = new HashMap<byte[], ArrayList<byte[]>>();
			
			/*
			 * Split list of newline seperated columns into (familiy, qualifier) pairs, 
			 * and put these into a hash map.
			 */
			String allColumns = TroilkattMapReduce.confEget(conf, "troilkatt.export.columns");
			String[] columns = allColumns.split("\n");			
			for (String c: columns) {
				String[] parts = c.split(":");
				System.err.println("Add: " + parts[0] + "\t" + parts[1]);
				// The values were checked in run() so they are ot checked here
				byte[] famBytes = Bytes.toBytes(parts[0]);
				byte[] qualBytes = Bytes.toBytes(parts[1]);				
				ArrayList<byte[]> quals = fam2qual.get(famBytes);
				if (quals == null) {
					quals = new ArrayList<byte[]>();
				}
				quals.add(qualBytes);
			}
		}		
		
		/**
		 * @param row  The current table row key.
		 * @param value  The columns.
		 * @param context  The current context.
		 * @throws IOException When something is broken with the data.
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 *   org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {			
			String rowID = Bytes.toString(row.get());

			String outVal = "";
			for (byte[] f: fam2qual.keySet()) {
				for (byte[] q: fam2qual.get(f)) {
					byte[] val = value.getValue(f, q);
					if (val == null) {
						outVal = outVal + "\t";
					}
					else {
						outVal = outVal + "\t" + Bytes.toString(val);
					}
				}
			}
			System.out.println(rowID + "\t" + outVal);
			context.write(new Text(rowID), new Text(outVal));			
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
	public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
		String tableName = args[0];
		System.out.println("tableName: " + tableName);
		Path outputDir = new Path(args[1]);
		System.out.println("hdfsDir: " + args[1]);
		
		// Must be at least one column
		String cols = args[2]; // newline seperated	
		if (args[2].split(":").length != 2) {
			System.err.println("Invalid colum identifier (family:qualifier): " + args[2]);
			System.exit(-1);
		}
		for (int i = 3; i < args.length; i++) {	
			if (args[i].split(":").length != 2) {
				System.err.println("Invalid colum identifier (family:qualifier): " + args[i]);
				System.exit(-1);
			}
			cols = cols + "\n" + args[i];
		}
		System.out.println("cols: " + cols);
		conf.set("troilkatt.export.columns", cols);
		
		Job job = new Job(conf, "export_" + tableName);
		job.setJarByClass(HbaseAsciiExport.class);
		job.setMapperClass(AsciiExporter.class);
		Scan s = new Scan();
		for (int i = 2; i < args.length; i++) {
			// arguments checked above
			String parts[] = args[i].split(":");
			s.addColumn(Bytes.toBytes(parts[0]), Bytes.toBytes(parts[1]));
		}
		
		// Optional arguments.
		//int versions = args.length > 2? Integer.parseInt(args[2]): 1;
		//s.setMaxVersions(versions);
		s.setMaxVersions(1);
		//long startTime = args.length > 3? Long.parseLong(args[3]): 0L;
		//long endTime = args.length > 4? Long.parseLong(args[4]): Long.MAX_VALUE;
		//s.setTimeRange(startTime, endTime);
		//Log.info("verisons=" + versions + ", starttime=" + startTime +
		//		", endtime=" + endTime);
		TableMapReduceUtil.initTableMapperJob(tableName, s, AsciiExporter.class, Text.class,
				Text.class, job);
		// No reducers.  Just write straight to output files.
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, outputDir);
		return job;
	}

	/*
	 * @param errorMsg Error message.  Can be null.
	 */
	private static void usage(final String errorMsg) {
		if (errorMsg != null && errorMsg.length() > 0) {
			System.err.println("ERROR: " + errorMsg);
		}
		System.err.println("Usage: Export <tablename> <outputdir> <column1> [column2 column3 ...]");
	}

	/**
	 * Main entry point.
	 *
	 * @param args  The command line parameters.
	 * @throws Exception When running the job fails.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = TroilkattMapReduce.getMergedConfiguration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3) {
			usage("Wrong number of arguments: " + otherArgs.length);
			System.exit(-1);
		}
		Job job = createSubmittableJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}
