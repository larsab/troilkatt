package edu.princeton.function.troilkatt.clients;

import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.utils.Utils;

import java.io.IOException;


/**
 * Delete a directory from the data directory
 */
public class DeleteDir extends CleanupDir {
	protected static final String clientName = "DeleteDir";
	
	/**
	 * Constructor
	 */
	public DeleteDir() {
		super();
	}
	
	/**
	 * Print usage information.
	 */
	@Override
	protected void usage(String progName) {
		System.out.println(String.format("%s [options] hdfs-dir\n\n" + 
				"Required:\n" +
				"\thdfs-dir: subdirectory to delete\n\n"+				
				"Options:\n" +				
				"\t-c FILE       Specify troilkatt configuration FILE to use (default: %s).\n" +
				"\t-l FILE       log4j.properties FILE to use (default: %s).\n" +
				"\t-h            Display command line options.",
				progName, DEFAULT_ARGS.get("configFile"), DEFAULT_ARGS.get("logProperties")));
	}

	/**
	 * Delete HDFS directory.
	 *
	 * @param argv sys.argv
	 * @throws TroilkattPropertiesException if properties file cannot be parsed
	 */
	public void run(String[] argv) throws TroilkattPropertiesException {
		setupClient(argv, clientName);
		
		String hdfsDir = getDirectory(args.get("hdfsDir"));	
		
		if (Utils.getYesOrNo("Delete directory: " + hdfsDir, false) == false) {
			System.exit(0);
		}
		
		/*
		 * Get list of HDFS-directory files.
		 */		
		logger.info("Deleting HDFS directory: " + hdfsDir);
		try {
			tfs.deleteDir(hdfsDir);
			System.out.println("Deleted: " + hdfsDir);		
		} catch (IOException e) {
			logger.fatal("Could not delete directory: " + hdfsDir, e);
			System.out.println("Could not deleted directory: " + hdfsDir);		
		}
	}
	
	/**
	 * @param args: see description in usage()
	 */
	public static void main(String[] args) {
		DeleteDir dc = new DeleteDir();
		try {
			dc.run(args);
		} catch (TroilkattPropertiesException e) {
			System.err.println("Invalid properties file.");
			System.exit(2);
		}
	}
}
